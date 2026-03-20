"""
Google Ads v20 data service
────────────────────────────
Handles ALL data fetching for all 4 analysis modules.

Big-data strategy:
  1. Stream rows from GAQL using the page iterator (never loads all into RAM)
  2. Collect into pandas DataFrame with a row cap
  3. Pre-aggregate: compute derived metrics, rank by spend/waste
  4. Return compact summary dict — this is what goes to Gemini

The AI never sees raw rows. It sees pre-aggregated tables.
"""
import logging
from typing import Generator
import pandas as pd

# Opt in to future pandas downcasting behaviour — silences FutureWarning
pd.set_option("future.no_silent_downcasting", True)

from google.ads.googleads.client import GoogleAdsClient
from google.ads.googleads.errors import GoogleAdsException

from config import settings

logger = logging.getLogger(__name__)


# ── Client factory ────────────────────────────────────────────────────────────

def _client() -> GoogleAdsClient:
    return GoogleAdsClient.load_from_dict({
        "developer_token":   settings.google_ads_developer_token,
        "client_id":         settings.google_ads_client_id,
        "client_secret":     settings.google_ads_client_secret,
        "refresh_token":     settings.google_ads_refresh_token,
        "login_customer_id": settings.google_ads_login_customer_id,
        "use_proto_plus":    True,
    })


# ── Low-level streaming fetch ─────────────────────────────────────────────────

def _stream_query(account_id: str, query: str) -> Generator[dict, None, None]:
    """
    Execute a GAQL query and yield one dict per row.
    Uses the SDK's paged iterator — rows are NOT all loaded into memory at once.
    Caller is responsible for capping iteration (row_limit).
    """
    client = _client()
    service = client.get_service("GoogleAdsService")
    try:
        for row in service.search(customer_id=account_id, query=query):
            yield row
    except GoogleAdsException as ex:
        msg = ex.failure.errors[0].message if ex.failure.errors else str(ex)
        logger.error("Google Ads API error for account %s: %s", account_id, msg)
        raise RuntimeError(f"Google Ads API error: {msg}") from ex


# ── Helper: micros → USD ──────────────────────────────────────────────────────

def _usd(micros: int) -> float:
    return micros / 1_000_000


# ── Module 1: Campaign data ───────────────────────────────────────────────────

def fetch_campaign_data(account_id: str, date_range: str) -> dict:
    """
    Returns a pre-aggregated summary dict ready for Gemini.
    Structure:
      account_totals: {...}
      campaigns: [ top N by spend, with all derived metrics ]
      enabled_modules: dict mapping campaign_id → list of enabled features
    """
    query = f"""
        SELECT
            campaign.id,
            campaign.name,
            campaign.status,
            campaign.advertising_channel_type,
            campaign_budget.amount_micros,
            metrics.cost_micros,
            metrics.impressions,
            metrics.clicks,
            metrics.conversions,
            metrics.conversions_value,
            metrics.search_impression_share,
            metrics.search_budget_lost_impression_share
        FROM campaign
        WHERE segments.date DURING {date_range}
          AND campaign.status != 'REMOVED'
        ORDER BY metrics.cost_micros DESC
    """

    rows = []
    for row in _stream_query(account_id, query):
        if len(rows) >= settings.max_campaigns:
            logger.warning("Campaign cap (%d) hit for account %s", settings.max_campaigns, account_id)
            break
        rows.append({
            "id":            str(row.campaign.id),
            "name":          row.campaign.name,
            "status":        row.campaign.status.name,
            "channel":       row.campaign.advertising_channel_type.name,
            "budget_usd":    _usd(row.campaign_budget.amount_micros),
            "spend_usd":     _usd(row.metrics.cost_micros),
            "impressions":   row.metrics.impressions,
            "clicks":        row.metrics.clicks,
            "conversions":   row.metrics.conversions,
            "conv_value":    row.metrics.conversions_value,
            "imp_share":     row.metrics.search_impression_share,
            "budget_lost_is": row.metrics.search_budget_lost_impression_share,
        })

    if not rows:
        return {"account_totals": {}, "campaigns": [], "enabled_modules": {}}

    df = pd.DataFrame(rows)

    # Derived metrics — do this in pandas, not in Gemini
    df["ctr"]  = (df["clicks"]  / df["impressions"].replace(0, pd.NA) * 100).fillna(0).round(2)
    df["cpc"]  = (df["spend_usd"] / df["clicks"].replace(0, pd.NA)).fillna(0).round(2)
    df["cpa"]  = (df["spend_usd"] / df["conversions"].replace(0, pd.NA)).fillna(0).round(2)
    df["roas"] = (df["conv_value"] / df["spend_usd"].replace(0, pd.NA)).fillna(0).round(2)

    # Account totals
    account_totals = {
        "total_spend_usd":     round(df["spend_usd"].sum(), 2),
        "total_conversions":   round(df["conversions"].sum(), 2),
        "total_conv_value":    round(df["conv_value"].sum(), 2),
        "overall_roas":        round(df["conv_value"].sum() / df["spend_usd"].sum(), 2) if df["spend_usd"].sum() else 0,
        "overall_cpa":         round(df["spend_usd"].sum() / df["conversions"].sum(), 2) if df["conversions"].sum() else 0,
        "overall_ctr":         round(df["clicks"].sum() / df["impressions"].sum() * 100, 2) if df["impressions"].sum() else 0,
        "overall_cpc":         round(df["spend_usd"].sum() / df["clicks"].sum(), 2) if df["clicks"].sum() else 0,
        "campaign_count":      len(df),
        "enabled_campaign_count": int((df["status"] == "ENABLED").sum()),
    }

    # Only send top N to Gemini — ranked by spend
    top = df.nlargest(settings.ai_campaign_top_n, "spend_usd")
    campaigns_for_ai = top[[
        "name", "status", "channel", "budget_usd", "spend_usd",
        "impressions", "clicks", "conversions", "conv_value",
        "ctr", "cpc", "cpa", "roas", "imp_share", "budget_lost_is"
    ]].to_dict(orient="records")

    logger.info("Campaign fetch: %d total → %d sent to AI for account %s",
                len(df), len(campaigns_for_ai), account_id)

    return {
        "account_totals": account_totals,
        "campaigns": campaigns_for_ai,
    }


# ── Module 2: Budget data ─────────────────────────────────────────────────────

def fetch_budget_data(account_id: str, date_range: str) -> dict:
    """
    Returns budget pacing and utilization per campaign.
    Key insight: we compute spend_rate vs budget to find overpacing/underpacing.
    """
    query = f"""
        SELECT
            campaign.id,
            campaign.name,
            campaign.status,
            campaign_budget.amount_micros,
            campaign_budget.has_recommended_budget,
            metrics.cost_micros,
            metrics.impressions,
            metrics.clicks,
            metrics.conversions,
            metrics.conversions_value,
            metrics.search_budget_lost_impression_share,
            metrics.search_rank_lost_impression_share
        FROM campaign
        WHERE segments.date DURING {date_range}
          AND campaign.status = 'ENABLED'
        ORDER BY metrics.cost_micros DESC
    """

    rows = []
    for row in _stream_query(account_id, query):
        if len(rows) >= settings.max_campaigns:
            break
        rows.append({
            "name":              row.campaign.name,
            "status":            row.campaign.status.name,
            "budget_usd":        _usd(row.campaign_budget.amount_micros),
            "has_rec_budget":    row.campaign_budget.has_recommended_budget,
            "spend_usd":         _usd(row.metrics.cost_micros),
            "impressions":       row.metrics.impressions,
            "clicks":            row.metrics.clicks,
            "conversions":       row.metrics.conversions,
            "conv_value":        row.metrics.conversions_value,
            "budget_lost_is":    row.metrics.search_budget_lost_impression_share,
            "rank_lost_is":      row.metrics.search_rank_lost_impression_share,
        })

    if not rows:
        return {"budget_summary": {}, "campaigns": []}

    df = pd.DataFrame(rows)

    # How many days in the range (approximate)
    range_days = {"LAST_7_DAYS": 7, "LAST_30_DAYS": 30, "LAST_90_DAYS": 90,
                  "THIS_MONTH": 30, "LAST_MONTH": 30}.get(date_range, 30)

    df["daily_spend_avg"] = (df["spend_usd"] / range_days).round(2)
    df["budget_util_pct"] = (df["daily_spend_avg"] / df["budget_usd"].replace(0, pd.NA) * 100).fillna(0).round(1)
    df["roas"]  = (df["conv_value"] / df["spend_usd"].replace(0, pd.NA)).fillna(0).round(2)
    df["cpa"]   = (df["spend_usd"] / df["conversions"].replace(0, pd.NA)).fillna(0).round(2)

    # Flag pacing status
    def pacing_status(row):
        if row["budget_util_pct"] >= 95:
            return "budget_capped"
        elif row["budget_util_pct"] >= 80:
            return "healthy"
        elif row["budget_util_pct"] >= 50:
            return "underpacing"
        else:
            return "severely_underpacing"

    df["pacing_status"] = df.apply(pacing_status, axis=1)

    budget_summary = {
        "total_daily_budget_usd":  round(df["budget_usd"].sum(), 2),
        "total_spend_usd":         round(df["spend_usd"].sum(), 2),
        "avg_daily_spend_usd":     round(df["daily_spend_avg"].sum(), 2),
        "budget_capped_count":     int((df["pacing_status"] == "budget_capped").sum()),
        "underpacing_count":       int(df["pacing_status"].isin(["underpacing", "severely_underpacing"]).sum()),
        "range_days":              range_days,
    }

    # Sort: budget_capped with high ROAS first (they need more budget),
    # then underpacing with low ROAS (they're wasting allocation)
    df["sort_key"] = df.apply(
        lambda r: (0 if r["pacing_status"] == "budget_capped" else 1, -r["roas"]),
        axis=1
    )
    top = df.nlargest(settings.ai_campaign_top_n, "spend_usd")
    campaigns_for_ai = top[[
        "name", "budget_usd", "spend_usd", "daily_spend_avg",
        "budget_util_pct", "roas", "cpa", "pacing_status",
        "budget_lost_is", "rank_lost_is", "has_rec_budget"
    ]].to_dict(orient="records")

    logger.info("Budget fetch: %d campaigns → %d sent to AI for account %s",
                len(df), len(campaigns_for_ai), account_id)

    return {"budget_summary": budget_summary, "campaigns": campaigns_for_ai}


# ── Module 3: Search term data ────────────────────────────────────────────────

def fetch_search_term_data(account_id: str, date_range: str) -> dict:
    """
    This is where data gets HUGE. A large account can have 100k+ search terms.

    Strategy:
    - Stream and cap at MAX_SEARCH_TERMS rows
    - Compute wasted spend = spend where conversions = 0
    - Send Gemini only top N worst offenders + top N opportunities
    """
    query = f"""
        SELECT
            search_term_view.search_term,
            search_term_view.status,
            campaign.name,
            ad_group.name,
            metrics.cost_micros,
            metrics.impressions,
            metrics.clicks,
            metrics.conversions,
            metrics.conversions_value
        FROM search_term_view
        WHERE segments.date DURING {date_range}
          AND metrics.cost_micros > 0
        ORDER BY metrics.cost_micros DESC
    """

    rows = []
    for row in _stream_query(account_id, query):
        if len(rows) >= settings.max_search_terms:
            logger.warning("Search term cap (%d) hit for account %s", settings.max_search_terms, account_id)
            break
        rows.append({
            "term":          row.search_term_view.search_term,
            "status":        row.search_term_view.status.name,
            "campaign":      row.campaign.name,
            "ad_group":      row.ad_group.name,
            "spend_usd":     _usd(row.metrics.cost_micros),
            "impressions":   row.metrics.impressions,
            "clicks":        row.metrics.clicks,
            "conversions":   row.metrics.conversions,
            "conv_value":    row.metrics.conversions_value,
        })

    if not rows:
        return {"summary": {}, "waste_terms": [], "opportunity_terms": []}

    df = pd.DataFrame(rows)
    df["cpa"]  = (df["spend_usd"] / df["conversions"].replace(0, pd.NA)).fillna(0).round(2)
    df["roas"] = (df["conv_value"] / df["spend_usd"].replace(0, pd.NA)).fillna(0).round(2)
    df["ctr"]  = (df["clicks"] / df["impressions"].replace(0, pd.NA) * 100).fillna(0).round(2)

    # Wasted spend = spend where conversions = 0
    waste_df = df[df["conversions"] == 0].copy()
    waste_df = waste_df.nlargest(settings.ai_search_term_top_n, "spend_usd")

    # Opportunity terms = converting well, not yet added as keywords
    opp_df = df[(df["conversions"] > 0) & (df["status"] == "NONE")].copy()
    opp_df = opp_df.nlargest(50, "conv_value")

    summary = {
        "total_terms_analyzed":    len(df),
        "total_spend_usd":         round(df["spend_usd"].sum(), 2),
        "total_wasted_spend_usd":  round(waste_df["spend_usd"].sum(), 2),
        "waste_term_count":        len(df[df["conversions"] == 0]),
        "converting_term_count":   int((df["conversions"] > 0).sum()),
        "opportunity_term_count":  len(opp_df),
    }

    logger.info("Search term fetch: %d total → %d waste + %d opp sent to AI for account %s",
                len(df), len(waste_df), len(opp_df), account_id)

    return {
        "summary": summary,
        "waste_terms": waste_df[[
            "term", "campaign", "ad_group", "spend_usd", "clicks", "conversions", "cpa"
        ]].to_dict(orient="records"),
        "opportunity_terms": opp_df[[
            "term", "campaign", "ad_group", "spend_usd", "clicks", "conversions", "conv_value", "roas"
        ]].to_dict(orient="records"),
    }


# ── Module 4: Keyword data ────────────────────────────────────────────────────

def fetch_keyword_data(account_id: str, date_range: str) -> dict:
    """
    Fetches keyword-level performance including quality score.
    Strategy: sort by spend, send top N + worst performers to Gemini.
    """
    query = f"""
        SELECT
            ad_group_criterion.keyword.text,
            ad_group_criterion.keyword.match_type,
            ad_group_criterion.quality_info.quality_score,
            ad_group_criterion.status,
            campaign.name,
            ad_group.name,
            metrics.cost_micros,
            metrics.impressions,
            metrics.clicks,
            metrics.conversions,
            metrics.conversions_value
        FROM keyword_view
        WHERE segments.date DURING {date_range}
          AND ad_group_criterion.status != 'REMOVED'
          AND campaign.status != 'REMOVED'
        ORDER BY metrics.cost_micros DESC
    """

    rows = []
    for row in _stream_query(account_id, query):
        if len(rows) >= settings.max_keywords:
            logger.warning("Keyword cap (%d) hit for account %s", settings.max_keywords, account_id)
            break
        rows.append({
            "keyword":       row.ad_group_criterion.keyword.text,
            "match_type":    row.ad_group_criterion.keyword.match_type.name,
            "quality_score": row.ad_group_criterion.quality_info.quality_score or 0,
            "status":        row.ad_group_criterion.status.name,
            "campaign":      row.campaign.name,
            "ad_group":      row.ad_group.name,
            "spend_usd":     _usd(row.metrics.cost_micros),
            "impressions":   row.metrics.impressions,
            "clicks":        row.metrics.clicks,
            "conversions":   row.metrics.conversions,
            "conv_value":    row.metrics.conversions_value,
        })

    if not rows:
        return {"summary": {}, "top_keywords": [], "problem_keywords": []}

    df = pd.DataFrame(rows)
    df["ctr"]  = (df["clicks"] / df["impressions"].replace(0, pd.NA) * 100).fillna(0).round(2)
    df["cpc"]  = (df["spend_usd"] / df["clicks"].replace(0, pd.NA)).fillna(0).round(2)
    df["cpa"]  = (df["spend_usd"] / df["conversions"].replace(0, pd.NA)).fillna(0).round(2)
    df["roas"] = (df["conv_value"] / df["spend_usd"].replace(0, pd.NA)).fillna(0).round(2)

    summary = {
        "total_keywords":    len(df),
        "active_keywords":   int((df["status"] == "ENABLED").sum()),
        "total_spend_usd":   round(df["spend_usd"].sum(), 2),
        "avg_quality_score": round(df[df["quality_score"] > 0]["quality_score"].mean(), 1) if (df["quality_score"] > 0).any() else 0,
        "low_qs_count":      int((df["quality_score"].between(1, 4)).sum()),
        "zero_impression_count": int((df["impressions"] == 0).sum()),
    }

    # Top performers (high spend, good ROAS) — show what's working
    top_df = df[df["spend_usd"] > 0].nlargest(settings.ai_keyword_top_n // 2, "spend_usd")

    # Problem keywords: low QS, high CPA, zero impressions on enabled keywords
    problem_df = df[
        ((df["quality_score"] > 0) & (df["quality_score"] <= 4)) |
        ((df["spend_usd"] > 5) & (df["conversions"] == 0)) |
        ((df["status"] == "ENABLED") & (df["impressions"] == 0))
    ].nlargest(settings.ai_keyword_top_n // 2, "spend_usd")

    cols = ["keyword", "match_type", "quality_score", "campaign", "ad_group",
            "spend_usd", "impressions", "clicks", "conversions", "ctr", "cpc", "cpa", "roas"]

    logger.info("Keyword fetch: %d total → %d top + %d problems sent to AI for account %s",
                len(df), len(top_df), len(problem_df), account_id)

    return {
        "summary": summary,
        "top_keywords": top_df[cols].to_dict(orient="records"),
        "problem_keywords": problem_df[cols].to_dict(orient="records"),
    }