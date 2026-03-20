"""
Performance Analysis API
─────────────────────────
4 endpoints, one per analysis module.
Each endpoint follows the same pattern:
  1. Validate request
  2. Fetch + pre-aggregate data from Google Ads
  3. Run AI agent
  4. Return structured report
"""
import logging
from fastapi import APIRouter, HTTPException, Query

from models.base import AnalysisRequest
from services.google_ads import (
    fetch_campaign_data,
    fetch_budget_data,
    fetch_search_term_data,
    fetch_keyword_data,
)
import agents.campaign_agent as campaign_agent
import agents.budget_agent as budget_agent
import agents.search_term_agent as search_term_agent
import agents.keyword_agent as keyword_agent

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/v1", tags=["Performance Analysis"])


def _handle(fn, account_id: str, module: str):
    """Shared error wrapper for all endpoints."""
    try:
        return fn()
    except HTTPException:
        raise
    except RuntimeError as e:
        logger.error("[%s] account=%s error=%s", module, account_id, e)
        raise HTTPException(status_code=502, detail=str(e))
    except Exception as e:
        logger.exception("[%s] Unexpected error for account %s", module, account_id)
        raise HTTPException(status_code=500, detail=f"Internal error: {e}")


# ── Module 1: Campaign performance ───────────────────────────────────────────

@router.post("/campaign", summary="AI campaign performance analysis")
async def analyse_campaigns(body: AnalysisRequest):
    """
    Analyses all enabled campaigns: ROAS, CTR, CPA, spend efficiency.
    Returns executive summary, issues, and prioritised actions.
    """
    def run():
        raw = fetch_campaign_data(body.account_id, body.date_range)
        if not raw["campaigns"]:
            raise HTTPException(404, f"No campaign data found for {body.account_id}")
        report = campaign_agent.run(body.account_id, body.date_range, raw)
        return {"success": True, "module": "campaign", "report": report.model_dump()}

    return _handle(run, body.account_id, "campaign")


# ── Module 2: Budget analysis ─────────────────────────────────────────────────

@router.post("/budget", summary="AI budget pacing and reallocation analysis")
async def analyse_budget(body: AnalysisRequest):
    """
    Analyses budget pacing, utilisation, and recommends shifts.
    Identifies capped campaigns that need more budget and
    underpacing campaigns wasting allocation.
    """
    def run():
        raw = fetch_budget_data(body.account_id, body.date_range)
        if not raw["campaigns"]:
            raise HTTPException(404, f"No budget data found for {body.account_id}")
        report = budget_agent.run(body.account_id, body.date_range, raw)
        return {"success": True, "module": "budget", "report": report.model_dump()}

    return _handle(run, body.account_id, "budget")


# ── Module 3: Search term analysis ───────────────────────────────────────────

@router.post("/search-terms", summary="AI search term waste detection and opportunity mining")
async def analyse_search_terms(body: AnalysisRequest):
    """
    Analyses search term report to find:
    - Wasted spend on irrelevant queries (recommend as negatives)
    - Converting queries not yet added as keywords (opportunities)

    This is usually the highest-ROI analysis for accounts with search campaigns.
    Works on large accounts — 50k+ search terms pre-aggregated before AI analysis.
    """
    def run():
        raw = fetch_search_term_data(body.account_id, body.date_range)
        if not raw["waste_terms"] and not raw["opportunity_terms"]:
            raise HTTPException(404, f"No search term data found for {body.account_id}")
        report = search_term_agent.run(body.account_id, body.date_range, raw)
        return {"success": True, "module": "search_terms", "report": report.model_dump()}

    return _handle(run, body.account_id, "search_terms")


# ── Module 4: Keyword analysis ────────────────────────────────────────────────

@router.post("/keywords", summary="AI keyword performance and quality score analysis")
async def analyse_keywords(body: AnalysisRequest):
    """
    Analyses keyword portfolio:
    - Low quality score keywords (driving up CPCs)
    - High-spend zero-conversion keywords
    - Match type strategy issues
    - Bid efficiency
    """
    def run():
        raw = fetch_keyword_data(body.account_id, body.date_range)
        if not raw["top_keywords"] and not raw["problem_keywords"]:
            raise HTTPException(404, f"No keyword data found for {body.account_id}")
        report = keyword_agent.run(body.account_id, body.date_range, raw)
        return {"success": True, "module": "keywords", "report": report.model_dump()}

    return _handle(run, body.account_id, "keywords")


# ── Quick raw data endpoint (no AI, fast) ────────────────────────────────────

@router.get("/raw/{module}/{account_id}", summary="Raw aggregated data without AI (fast)")
async def raw_data(
    module: str,
    account_id: str,
    date_range: str = Query(default="LAST_30_DAYS"),
):
    """
    Returns pre-aggregated data without running AI.
    Useful for dashboards, debugging, or building your own frontend tables.
    module: campaign | budget | search_terms | keywords
    """
    fetchers = {
        "campaign":     lambda: fetch_campaign_data(account_id, date_range),
        "budget":       lambda: fetch_budget_data(account_id, date_range),
        "search_terms": lambda: fetch_search_term_data(account_id, date_range),
        "keywords":     lambda: fetch_keyword_data(account_id, date_range),
    }
    if module not in fetchers:
        raise HTTPException(400, f"Unknown module '{module}'. Choose: {list(fetchers)}")

    def run():
        return {"success": True, "module": module, "data": fetchers[module]()}

    return _handle(run, account_id, module)