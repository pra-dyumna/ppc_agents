"""
AdTunez AI — Gradio Testing UI
────────────────────────────────
Test all 4 analysis modules from a single interface.
Run: python gradio_ui.py
"""
import json
import sys
import os

# Make sure project root is on path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import gradio as gr
import pandas as pd

from config import settings
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

DATE_RANGES = [
    "LAST_7_DAYS",
    "LAST_30_DAYS",
    "LAST_90_DAYS",
    "THIS_MONTH",
    "LAST_MONTH",
]

# ── Helpers ───────────────────────────────────────────────────────────────────

def _health_badge(health: str, score: int) -> str:
    colors = {"healthy": "🟢", "needs_attention": "🟡", "critical": "🔴"}
    icon = colors.get(health, "⚪")
    return f"{icon}  {health.replace('_', ' ').title()}  —  Score: {score}/100"


def _fmt_usd(v) -> str:
    try:
        return f"${float(v):,.2f}"
    except Exception:
        return str(v)


def _severity_icon(s: str) -> str:
    return {"critical": "🔴", "warning": "🟡", "info": "🔵"}.get(s.lower(), "⚪")


def _impact_icon(i: str) -> str:
    return {"high": "⬆", "medium": "➡", "low": "⬇"}.get(i.lower(), "")


def _actions_df(actions: list) -> pd.DataFrame:
    if not actions:
        return pd.DataFrame(columns=["#", "Action", "Target", "From", "To", "Impact", "ROAS delta", "CPA delta"])
    rows = []
    for a in sorted(actions, key=lambda x: x.get("priority", 99)):
        rows.append({
            "#":          a.get("priority", ""),
            "Action":     a.get("action_type", "").upper(),
            "Target":     a.get("target_name", ""),
            "From":       a.get("current_value", ""),
            "To":         a.get("recommended_value", ""),
            "Impact":     f"{_impact_icon(a.get('impact',''))} {a.get('impact','').title()}",
            "ROAS delta": a.get("expected_roas_delta", ""),
            "CPA delta":  a.get("expected_cpa_delta", ""),
        })
    return pd.DataFrame(rows)


# ── Module 1: Campaign ────────────────────────────────────────────────────────

def run_campaign(account_id: str, date_range: str):
    account_id = account_id.strip().replace("-", "")
    if not account_id:
        return "❌ Enter a Google Ads account ID", None, None, None, None, None

    try:
        raw = fetch_campaign_data(account_id, date_range)
        if not raw.get("campaigns"):
            return "❌ No campaign data found for this account / date range.", None, None, None, None, None

        report = campaign_agent.run(account_id, date_range, raw)

        # --- Summary text
        totals = raw["account_totals"]
        summary_md = f"""
## Account Summary

| Metric | Value |
|--------|-------|
| Total Spend | {_fmt_usd(report.total_spend_usd)} |
| Total Conversions | {report.total_conversions:,.1f} |
| Conversion Value | {_fmt_usd(report.total_conversion_value_usd)} |
| Overall ROAS | {report.overall_roas:.2f}x |
| Overall CPA | {_fmt_usd(report.overall_cpa)} |
| Overall CTR | {report.overall_ctr:.2f}% |
| Overall CPC | {_fmt_usd(report.overall_cpc)} |
| Campaigns | {totals.get('campaign_count', 0)} total / {totals.get('enabled_campaign_count', 0)} enabled |

---

### {_health_badge(report.overall_health, report.health_score)}

**Executive Summary**

{report.executive_summary}

---

**Performance Narrative**

{report.performance_narrative}

---

**Top Performing:** {", ".join(report.top_performing_campaigns) or "—"}

**Underperforming:** {", ".join(report.underperforming_campaigns) or "—"}
""".strip()

        # --- Issues table
        if report.issues:
            issue_rows = [
                {
                    "Severity": f"{_severity_icon(i.issue_type)} {i.issue_type}",
                    "Campaign": i.campaign_name,
                    "Type": i.issue_type,
                    "Detail": i.detail,
                    "Fix": i.suggested_fix,
                }
                for i in sorted(report.issues, key=lambda x: ["critical","warning","info"].index(x.severity))
            ]
            issues_df = pd.DataFrame(issue_rows)
        else:
            issues_df = pd.DataFrame(columns=["Severity", "Campaign", "Type", "Detail", "Fix"])

        # --- Actions table
        actions_df = _actions_df([a.model_dump() for a in report.actions])

        # --- Campaign data table
        camp_rows = []
        for c in raw["campaigns"]:
            camp_rows.append({
                "Campaign": c["name"],
                "Status": c["status"],
                "Budget/day": _fmt_usd(c["budget_usd"]),
                "Spend": _fmt_usd(c["spend_usd"]),
                "CTR": f"{c['ctr']:.2f}%",
                "CPC": _fmt_usd(c["cpc"]),
                "Conv.": f"{c['conversions']:.1f}",
                "CPA": _fmt_usd(c["cpa"]),
                "ROAS": f"{c['roas']:.2f}x",
            })
        camp_df = pd.DataFrame(camp_rows)

        # --- Raw JSON
        raw_json = json.dumps(report.model_dump(), indent=2)

        return summary_md, issues_df, actions_df, camp_df, raw_json, None

    except Exception as e:
        return f"❌ Error: {e}", None, None, None, None, str(e)


# ── Module 2: Budget ──────────────────────────────────────────────────────────

def run_budget(account_id: str, date_range: str):
    account_id = account_id.strip().replace("-", "")
    if not account_id:
        return "❌ Enter a Google Ads account ID", None, None, None, None

    try:
        raw = fetch_budget_data(account_id, date_range)
        if not raw.get("campaigns"):
            return "❌ No budget data found.", None, None, None, None

        report = budget_agent.run(account_id, date_range, raw)
        bs = raw["budget_summary"]

        summary_md = f"""
## Budget Overview

| Metric | Value |
|--------|-------|
| Total Daily Budget | {_fmt_usd(bs.get('total_daily_budget_usd', 0))} |
| Total Spend | {_fmt_usd(bs.get('total_spend_usd', 0))} |
| Avg Daily Spend | {_fmt_usd(bs.get('avg_daily_spend_usd', 0))} |
| Budget Utilisation | {report.budget_utilization_pct:.1f}% |
| Budget-Capped Campaigns | {bs.get('budget_capped_count', 0)} |
| Underpacing Campaigns | {bs.get('underpacing_count', 0)} |

---

{report.summary}
""".strip()

        # Issues
        issue_rows = [
            {
                "Severity": f"{_severity_icon(i.severity)} {i.severity.title()}",
                "Campaign": i.campaign_name,
                "Type": i.issue_type,
                "Daily Budget": _fmt_usd(i.daily_budget_usd),
                "Spend Rate": _fmt_usd(i.current_spend_rate_usd),
                "Detail": i.detail,
                "Fix": i.suggested_fix,
            }
            for i in report.issues
        ]
        issues_df = pd.DataFrame(issue_rows) if issue_rows else pd.DataFrame(
            columns=["Severity", "Campaign", "Type", "Daily Budget", "Spend Rate", "Detail", "Fix"]
        )

        # Budget shifts
        shift_rows = [
            {
                "From Campaign": s.from_campaign,
                "To Campaign": s.to_campaign,
                "Shift/day": _fmt_usd(s.daily_shift_usd),
                "Reason": s.reason,
                "Expected Impact": s.expected_impact,
            }
            for s in report.budget_shifts
        ]
        shifts_df = pd.DataFrame(shift_rows) if shift_rows else pd.DataFrame(
            columns=["From Campaign", "To Campaign", "Shift/day", "Reason", "Expected Impact"]
        )

        actions_df = _actions_df([a.model_dump() for a in report.actions])
        raw_json = json.dumps(report.model_dump(), indent=2)

        return summary_md, issues_df, shifts_df, actions_df, raw_json

    except Exception as e:
        return f"❌ Error: {e}", None, None, None, str(e)


# ── Module 3: Search Terms ────────────────────────────────────────────────────

def run_search_terms(account_id: str, date_range: str):
    account_id = account_id.strip().replace("-", "")
    if not account_id:
        return "❌ Enter a Google Ads account ID", None, None, None, None

    try:
        raw = fetch_search_term_data(account_id, date_range)
        s = raw.get("summary", {})

        if not raw.get("waste_terms") and not raw.get("opportunity_terms"):
            return "❌ No search term data found.", None, None, None, None

        report = search_term_agent.run(account_id, date_range, raw)

        summary_md = f"""
## Search Term Analysis

| Metric | Value |
|--------|-------|
| Total Terms Analysed | {report.total_search_terms_analyzed:,} |
| Wasted Spend (zero conv.) | {_fmt_usd(report.total_wasted_spend_usd)} |
| Waste Terms Found | {s.get('waste_term_count', 0):,} |
| Converting Terms | {s.get('converting_term_count', 0):,} |
| Opportunity Terms | {s.get('opportunity_term_count', 0):,} |

---

{report.summary}
""".strip()

        # Waste terms
        waste_rows = [
            {
                "Recommendation": t.recommendation.replace("_", " ").upper(),
                "Search Term": t.search_term,
                "Campaign": t.campaign_name,
                "Ad Group": t.ad_group_name,
                "Spend": _fmt_usd(t.spend_usd),
                "Clicks": t.clicks,
                "Conv.": t.conversions,
                "Issue": t.issue,
            }
            for t in sorted(report.high_waste_terms, key=lambda x: -x.spend_usd)
        ]
        waste_df = pd.DataFrame(waste_rows) if waste_rows else pd.DataFrame(
            columns=["Recommendation", "Search Term", "Campaign", "Ad Group", "Spend", "Clicks", "Conv.", "Issue"]
        )

        # Opportunity terms
        opp_rows = [
            {
                "Recommendation": t.recommendation.replace("_", " ").upper(),
                "Search Term": t.search_term,
                "Match Type": t.match_type_suggestion or "—",
                "Campaign": t.campaign_name,
                "Conv.": t.conversions,
                "Spend": _fmt_usd(t.spend_usd),
                "Issue": t.issue,
            }
            for t in report.opportunity_terms
        ]
        opp_df = pd.DataFrame(opp_rows) if opp_rows else pd.DataFrame(
            columns=["Recommendation", "Search Term", "Match Type", "Campaign", "Conv.", "Spend", "Issue"]
        )

        actions_df = _actions_df([a.model_dump() for a in report.actions])
        raw_json = json.dumps(report.model_dump(), indent=2)

        return summary_md, waste_df, opp_df, actions_df, raw_json

    except Exception as e:
        return f"❌ Error: {e}", None, None, None, str(e)


# ── Module 4: Keywords ────────────────────────────────────────────────────────

def run_keywords(account_id: str, date_range: str):
    account_id = account_id.strip().replace("-", "")
    if not account_id:
        return "❌ Enter a Google Ads account ID", None, None, None, None

    try:
        raw = fetch_keyword_data(account_id, date_range)
        s = raw.get("summary", {})

        if not raw.get("top_keywords") and not raw.get("problem_keywords"):
            return "❌ No keyword data found.", None, None, None, None

        report = keyword_agent.run(account_id, date_range, raw)

        summary_md = f"""
## Keyword Portfolio

| Metric | Value |
|--------|-------|
| Total Keywords | {report.total_keywords_analyzed:,} |
| Active Keywords | {report.total_active_keywords:,} |
| Total Spend | {_fmt_usd(report.total_spend_usd)} |
| Avg Quality Score | {s.get('avg_quality_score', 0):.1f} / 10 |
| Low QS Keywords (≤4) | {s.get('low_qs_count', 0):,} |
| Zero Impression Keywords | {s.get('zero_impression_count', 0):,} |

---

{report.summary}
""".strip()

        def _kw_row(k):
            qs = k.quality_score or 0
            qs_display = f"{'🔴' if qs <= 4 else '🟡' if qs <= 6 else '🟢'} {qs}" if qs else "—"
            return {
                "Keyword": k.keyword_text,
                "Match": k.match_type,
                "QS": qs_display,
                "Campaign": k.campaign_name,
                "Ad Group": k.ad_group_name,
                "Spend": _fmt_usd(k.spend_usd),
                "Conv.": k.conversions,
                "CPA": _fmt_usd(k.cpa),
                "ROAS": f"{k.roas:.2f}x",
                "Issue": k.issue,
                "Fix": k.recommendation,
            }

        problem_df = pd.DataFrame([_kw_row(k) for k in report.problem_keywords]) if report.problem_keywords else pd.DataFrame()
        opp_df = pd.DataFrame([_kw_row(k) for k in report.opportunity_keywords]) if report.opportunity_keywords else pd.DataFrame()

        actions_df = _actions_df([a.model_dump() for a in report.actions])
        raw_json = json.dumps(report.model_dump(), indent=2)

        return summary_md, problem_df, opp_df, actions_df, raw_json

    except Exception as e:
        return f"❌ Error: {e}", None, None, None, str(e)


# ── Build Gradio UI ───────────────────────────────────────────────────────────

custom_css = """
.gradio-container { font-family: 'Inter', sans-serif; }
.tab-nav button { font-size: 15px; font-weight: 500; }
.module-header { background: #0f172a; color: #e2e8f0; padding: 16px 20px; border-radius: 8px; margin-bottom: 12px; }
footer { display: none !important; }
"""

with gr.Blocks(
    title="AdTunez AI — Performance Testing",
    theme=gr.themes.Base(
        primary_hue="violet",
        neutral_hue="slate",
        font=gr.themes.GoogleFont("Inter"),
    ),
    css=custom_css,
) as demo:

    gr.Markdown("# AdTunez AI — Performance Analysis")
    gr.Markdown("Test all 4 AI analysis modules against a real Google Ads account.")

    # ── Shared inputs at top ──────────────────────────────────────────────────
    with gr.Row():
        account_id_input = gr.Textbox(
            label="Google Ads Account ID",
            placeholder="1234567890  (no dashes)",
            scale=3,
        )
        date_range_input = gr.Dropdown(
            choices=DATE_RANGES,
            value="LAST_30_DAYS",
            label="Date Range",
            scale=1,
        )

    gr.Markdown("---")

    # ── Tabs ──────────────────────────────────────────────────────────────────
    with gr.Tabs():

        # ── Tab 1: Campaign ───────────────────────────────────────────────────
        with gr.TabItem("📊  Campaign Performance"):
            gr.Markdown("Analyses ROAS, CTR, CPA per campaign. Identifies what's working and what's dragging results.")
            camp_btn = gr.Button("Run Campaign Analysis", variant="primary", size="lg")

            with gr.Row():
                camp_summary = gr.Markdown(label="Summary")

            with gr.Tabs():
                with gr.TabItem("Issues"):
                    camp_issues = gr.Dataframe(wrap=True, interactive=False)
                with gr.TabItem("Recommended Actions"):
                    camp_actions = gr.Dataframe(wrap=True, interactive=False)
                with gr.TabItem("All Campaigns"):
                    camp_table = gr.Dataframe(wrap=True, interactive=False)
                with gr.TabItem("Raw JSON"):
                    camp_json = gr.Code(language="json", label="Full report JSON")
                with gr.TabItem("Error Log"):
                    camp_error = gr.Textbox(label="Error", lines=5)

            camp_btn.click(
                fn=run_campaign,
                inputs=[account_id_input, date_range_input],
                outputs=[camp_summary, camp_issues, camp_actions, camp_table, camp_json, camp_error],
            )

        # ── Tab 2: Budget ─────────────────────────────────────────────────────
        with gr.TabItem("💰  Budget Analysis"):
            gr.Markdown("Finds budget-capped campaigns with good ROAS (need more budget) and underpacing campaigns wasting allocation.")
            budget_btn = gr.Button("Run Budget Analysis", variant="primary", size="lg")

            with gr.Row():
                budget_summary = gr.Markdown(label="Summary")

            with gr.Tabs():
                with gr.TabItem("Budget Issues"):
                    budget_issues = gr.Dataframe(wrap=True, interactive=False)
                with gr.TabItem("Budget Shifts"):
                    budget_shifts = gr.Dataframe(wrap=True, interactive=False)
                with gr.TabItem("Actions"):
                    budget_actions = gr.Dataframe(wrap=True, interactive=False)
                with gr.TabItem("Raw JSON"):
                    budget_json = gr.Code(language="json", label="Full report JSON")

            budget_btn.click(
                fn=run_budget,
                inputs=[account_id_input, date_range_input],
                outputs=[budget_summary, budget_issues, budget_shifts, budget_actions, budget_json],
            )

        # ── Tab 3: Search Terms ───────────────────────────────────────────────
        with gr.TabItem("🔍  Search Terms"):
            gr.Markdown("Detects wasted spend on irrelevant queries and finds opportunity terms to add as keywords. Works on 50k+ search terms.")
            st_btn = gr.Button("Run Search Term Analysis", variant="primary", size="lg")

            with gr.Row():
                st_summary = gr.Markdown(label="Summary")

            with gr.Tabs():
                with gr.TabItem("Waste Terms (add as negatives)"):
                    st_waste = gr.Dataframe(wrap=True, interactive=False)
                with gr.TabItem("Opportunity Terms (add as keywords)"):
                    st_opp = gr.Dataframe(wrap=True, interactive=False)
                with gr.TabItem("Actions"):
                    st_actions = gr.Dataframe(wrap=True, interactive=False)
                with gr.TabItem("Raw JSON"):
                    st_json = gr.Code(language="json", label="Full report JSON")

            st_btn.click(
                fn=run_search_terms,
                inputs=[account_id_input, date_range_input],
                outputs=[st_summary, st_waste, st_opp, st_actions, st_json],
            )

        # ── Tab 4: Keywords ───────────────────────────────────────────────────
        with gr.TabItem("🎯  Keywords"):
            gr.Markdown("Analyses Quality Score, bid efficiency, match type strategy, and keyword health across the full portfolio.")
            kw_btn = gr.Button("Run Keyword Analysis", variant="primary", size="lg")

            with gr.Row():
                kw_summary = gr.Markdown(label="Summary")

            with gr.Tabs():
                with gr.TabItem("Problem Keywords"):
                    kw_problems = gr.Dataframe(wrap=True, interactive=False)
                with gr.TabItem("Opportunity Keywords"):
                    kw_opp = gr.Dataframe(wrap=True, interactive=False)
                with gr.TabItem("Actions"):
                    kw_actions = gr.Dataframe(wrap=True, interactive=False)
                with gr.TabItem("Raw JSON"):
                    kw_json = gr.Code(language="json", label="Full report JSON")

            kw_btn.click(
                fn=run_keywords,
                inputs=[account_id_input, date_range_input],
                outputs=[kw_summary, kw_problems, kw_opp, kw_actions, kw_json],
            )

        # ── Tab 5: Raw data (no AI) ───────────────────────────────────────────
        with gr.TabItem("🗂  Raw Data (no AI)"):
            gr.Markdown("Fetch pre-aggregated data without running AI. Fast — good for checking what the AI will see.")

            with gr.Row():
                raw_module = gr.Dropdown(
                    choices=["campaign", "budget", "search_terms", "keywords"],
                    value="campaign",
                    label="Module",
                    scale=1,
                )
                raw_btn = gr.Button("Fetch Raw Data", variant="secondary", scale=1)

            raw_output = gr.JSON(label="Pre-aggregated data sent to Gemini")

            def fetch_raw(account_id, date_range, module):
                account_id = account_id.strip().replace("-", "")
                if not account_id:
                    return {"error": "Enter an account ID"}
                fetchers = {
                    "campaign":     lambda: fetch_campaign_data(account_id, date_range),
                    "budget":       lambda: fetch_budget_data(account_id, date_range),
                    "search_terms": lambda: fetch_search_term_data(account_id, date_range),
                    "keywords":     lambda: fetch_keyword_data(account_id, date_range),
                }
                try:
                    return fetchers[module]()
                except Exception as e:
                    return {"error": str(e)}

            raw_btn.click(
                fn=fetch_raw,
                inputs=[account_id_input, date_range_input, raw_module],
                outputs=[raw_output],
            )

    gr.Markdown("---")
    gr.Markdown(
        f"Connected to: **Google Ads** (account: {settings.google_ads_login_customer_id})  |  "
        f"AI: **Gemini 1.5 Flash**  |  API env: **{settings.app_env}**",
        elem_classes=["footer-info"]
    )


if __name__ == "__main__":
    demo.launch(
        server_name="0.0.0.0",
        server_port=7860,
        share=False,         # set True to get a public gradio.live link
        show_error=True,
    )