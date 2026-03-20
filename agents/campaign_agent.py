"""
Module 1: Campaign Performance Agent
──────────────────────────────────────
Analyses campaign-level ROAS, CTR, CPA, spend efficiency.
Identifies what's working, what's dragging the account down.
"""
import json
import logging
from datetime import datetime, timezone

from core.gemini import ask_gemini
from models.base import CampaignReport, CampaignIssue, Action

logger = logging.getLogger(__name__)

SYSTEM = """
You are a senior Google Ads performance analyst inside AdTunez PPC platform.

Analyse the campaign data and return a structured JSON performance report.

Scoring rules:
- ROAS ≥ 5x = excellent  |  3-5x = good  |  2-3x = acceptable  |  <2x = problem
- CTR ≥ 3% = strong  |  1-3% = normal  |  <1% = weak
- CPA: judge relative to conv_value, not absolute
- Budget capped (budget_lost_is > 0.2) + ROAS ≥ 3x = must increase budget
- High spend + ROAS < 1.5x = reallocate budget away from this campaign

Respond ONLY with valid JSON matching the exact schema provided. No preamble, no explanation.
""".strip()


def _build_prompt(data: dict, date_range: str) -> str:
    schema = {
        "executive_summary": "3-4 sentence overview of the account",
        "performance_narrative": "Detailed explanation: what is driving results, what is hurting results",
        "overall_health": "healthy | needs_attention | critical",
        "health_score": "integer 0-100",
        "top_performing_campaigns": ["campaign name"],
        "underperforming_campaigns": ["campaign name"],
        "issues": [{"campaign_name": "str", "issue_type": "str", "severity": "critical|warning|info",
                    "detail": "str", "suggested_fix": "str"}],
        "actions": [{"priority": 1, "action_type": "increase|decrease|pause|enable",
                     "target_type": "campaign", "target_name": "str",
                     "current_value": "str", "recommended_value": "str",
                     "reason": "str", "expected_roas_delta": "str",
                     "expected_cpa_delta": "str", "impact": "high|medium|low"}]
    }
    return f"""
Account totals:
{json.dumps(data['account_totals'], indent=2)}

Campaigns (top {len(data['campaigns'])} by spend, date range: {date_range}):
{json.dumps(data['campaigns'], indent=2)}

Return this JSON schema filled with real findings:
{json.dumps(schema, indent=2)}
""".strip()


def run(account_id: str, date_range: str, raw_data: dict) -> CampaignReport:
    logger.info("Running campaign agent for account %s", account_id)
    data = ask_gemini(SYSTEM, _build_prompt(raw_data, date_range))
    totals = raw_data["account_totals"]

    return CampaignReport(
        account_id=account_id,
        date_range=date_range,
        generated_at=datetime.now(timezone.utc).isoformat(),
        total_spend_usd=totals.get("total_spend_usd", 0),
        total_conversions=totals.get("total_conversions", 0),
        total_conversion_value_usd=totals.get("total_conv_value", 0),
        overall_roas=totals.get("overall_roas", 0),
        overall_cpa=totals.get("overall_cpa", 0),
        overall_ctr=totals.get("overall_ctr", 0),
        overall_cpc=totals.get("overall_cpc", 0),
        executive_summary=data.get("executive_summary", ""),
        performance_narrative=data.get("performance_narrative", ""),
        overall_health=data.get("overall_health", "needs_attention"),
        health_score=data.get("health_score", 50),
        top_performing_campaigns=data.get("top_performing_campaigns", []),
        underperforming_campaigns=data.get("underperforming_campaigns", []),
        issues=[CampaignIssue(**i) for i in data.get("issues", [])],
        actions=[Action(**a) for a in data.get("actions", [])],
    )