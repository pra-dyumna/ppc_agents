"""
Module 2: Budget Analysis Agent
─────────────────────────────────
Analyses budget pacing, utilisation, and recommends reallocation.
"""
import json
import logging
from datetime import datetime, timezone

from core.gemini import ask_gemini
from models.base import BudgetReport, BudgetIssue, BudgetShift, Action

logger = logging.getLogger(__name__)

SYSTEM = """
You are a Google Ads budget optimisation specialist inside AdTunez PPC platform.

Analyse budget pacing and utilisation data and identify:
1. Campaigns that are budget-capped with good ROAS (need more budget)
2. Campaigns that are severely underpacing (wasting budget allocation)
3. Campaigns burning budget with poor ROAS (should be cut)
4. Budget shifts that would improve overall account ROAS

Pacing status meanings:
- budget_capped: spending ≥95% of daily budget — losing impressions
- healthy: 80-95% utilisation
- underpacing: 50-80% utilisation — not spending what it should
- severely_underpacing: <50% utilisation — serious problem

budget_lost_is = impression share lost due to budget (higher = more capped)
rank_lost_is = impression share lost due to ad rank (different problem — not budget related)

IMPORTANT OUTPUT RULES:
- Respond ONLY with valid JSON. No preamble, no explanation outside JSON.
- Keep ALL string values concise — max 20 words per field.
- "detail" and "reason" fields: 1 sentence only, under 15 words.
- "suggested_fix" and "expected_impact": 1 sentence only, under 15 words.
- Do NOT write paragraphs. Every string field must be brief.
""".strip()


def _build_prompt(data: dict, date_range: str) -> str:
    schema = {
        "summary": "2-3 sentence budget health overview",
        "issues": [{"campaign_name": "str", "issue_type": "str", "severity": "critical|warning|info",
                    "daily_budget_usd": 0.0, "current_spend_rate_usd": 0.0,
                    "detail": "str", "suggested_fix": "str"}],
        "budget_shifts": [{"from_campaign": "str", "to_campaign": "str",
                           "daily_shift_usd": 0.0, "reason": "str", "expected_impact": "str"}],
        "actions": [{"priority": 1, "action_type": "increase|decrease",
                     "target_type": "campaign", "target_name": "str",
                     "current_value": "str", "recommended_value": "str",
                     "reason": "str", "expected_roas_delta": "str",
                     "expected_cpa_delta": "str", "impact": "high|medium|low"}]
    }
    return f"""
Budget summary ({date_range}):
{json.dumps(data['budget_summary'], indent=2)}

Campaign budget data:
{json.dumps(data['campaigns'], indent=2)}

Return this JSON schema:
{json.dumps(schema, indent=2)}
""".strip()


def run(account_id: str, date_range: str, raw_data: dict) -> BudgetReport:
    logger.info("Running budget agent for account %s", account_id)
    data = ask_gemini(SYSTEM, _build_prompt(raw_data, date_range))
    bs = raw_data["budget_summary"]

    return BudgetReport(
        account_id=account_id,
        date_range=date_range,
        generated_at=datetime.now(timezone.utc).isoformat(),
        total_daily_budget_usd=bs.get("total_daily_budget_usd", 0),
        total_spend_usd=bs.get("total_spend_usd", 0),
        budget_utilization_pct=round(
            bs.get("avg_daily_spend_usd", 0) / bs.get("total_daily_budget_usd", 1) * 100, 1
        ),
        summary=data.get("summary", ""),
        issues=[BudgetIssue(**i) for i in data.get("issues", [])],
        budget_shifts=[BudgetShift(**b) for b in data.get("budget_shifts", [])],
        actions=[Action(**a) for a in data.get("actions", [])],
    )