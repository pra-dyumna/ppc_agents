"""
Module 4: Keyword Analysis Agent
──────────────────────────────────
Analyses keyword performance, quality scores, bid efficiency, and match type strategy.
"""
import json
import logging
from datetime import datetime, timezone

from core.gemini import ask_gemini
from models.base import KeywordReport, KeywordFinding, Action

logger = logging.getLogger(__name__)

SYSTEM = """
You are a Google Ads keyword strategist inside AdTunez PPC platform.

Analyse keyword performance data and identify:
1. Low Quality Score keywords (QS ≤ 4) — these raise CPCs for everything in the ad group
2. High-spend keywords with zero or low conversions — money drains
3. Keywords with zero impressions while enabled — bids too low, landing page issues, or policy
4. Over-broad match types on high-spend terms — generating waste
5. Opportunities to add exact match versions of converting phrase/broad keywords

Quality Score impact reminder:
- QS 10 = 50% CPC discount vs QS 5 baseline
- QS 1 = 400% CPC premium vs QS 5 baseline
- Fixing QS 3→8 on a $500/mo keyword saves ~$200/mo

Respond ONLY with valid JSON. No preamble.
""".strip()


def _build_prompt(data: dict, date_range: str) -> str:
    schema = {
        "summary": "2-3 sentence overview of keyword portfolio health",
        "problem_keywords": [{
            "keyword_text": "str", "match_type": "str", "campaign_name": "str",
            "ad_group_name": "str", "spend_usd": 0.0, "impressions": 0, "clicks": 0,
            "conversions": 0.0, "quality_score": 0, "cpc": 0.0, "cpa": 0.0, "roas": 0.0,
            "issue": "str", "recommendation": "str"
        }],
        "opportunity_keywords": [{
            "keyword_text": "str", "match_type": "str", "campaign_name": "str",
            "ad_group_name": "str", "spend_usd": 0.0, "impressions": 0, "clicks": 0,
            "conversions": 0.0, "quality_score": 0, "cpc": 0.0, "cpa": 0.0, "roas": 0.0,
            "issue": "str", "recommendation": "str"
        }],
        "actions": [{"priority": 1, "action_type": "increase|decrease|pause|add|remove",
                     "target_type": "keyword", "target_name": "str",
                     "current_value": "str", "recommended_value": "str",
                     "reason": "str", "expected_roas_delta": "str",
                     "expected_cpa_delta": "str", "impact": "high|medium|low"}]
    }
    return f"""
Keyword analysis ({date_range}):
Summary: {json.dumps(data['summary'], indent=2)}

Top keywords by spend:
{json.dumps(data['top_keywords'], indent=2)}

Problem keywords (low QS, high CPA, zero impressions):
{json.dumps(data['problem_keywords'], indent=2)}

Return this JSON schema:
{json.dumps(schema, indent=2)}
""".strip()


def run(account_id: str, date_range: str, raw_data: dict) -> KeywordReport:
    logger.info("Running keyword agent for account %s", account_id)
    data = ask_gemini(SYSTEM, _build_prompt(raw_data, date_range))
    s = raw_data["summary"]

    return KeywordReport(
        account_id=account_id,
        date_range=date_range,
        generated_at=datetime.now(timezone.utc).isoformat(),
        total_keywords_analyzed=s.get("total_keywords", 0),
        total_active_keywords=s.get("active_keywords", 0),
        total_spend_usd=s.get("total_spend_usd", 0),
        summary=data.get("summary", ""),
        problem_keywords=[KeywordFinding(**k) for k in data.get("problem_keywords", [])],
        opportunity_keywords=[KeywordFinding(**k) for k in data.get("opportunity_keywords", [])],
        actions=[Action(**a) for a in data.get("actions", [])],
    )