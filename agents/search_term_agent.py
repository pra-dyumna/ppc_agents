"""
Module 3: Search Term Analysis Agent
──────────────────────────────────────
Identifies wasted spend on irrelevant queries and finds new keyword opportunities.
This is the highest-value module for most accounts.
"""
import json
import logging
from datetime import datetime, timezone

from core.gemini import ask_gemini
from models.base import SearchTermReport, SearchTermFinding, Action

logger = logging.getLogger(__name__)

SYSTEM = """
You are a Google Ads search term analyst inside AdTunez PPC platform.

You receive two lists:
1. waste_terms — search queries that spent money but got ZERO conversions (pure waste)
2. opportunity_terms — search queries that converted well but aren't added as keywords yet

For waste terms, classify each as:
- add_as_negative: clearly irrelevant to the business (wrong intent, wrong audience)
- monitor: spent a little, might convert with more data
- ignore: tiny spend, not worth action

For opportunity terms, classify each as:
- add_as_keyword: strong signal, should be added as exact or phrase match keyword
- monitor: good but needs more data

Be specific. Name actual queries. Explain why each is a problem or opportunity.
Respond ONLY with valid JSON. No preamble.
""".strip()


def _build_prompt(data: dict, date_range: str) -> str:
    schema = {
        "summary": "2-3 sentence overview of search term health and estimated wasted spend",
        "high_waste_terms": [{
            "search_term": "str", "campaign_name": "str", "ad_group_name": "str",
            "spend_usd": 0.0, "clicks": 0, "conversions": 0.0, "cpa": 0.0,
            "issue": "str", "recommendation": "add_as_negative|monitor|ignore",
            "match_type_suggestion": None
        }],
        "opportunity_terms": [{
            "search_term": "str", "campaign_name": "str", "ad_group_name": "str",
            "spend_usd": 0.0, "clicks": 0, "conversions": 0.0, "cpa": 0.0,
            "issue": "str", "recommendation": "add_as_keyword|monitor",
            "match_type_suggestion": "EXACT|PHRASE"
        }],
        "actions": [{"priority": 1, "action_type": "add|remove",
                     "target_type": "search_term", "target_name": "str",
                     "current_value": "active", "recommended_value": "str",
                     "reason": "str", "expected_roas_delta": "str",
                     "expected_cpa_delta": "str", "impact": "high|medium|low"}]
    }
    return f"""
Search term analysis ({date_range}):
Account summary: {json.dumps(data['summary'], indent=2)}

Top wasted spend terms (zero conversions, sorted by spend):
{json.dumps(data['waste_terms'], indent=2)}

Top opportunity terms (converting, not yet as keywords):
{json.dumps(data['opportunity_terms'], indent=2)}

Return this JSON schema:
{json.dumps(schema, indent=2)}
""".strip()


def run(account_id: str, date_range: str, raw_data: dict) -> SearchTermReport:
    logger.info("Running search term agent for account %s", account_id)
    data = ask_gemini(SYSTEM, _build_prompt(raw_data, date_range))
    s = raw_data["summary"]

    return SearchTermReport(
        account_id=account_id,
        date_range=date_range,
        generated_at=datetime.now(timezone.utc).isoformat(),
        total_search_terms_analyzed=s.get("total_terms_analyzed", 0),
        total_wasted_spend_usd=s.get("total_wasted_spend_usd", 0),
        summary=data.get("summary", ""),
        high_waste_terms=[SearchTermFinding(**t) for t in data.get("high_waste_terms", [])],
        opportunity_terms=[SearchTermFinding(**t) for t in data.get("opportunity_terms", [])],
        actions=[Action(**a) for a in data.get("actions", [])],
    )