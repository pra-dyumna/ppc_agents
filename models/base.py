"""
Shared data models used across all 4 analysis modules.
"""
from pydantic import BaseModel, Field
from typing import Optional, Literal


# ── Common request ────────────────────────────────────────────────────────────

class AnalysisRequest(BaseModel):
    account_id: str = Field(..., description="Google Ads customer ID, no dashes")
    date_range: str = Field(
        default="LAST_30_DAYS",
        description="LAST_7_DAYS | LAST_30_DAYS | LAST_90_DAYS | THIS_MONTH | LAST_MONTH"
    )


# ── Severity / impact enums ───────────────────────────────────────────────────

Severity  = Literal["critical", "warning", "info"]
Impact    = Literal["high", "medium", "low"]
Direction = Literal["increase", "decrease", "pause", "enable", "add", "remove"]


# ── Shared action model ───────────────────────────────────────────────────────

class Action(BaseModel):
    priority: int
    action_type: Direction
    target_type: str          # "campaign" | "ad_group" | "keyword" | "search_term"
    target_name: str
    current_value: str
    recommended_value: str
    reason: str
    expected_roas_delta: str  # e.g. "+0.4x" or "no change"
    expected_cpa_delta: str   # e.g. "-$12" or "no change"
    impact: Impact


# ── Module 1: Campaign performance ───────────────────────────────────────────

class CampaignIssue(BaseModel):
    campaign_name: str
    issue_type: str       # "Low ROAS" | "High CPA" | "Low CTR" | "Budget capped" | etc.
    severity: Severity
    detail: str
    suggested_fix: str

class CampaignReport(BaseModel):
    account_id: str
    date_range: str
    generated_at: str

    # Account totals
    total_spend_usd: float
    total_conversions: float
    total_conversion_value_usd: float
    overall_roas: float
    overall_cpa: float
    overall_ctr: float
    overall_cpc: float

    # AI output
    executive_summary: str
    performance_narrative: str
    overall_health: Literal["healthy", "needs_attention", "critical"]
    health_score: int              # 0-100

    top_performing_campaigns: list[str]
    underperforming_campaigns: list[str]
    issues: list[CampaignIssue]
    actions: list[Action]


# ── Module 2: Budget analysis ─────────────────────────────────────────────────

class BudgetShift(BaseModel):
    from_campaign: str
    to_campaign: str
    daily_shift_usd: float
    reason: str
    expected_impact: str

class BudgetIssue(BaseModel):
    campaign_name: str
    issue_type: str    # "Overpacing" | "Underpacing" | "Budget capped" | "Low ROAS burn" | etc.
    severity: Severity
    daily_budget_usd: float
    current_spend_rate_usd: float
    detail: str
    suggested_fix: str

class BudgetReport(BaseModel):
    account_id: str
    date_range: str
    generated_at: str

    total_daily_budget_usd: float
    total_spend_usd: float
    budget_utilization_pct: float

    summary: str
    issues: list[BudgetIssue]
    budget_shifts: list[BudgetShift]
    actions: list[Action]


# ── Module 3: Search term analysis ───────────────────────────────────────────

class SearchTermFinding(BaseModel):
    search_term: str
    campaign_name: str
    ad_group_name: str
    spend_usd: float
    clicks: int
    conversions: float
    cpa: float
    issue: str    # "Zero conversions high spend" | "Irrelevant query" | "Duplicate of keyword" | etc.
    recommendation: Literal["add_as_negative", "add_as_keyword", "monitor", "ignore"]
    match_type_suggestion: Optional[str] = None   # if add_as_keyword

class SearchTermReport(BaseModel):
    account_id: str
    date_range: str
    generated_at: str

    total_search_terms_analyzed: int
    total_wasted_spend_usd: float

    summary: str
    high_waste_terms: list[SearchTermFinding]    # add as negatives
    opportunity_terms: list[SearchTermFinding]   # add as keywords
    actions: list[Action]


# ── Module 4: Keyword analysis ────────────────────────────────────────────────

class KeywordFinding(BaseModel):
    keyword_text: str
    match_type: str
    campaign_name: str
    ad_group_name: str
    spend_usd: float
    clicks: int
    impressions: int
    conversions: float
    quality_score: Optional[int] = None
    cpc: float
    cpa: float
    roas: float
    issue: str   # "Low QS" | "High CPA" | "No impressions" | "Cannibalising" | etc.
    recommendation: str

class KeywordReport(BaseModel):
    account_id: str
    date_range: str
    generated_at: str

    total_keywords_analyzed: int
    total_active_keywords: int
    total_spend_usd: float

    summary: str
    problem_keywords: list[KeywordFinding]
    opportunity_keywords: list[KeywordFinding]
    actions: list[Action]


# ── Generic API response wrapper ──────────────────────────────────────────────

class APIResponse(BaseModel):
    success: bool
    account_id: str
    module: str
    data: Optional[dict] = None
    error: Optional[str] = None