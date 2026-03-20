from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    gemini_api_key: str

    google_ads_developer_token: str
    google_ads_client_id: str
    google_ads_client_secret: str
    google_ads_refresh_token: str
    google_ads_login_customer_id: str

    app_env: str = "development"
    log_level: str = "INFO"

    # Pipeline limits
    max_campaigns: int = 500
    max_ad_groups: int = 2000
    max_search_terms: int = 50000
    max_keywords: int = 20000

    # How many rows go to Gemini after aggregation
    ai_campaign_top_n: int = 50
    ai_ad_group_top_n: int = 100
    ai_search_term_top_n: int = 100
    ai_keyword_top_n: int = 100


settings = Settings()