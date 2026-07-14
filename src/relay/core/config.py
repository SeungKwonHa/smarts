"""Runtime configuration via pydantic-settings.

All values come from environment variables (see .env.example).
Secrets are NEVER committed; sensitive fields are marked with repr=False.
"""

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    # ── Environment ────────────────────────────────────────────────────────────
    relay_env: str = "dev"
    relay_dry_run: bool = True  # True = no real external writes; log intent only

    # ── Database ───────────────────────────────────────────────────────────────
    database_url: str = "postgresql+asyncpg://relay:relay_dev_password@localhost:5432/relay"

    # ── Redis ──────────────────────────────────────────────────────────────────
    redis_url: str = "redis://localhost:6379/0"

    # ── LLM (LongCat API) ──────────────────────────────────────────────────────
    llm_provider: str = "longcat"
    llm_base_url: str = ""
    llm_api_key: str = Field(default="", repr=False)
    llm_model_t0: str = ""
    llm_model_t1: str = ""
    llm_model_t2: str = ""
    llm_timeout_s: int = 60
    llm_max_retries: int = 2
    llm_daily_budget_tokens: int = 30_000_000

    # ── FX ────────────────────────────────────────────────────────────────────
    fx_api_url: str = "https://open.er-api.com/v6/latest"
    fx_buffer: float = 1.03  # 3% buffer over spot rate in pricing

    # ── Marketplace ────────────────────────────────────────────────────────────
    naver_client_id: str = ""
    naver_client_secret: str = Field(default="", repr=False)
    naver_seller_id: str = ""

    # ── Sourcing ──────────────────────────────────────────────────────────────
    rakuten_app_id: str = ""
    rakuten_access_key: str = Field(default="", repr=False)

    # ── Observability ─────────────────────────────────────────────────────────
    langfuse_host: str = ""
    langfuse_public_key: str = ""
    langfuse_secret_key: str = Field(default="", repr=False)

    # ── App ───────────────────────────────────────────────────────────────────
    secret_key: str = Field(default="change-me-in-production", repr=False)

    # ── Operational limits (also tunable at runtime via app_config table) ─────
    publish_rate_daily: int = 300          # max new listings/day/store during ramp
    auto_pay_limit_krw: int = 0            # 0 = all purchases require HITL (M1)
    auto_pay_daily_cap_krw: int = 0
    stock_staleness_alert_hours: int = 36  # StockMonitor SLA
    cancel_rate_throttle_threshold: float = 0.02  # 2% → throttle publishes

    @property
    def is_prod(self) -> bool:
        return self.relay_env == "prod"

    @property
    def llm_configured(self) -> bool:
        return bool(self.llm_base_url and self.llm_api_key and self.llm_model_t0)


# Singleton — import this everywhere
settings = Settings()
