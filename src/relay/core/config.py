"""Runtime configuration via pydantic-settings.

All values come from environment variables (see .env.example).
Secrets are NEVER committed; sensitive fields are marked with repr=False.
"""

from typing import Any

from pydantic import Field, field_validator
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
    llm_timeout_s: int = 120
    llm_max_retries: int = 2
    llm_daily_budget_tokens: int = 30_000_000

    # ── FX ────────────────────────────────────────────────────────────────────
    fx_api_url: str = "https://open.er-api.com/v6/latest"
    fx_buffer: float = 1.03

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
    publish_rate_daily: int = 300
    auto_pay_limit_krw: int = 0
    auto_pay_daily_cap_krw: int = 0
    stock_staleness_alert_hours: int = 36
    cancel_rate_throttle_threshold: float = 0.02

    # ── Validators ────────────────────────────────────────────────────────────

    @field_validator(
        "llm_base_url",
        "llm_api_key",
        "llm_model_t0",
        "llm_model_t1",
        "llm_model_t2",
        "naver_client_id",
        "naver_client_secret",
        "naver_seller_id",
        "rakuten_app_id",
        "rakuten_access_key",
        "langfuse_host",
        "langfuse_public_key",
        "langfuse_secret_key",
        "secret_key",
        "database_url",
        "redis_url",
        "fx_api_url",
        mode="before",
    )
    @classmethod
    def strip_inline_comments(cls, v: Any) -> str:
        """Strip inline comments that pydantic-settings would parse as values.

        .env parsers that don't support inline comments would read:
            LLM_BASE_URL=   # comment
        as the string "# comment" instead of "". This validator catches that.
        """
        if not isinstance(v, str):
            return v
        stripped = v.strip()
        # If the entire value looks like an inline comment, reject it
        if stripped.startswith("#"):
            return ""
        # If there's an inline comment after whitespace, strip it
        # e.g. "https://api.example.com  # production" → "https://api.example.com"
        idx = stripped.find(" #")
        if idx != -1:
            stripped = stripped[:idx].strip()
        return stripped

    @property
    def is_prod(self) -> bool:
        return self.relay_env == "prod"

    @property
    def llm_configured(self) -> bool:
        return bool(self.llm_base_url and self.llm_api_key and self.llm_model_t0)


# Singleton — import this everywhere
settings = Settings()
