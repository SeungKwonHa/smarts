"""M3 Intelligence — add config seeds for TrendScout, GapAnalyzer, PromotionEngine.

Adds runtime-tunable thresholds for:
- TrendScout: score threshold, max candidates per scan
- GapAnalyzer: gap score threshold
- PromotionEngine: min orders, margin rate, OOS events
- CS auto-send: inquiry.auto_send_classes
- Purchase auto-pay: auto_pay_limit_krw, auto_pay_daily_cap_krw, hitl.auto.purchase_pay
- Weekly narrative: a3.narrative config

Revision: 0003
"""

from alembic import op

revision = "0003"
down_revision = "0002"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # ── TrendScout (I1) ──────────────────────────────────────────────────────
    _seed("trend.score_threshold", '{"value": 0.3}')
    _seed("trend.max_candidates_per_scan", '{"value": 20}')

    # ── GapAnalyzer (I2) ─────────────────────────────────────────────────────
    _seed("gap.score_threshold", '{"value": 0.5}')

    # ── PromotionEngine (A2) ─────────────────────────────────────────────────
    _seed("promo.min_orders_30d", '{"value": 5}')
    _seed("promo.min_margin_rate", '{"value": 0.12}')
    _seed("promo.max_oos_events", '{"value": 1}')

    # ── CS auto-send (M3 HITL graduation) ────────────────────────────────────
    _seed("inquiry.auto_send", '{"enabled": true}')
    _seed("inquiry.auto_send_classes", '{"classes": ["tracking", "pccc"]}')

    # ── Purchase auto-pay (M3 HITL graduation) ───────────────────────────────
    _seed("hitl.auto.purchase_pay", '{"enabled": true}')
    _seed("auto_pay_limit_krw", '{"value": 50000}')
    _seed("auto_pay_daily_cap_krw", '{"value": 500000}')

    # ── Weekly narrative ─────────────────────────────────────────────────────
    _seed("a3.narrative_enabled", '{"enabled": true}')


def _seed(key: str, value: str) -> None:
    op.execute(
        f"INSERT INTO app_config (key, value) VALUES ('{key}', '{value}'::jsonb) "
        "ON CONFLICT DO NOTHING"
    )


def downgrade() -> None:
    keys = [
        "trend.score_threshold",
        "trend.max_candidates_per_scan",
        "gap.score_threshold",
        "promo.min_orders_30d",
        "promo.min_margin_rate",
        "promo.max_oos_events",
        "inquiry.auto_send",
        "inquiry.auto_send_classes",
        "hitl.auto.purchase_pay",
        "auto_pay_limit_krw",
        "auto_pay_daily_cap_krw",
        "a3.narrative_enabled",
    ]
    for key in keys:
        op.execute(f"DELETE FROM app_config WHERE key = '{key}'")
