"""M2 Core — add app_config seed keys + schema for C1/C2 agents.

Adds runtime-tunable thresholds for:
- SKU lifecycle policies (retire dead SKUs, promote risers)
- PCC collection flow
- Inquiry handling config

Schema additions:
- claims.draft_message (TEXT) — ClaimTriage C2 draft

Revision: 0002
"""

from alembic import op

revision = "0002"
down_revision = "0001"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # ── SKU lifecycle thresholds ─────────────────────────────────────────────
    _seed("sku.retire.no_sale_days", '{"value": 30}')
    _seed("sku.retire.oos_event_threshold", '{"value": 3}')
    _seed("sku.tier.riser_orders_7d", '{"value": 1}')

    # ── PCCC collection ──────────────────────────────────────────────────────
    _seed("pccc.hold_enabled", '{"enabled": true}')
    _seed("pccc.request_message",
          '{"message": "개인통관고유부호를 보내주세요. 택배 발송에 필요합니다. '
          '관세청 유니패스 앱(unipass.customs.go.kr)에서 발급 가능합니다."}')

    # ── Inquiry handling ──────────────────────────────────────────────────────
    _seed("inquiry.auto_draft_classes",
          '{"classes": ["tracking", "spec", "pccc"]}')
    _seed("inquiry.confidence_threshold", '{"value": 0.7}')
    _seed("inquiry.auto_send", '{"enabled": false}')  # M3: enable for tracking/pccc

    # ── Dispatch idempotency ─────────────────────────────────────────────────
    _seed("dispatch.idempotent", '{"enabled": true}')

    # ── ClaimTriage (C2) ────────────────────────────────────────────────────
    _seed("claim.auto_refund_pre_ship", '{"enabled": false}')  # M3: auto-cancel pre-shipment
    op.execute("ALTER TABLE claims ADD COLUMN IF NOT EXISTS draft_message TEXT")


def _seed(key: str, value: str) -> None:
    op.execute(
        f"INSERT INTO app_config (key, value) VALUES ('{key}', '{value}'::jsonb) "
        "ON CONFLICT DO NOTHING"
    )


def downgrade() -> None:
    keys = [
        "sku.retire.no_sale_days",
        "sku.retire.oos_event_threshold",
        "sku.tier.riser_orders_7d",
        "pccc.hold_enabled",
        "pccc.request_message",
        "inquiry.auto_draft_classes",
        "inquiry.confidence_threshold",
        "inquiry.auto_send",
        "dispatch.idempotent",
        "claim.auto_refund_pre_ship",
    ]
    for key in keys:
        op.execute(f"DELETE FROM app_config WHERE key = '{key}'")
