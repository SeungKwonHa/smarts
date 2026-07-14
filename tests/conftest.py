"""Pytest fixtures for RELAY.

Session-scoped event loop + engine (required for asyncpg connection pools to
work correctly across fixtures). Tests use unique idempotency keys so they
don't need DB rollback for isolation.
"""

import os
import pytest_asyncio

os.environ.setdefault("RELAY_DRY_RUN", "1")
os.environ.setdefault("RELAY_ENV", "test")

TEST_DB_URL = "postgresql+asyncpg://relay:relay_dev_password@localhost:5433/relay"
os.environ["DATABASE_URL"] = TEST_DB_URL
os.environ["REDIS_URL"] = os.environ.get("REDIS_URL", "redis://localhost:6379/1")


@pytest_asyncio.fixture(scope="session")
async def db_engine():
    from sqlalchemy import text
    from sqlalchemy.ext.asyncio import create_async_engine

    engine = create_async_engine(TEST_DB_URL, echo=False)
    async with engine.begin() as conn:
        try:
            await conn.execute(text("SELECT 1 FROM event_outbox LIMIT 1"))
        except Exception as e:
            await engine.dispose()
            import pytest
            pytest.skip(f"DB not migrated. Run: alembic upgrade head — {e}")
        # Truncate all business tables for a clean test slate.
        # CASCADE handles FK dependencies.
        await conn.execute(text("""
            TRUNCATE TABLE
              processed_events, event_outbox, approval_requests,
              order_events, purchases, shipments, orders,
              price_history, listings, product_sources, risk_flags,
              products, trend_candidates, brand_leads,
              preorder_campaigns, inquiries, claims,
              fx_rates, llm_calls, app_config
            RESTART IDENTITY CASCADE
        """))
        # Re-seed essential config data
        await conn.execute(text("""
            INSERT INTO app_config (key, value, updated_by) VALUES
            ('publish.mode', '{"mode": "api"}', 'system'),
            ('forwarder.warehouse_address.jp', '{"address": "〒123-4567 東京都テスト区1-2-3"}', 'system')
            ON CONFLICT (key) DO NOTHING
        """))
    yield engine
    await engine.dispose()


@pytest_asyncio.fixture
async def db_session(db_engine):
    """Per-test session — no rollback; tests use unique ikeys for isolation."""
    from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
    factory = async_sessionmaker(db_engine, class_=AsyncSession, expire_on_commit=False)
    async with factory() as session:
        yield session


@pytest_asyncio.fixture(scope="session")
async def redis_client():
    import redis.asyncio as aioredis
    r = aioredis.from_url(os.environ["REDIS_URL"], decode_responses=True)
    await r.flushdb()
    yield r
    await r.flushdb()
    await r.aclose()
