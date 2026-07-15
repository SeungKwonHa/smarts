"""TrendScout scoring v1 — add ranking_metadata + scoring columns.

Adds:
- trend_candidates.ranking_metadata (JSONB): raw rank, reviewCount, reviewAverage, price
- trend_candidates.rank_position (INT): latest rank in source genre
- trend_candidates.review_count (INT): latest review count
- trend_candidates.scan_count (INT): how many scans this candidate appeared in

Revision: 0004
"""

from alembic import op

revision = "0004"
down_revision = "0003"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("""
        ALTER TABLE trend_candidates
        ADD COLUMN IF NOT EXISTS ranking_metadata JSONB DEFAULT '{}'::jsonb
    """)
    op.execute("""
        ALTER TABLE trend_candidates
        ADD COLUMN IF NOT EXISTS rank_position INT
    """)
    op.execute("""
        ALTER TABLE trend_candidates
        ADD COLUMN IF NOT EXISTS review_count INT
    """)
    op.execute("""
        ALTER TABLE trend_candidates
        ADD COLUMN IF NOT EXISTS scan_count INT DEFAULT 1
    """)
    op.execute("CREATE INDEX ON trend_candidates (rank_position) WHERE rank_position IS NOT NULL")
    op.execute("CREATE INDEX ON trend_candidates (review_count DESC) WHERE review_count IS NOT NULL")

    # ── Scoring function ──────────────────────────────────────────────────────
    # Mirrors the Python _compute_accel_score logic in pure SQL for bulk UPDATE.
    # Components:
    #   position_score = exp(-0.30 * (rank - 1))        [weight 0.45]
    #   velocity_score = ln(1+reviews) / ln(1+5000)      [weight 0.30]
    #   quality_score  = review_avg / 5.0                [weight 0.25]
    #   cross_genre_bonus = min(0.15, 0.05 * (genre_count - 1))
    #   accel_bonus    = min(0.10, 0.01 * rank_improvement)
    # Total clamped to [0, 1].
    op.execute("""
        CREATE OR REPLACE FUNCTION _compute_accel_score(
            p_rank_position INT,
            p_review_count INT,
            p_review_average FLOAT,
            p_cross_genre_count INT,
            p_previous_rank INT
        )
        RETURNS FLOAT
        LANGUAGE plpgsql
        IMMUTABLE
        AS $$
        DECLARE
            v_position FLOAT;
            v_velocity FLOAT;
            v_quality  FLOAT;
            v_base     FLOAT;
            v_bonus    FLOAT;
        BEGIN
            -- Position score: exponential decay by rank
            IF p_rank_position IS NOT NULL AND p_rank_position > 0 THEN
                v_position := EXP(-0.30 * (p_rank_position - 1));
            ELSE
                v_position := 0.0;
            END IF;

            -- Velocity score: log-normalized review count
            IF p_review_count IS NOT NULL AND p_review_count > 0 THEN
                v_velocity := LEAST(1.0, LN(1 + p_review_count) / LN(1 + 5000));
            ELSE
                v_velocity := 0.0;
            END IF;

            -- Quality score: review average / 5.0
            IF p_review_average IS NOT NULL AND p_review_average > 0 THEN
                v_quality := LEAST(1.0, p_review_average / 5.0);
            ELSE
                v_quality := 0.0;
            END IF;

            v_base := 0.45 * v_position + 0.30 * v_velocity + 0.25 * v_quality;

            -- Cross-genre bonus
            IF p_cross_genre_count IS NOT NULL AND p_cross_genre_count > 1 THEN
                v_base := v_base + LEAST(0.15, 0.05 * (p_cross_genre_count - 1));
            END IF;

            -- Acceleration bonus: rank improved
            IF p_previous_rank IS NOT NULL AND p_rank_position IS NOT NULL
               AND p_previous_rank > p_rank_position THEN
                v_bonus := LEAST(0.10, 0.01 * (p_previous_rank - p_rank_position));
                v_base := v_base + v_bonus;
            END IF;

            RETURN LEAST(1.0, ROUND(v_base::numeric, 4))::float;
        END;
        $$;
    """)


def downgrade() -> None:
    op.execute("DROP FUNCTION IF EXISTS _compute_accel_score(INT, INT, FLOAT, INT, INT)")
    op.execute("DROP INDEX IF EXISTS trend_candidates_rank_position_idx")
    op.execute("DROP INDEX IF EXISTS trend_candidates_review_count_idx")
    op.execute("ALTER TABLE trend_candidates DROP COLUMN IF EXISTS ranking_metadata")
    op.execute("ALTER TABLE trend_candidates DROP COLUMN IF EXISTS rank_position")
    op.execute("ALTER TABLE trend_candidates DROP COLUMN IF EXISTS review_count")
    op.execute("ALTER TABLE trend_candidates DROP COLUMN IF EXISTS scan_count")
