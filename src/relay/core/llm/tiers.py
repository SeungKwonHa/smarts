"""LLM task tier routing table.

Agents request a TaskName; this module resolves it to a Tier + per-task params.
Model IDs are injected from settings at runtime — no model names in agent code.

Tiers (doc 05):
  T0 — classify/extract/normalize (cheap, high-volume, temp 0–0.2)
  T1 — generate quality Korean (mid, temp 0.3–0.7)
  T2 — multi-step reasoning (expensive, low-volume, thinking model)

NOTE: LongCat-2.0 uses internal reasoning (thinking tokens). All max_tokens
values MUST include headroom for reasoning overhead. If max_tokens is too
small, the model consumes all tokens for reasoning and returns None content.
Rule of thumb: T0 needs min 256, T1 needs min 512, T2 needs min 1024.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Any


class Tier(str, Enum):
    T0 = "T0"
    T1 = "T1"
    T2 = "T2"


@dataclass(frozen=True)
class TaskParams:
    tier: Tier
    temperature: float
    max_tokens: int
    json_mode: bool = True      # always True — structured output only
    vision: bool = False        # requires T1+ and model support
    batch_ok: bool = False      # can this task be batched (array input)?
    cache_ttl_s: int = 3600     # cache TTL for content-addressed responses


# ── Task routing table (add a row here when adding a new LLM task) ─────────
# NOTE: max_tokens values are raised for LongCat-2.0 reasoning overhead
TASK_PARAMS: dict[str, TaskParams] = {
    # Intelligence
    "i1.entity_extract":        TaskParams(Tier.T0, 0.0, 1024, batch_ok=True,  cache_ttl_s=86400),
    "i2.kr_keywords":           TaskParams(Tier.T0, 0.1, 512,  batch_ok=True,  cache_ttl_s=86400),
    "i3.ip_text_screen":        TaskParams(Tier.T1, 0.0, 2048, vision=True,    cache_ttl_s=3600),
    "i3.ip_image_check":        TaskParams(Tier.T1, 0.0, 512,  vision=True,    cache_ttl_s=3600),
    "i4.brand_dossier":         TaskParams(Tier.T2, 0.5, 4096, json_mode=False, cache_ttl_s=0),
    "i4.outreach_draft":        TaskParams(Tier.T2, 0.6, 2048, json_mode=False, cache_ttl_s=0),
    # Listing
    "l1.variant_normalize":     TaskParams(Tier.T0, 0.0, 1024, batch_ok=True,  cache_ttl_s=86400),
    "l1.category_map":          TaskParams(Tier.T0, 0.0, 256,  batch_ok=True,  cache_ttl_s=86400),
    "l3.title_gen":             TaskParams(Tier.T1, 0.4, 2048,                 cache_ttl_s=0),
    "l3.detail_gen":            TaskParams(Tier.T1, 0.5, 4096,                 cache_ttl_s=0),
    "l3.image_overlay_check":   TaskParams(Tier.T0, 0.0, 256,  vision=True,    cache_ttl_s=3600),
    # CS
    "c1.inquiry_classify":      TaskParams(Tier.T0, 0.0, 256,  batch_ok=True,  cache_ttl_s=0),
    "c1.inquiry_reply":         TaskParams(Tier.T1, 0.4, 1024,                 cache_ttl_s=0),
    "c2.claim_triage":          TaskParams(Tier.T2, 0.3, 2048,                 cache_ttl_s=0),
    # Analytics
    "a3.weekly_narrative":      TaskParams(Tier.T1, 0.5, 2048, json_mode=False, cache_ttl_s=0),
}


def get_task_params(task_name: str) -> TaskParams:
    """Resolve task name → params. Raises KeyError for unknown tasks."""
    try:
        return TASK_PARAMS[task_name]
    except KeyError:
        known = ", ".join(sorted(TASK_PARAMS))
        raise KeyError(f"Unknown LLM task '{task_name}'. Known: {known}") from None


def get_model_id(tier: Tier, model_t0: str, model_t1: str, model_t2: str) -> str:
    mapping = {Tier.T0: model_t0, Tier.T1: model_t1, Tier.T2: model_t2}
    model = mapping[tier]
    if not model:
        raise RuntimeError(
            f"Model ID for tier {tier} not configured. "
            "Set LLM_MODEL_T0/T1/T2 in .env (see docs/notes/longcat_api_verified.md)."
        )
    return model
