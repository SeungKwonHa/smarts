# -*- coding: utf-8 -*-
"""T0 entity extraction for noisy product titles.

Cleans raw product names from source marketplaces (Rakuten, Amazon) into
structured fields:
  - product_name: core product name stripped of SEO/size/model noise
  - brand: extracted brand (if identifiable)
  - category_guess: coarse category bucket
  - attributes: list of key attributes (color, size, material, etc.)

Uses LLM task "i1.entity_extract" (T0, json_mode, batch_ok, cache 24h).
In DRY_RUN or when LLM unavailable, falls back to rule-based extraction.

Called by: TrendScout (I1), SourceMatcher (L1).
"""

from __future__ import annotations

import re
from typing import Any

import structlog
from sqlalchemy.ext.asyncio import AsyncSession

from relay.core.config import settings
from relay.core.llm.client import client as llm
from relay.core.llm.tiers import get_task_params

log = structlog.get_logger(__name__)

# ── System prompt ─────────────────────────────────────────────────────────────
# NOTE: _SCHEMA_DESCRIPTION is appended via concatenation (not f-string
# interpolation) to avoid triple-quote ambiguity inside the f-string.
_SYSTEM_PROMPT = (
    "You are a product data normalization engine for Japanese "
    "marketplace titles. Your job is to strip ALL marketing noise and return ONLY "
    "the core product identity.\n\n"
    "## REMOVE completely (do not include in output):\n"
    "- Ranking claims: 楽天ランキング1位, ＼楽天1位／, rank 1, best seller, ランキング受賞\n"
    "- Coupon/discount: 〜OFFクーポン, クーポンで〜円, %off, ポイントN倍, 限定価格\n"
    "- Shop tags: 〜公式店, 楽天市場店, 正規商品, 公式\n"
    "- Promotional brackets and their contents: 【〜】, ★〜★, ＼〜／, ■〜■\n"
    "- Capacity/specs: 500ml, 2L, 12個セット, 3pack, mmz-xxxx\n"
    "- Conditional phrases: 〜付, 〜付き, 〜対応, 保証付き, 送料無料\n"
    "- Media mentions: 辻ちゃんネル紹介, TV出演, YouTuber推薦\n\n"
    "## KEEP in product_name:\n"
    "- Core product noun phrase in original language (Japanese or English)\n"
    "- Brand name (as a word, not with 公式 tags)\n"
    "- 2-3 key feature words only if they define the product (真空断熱, 电动, etc.)\n\n"
    "## Rules:\n"
    "- product_name max 40 characters. Be AGGRESSIVE about cutting.\n"
    "- If the title is half promotional text, return the short core noun.\n"
    "- Never include 【】, ★, ＼, ／, or promotional symbols in the output.\n"
    "- Never include price, coupon, or discount text.\n"
    "- Return language: keep Japanese titles in Japanese, English in English.\n\n"
    "Return a JSON object with these fields:\n"
    "- product_name: core product name in the ORIGINAL language (Japanese/English), "
    "stripped of SEO noise, coupon text, sizing, model numbers, shop names. Keep "
    "only what a human would call this product. Max 60 chars.\n"
    "- brand: brand name if identifiable (or empty string).\n"
    "- category_guess: one of: kitchen, stationery, hobby, outdoor, electronics, "
    "beauty, fashion, home, pet, baby, automotive, other.\n"
    "- attributes: list of 1-5 key attributes (color, material, size, pattern, use-case) "
    "extracted from the title. Short phrases, not full sentences.\n\n"
    "## Examples:\n"
    "Input: \"＼楽天ランキング受賞 ／ 辻ちゃんネル紹介記念500円offクーポン tyeso 新登場ストロー付き タンブラー 水筒 蓋付き 直飲み 600ml 750ml\"\n"
    '→ product_name: "TYESO ストロー付き タンブラー 水筒", brand: "TYESO", category: "kitchen"\n\n'
    "Input: \"先着限定★セットが10780円⇒9280円！ carote カローテ フライパン セット12・9・8・ ih対応 pfoa pfos フリー 鍋セット\"\n"
    '→ product_name: "CAROTE カローテ フライパン セット", brand: "CAROTE", category: "kitchen"\n\n'
    "Input: \"公式 vakuen 真空保存容器 電動 強力密閉 鮮度長持ち コンテナ タッパー キャニスター bpaフリー 電子レンジ\"\n"
    '→ product_name: "VAKUEN 真空保存容器 電動", brand: "VAKUEN", category: "kitchen"\n\n'
    "Input: \"【15%OFFクーポン】象印 水筒 ワンタッチ 直飲み シームレス パッキンなし スポーツボトル 保冷 ステンレスボトル 食洗機対応 大容量 sd-kaシリーズ\"\n"
    '→ product_name: "象印 ワンタッチ スポーツボトル", brand: "象印", category: "kitchen"'
)

# ── Rule-based fallback patterns ─────────────────────────────────────────────
# These remove the most common noise from Japanese marketplace titles.
# Applied BOTH in pure rule-mode AND as post-processing on LLM output.

# SEO / promotional phrases to strip (order matters: greedy first)
_SEO_PATTERNS = [
    # Full bracket contents (most aggressive - remove entirely)
    r'【[^】]*】',       # Japanese promotional brackets
    r'「[^」]*」',       # Japanese quotation brackets
    r'\[[^\]]*\]',       # English square brackets
    r'（[^）]*）',       # Japanese parentheses with content
    r'\([^)]*\)',        # English parentheses with content
    # Full slash-box contents (＼...／)
    r'＼[^／]*／',
    # Ranking claims
    r'楽天ランキング\s*[0-9００]*位?',
    r'楽天ランキング受賞',
    r'楽天[0-9０]*位',
    r'ランキング[0-9０]*位?',
    r'ランキング受賞',
    # Coupon / discount
    r'[0-9０]+％?\s*OFF\s*クーポン[^】]*',
    r'クーポンで[0-9０，,]+円',
    r'[0-9０]+％?off',
    r'[0-9０]+%OFF',
    r'ポイント[0-9０]*倍',
    r'MAX[0-9,，]+円',
    r'[0-9０][0-9０,，]*円\s*[⇒~〜]\s*[0-9０][0-9０,，]*円',  # 10780円⇒9280円
    # Shop tags
    r'公式店?',
    r'楽天市場店',
    r'正規商品',
    r'正規品',
    # Promotional words
    r'送料無料',
    r'送料込み',
    r'限定',
    r'セール',
    r'特価',
    r'ギフト',
    r'保証付(?:き)?',
    r'先着[0-9０]*点セット',
    r'先着限定',
    # Star/asterisk decorations
    r'★[^*]*★',
    r'\*[＊\*][^*]*\*[＊\*]',
    # Decorative diamonds/triangles
    r'◆+',
    r'▲+',
    r'■+',
    # Media mentions
    r'辻ちゃんネル紹介',
    r'辻ちゃんネル紹介記念[0-9０，,]+円?\s*(off|OFF)?\s*クーポン?',
    # Misc
    r'新登場',
    r'楽天1位',
    r'rank\s*1',
    r'best\s*seller',
]

# Common Japanese units/specs to strip (keep the product, lose the spec)
_SPEC_PATTERNS = [
    r'\b[0-9０]+ml\b',
    r'\b[0-9０]+l\b',
    r'\b[0-9０]+リットル\b',
    r'\b[0-9０]+cm\b',
    r'\b[0-9０]+mm\b',
    r'\*[0-9０]+\*[個点本枚セット]\b',
    r'\b[0-9０]+[個点本枚セット]\b',
    r' ~[0-9０]+',
    r'[0-9０]+[〜~][0-9０]+ml',
    r'[0-9０]+[〜~][0-9０]+l\b',
    r'mmz-[a-z0-9]+',
    r'fjq-[0-9]+',
    r'ew-?[a-z]+',
    r'sd-[a-z]+',
    r'adj[b]?-[0-9a-z]+',
    r'aw[a-z]{2}-[0-9a-z]+',
    r'bt[0-9]+',
    r'abib-[a-z0-9]+',
    # Generic model patterns (alphanumeric after dash)
    r'\b[A-Z]{2,}-[A-Z0-9]+\b',  # JNL-500, SD-KA500, etc.
    r'\b[A-Z][a-z]+-[0-9]+\b',    # Jnl-500 style
    # Size notations like 60×80, 60x80, 60*80, 20×
    r'\b[0-9０]+\s*[×x*]\s*[0-9０]*\b',
    # Capacity with decimal (0.89L, 0.59L)
    r'\b[0-9０]+\.[0-9０]+\s*[LlＬ]\b',
    # Standalone large model numbers (2.0 at end, 0. 0.)
    r'\b\d+\.\d+\s+\d+\.\d+\b',
]

# Brand detection (common Japanese-foreign brands on Rakuten)
_KNOWN_BRANDS = {
    'thermos': 'サーモス', 'tiger': 'タイガー魔法瓶', 'zojirushi': '象印',
    'vakuen': 'バクエン', 'carote': 'カローテ', 'lunchichi': 'lunchichi',
    'georg jensen': 'ジョージ ジェンセン', 'azuma': 'アズマ工業',
    'bbox': 'b.box', 'tyeso': 'tyeso', 'shidax': 'シダガー',
    'takeshi': 'タケヤ', 'iris': 'アイリスオーヤマ',
    'wens': 'ウェンズプロダクツ',
}


async def extract_entities(
    raw_title: str,
    session: AsyncSession | None = None,
) -> dict[str, Any]:
    """Extract structured entity from a noisy product title.

    In DRY_RUN: uses rule-based extraction (no LLM).
    In live mode: uses LLM T0 with caching, falls back to rule-based on error.
    """
    if settings.relay_dry_run:
        return _rule_extract(raw_title)

    try:
        return await _llm_extract(raw_title, session)
    except Exception as e:
        log.warning("entity_extract_llm_failed", error=str(e)[:200], title=raw_title[:80])
        return _rule_extract(raw_title)


async def extract_entities_batch(
    titles: list[str],
    session: AsyncSession | None = None,
) -> list[dict[str, Any]]:
    """Batch version - sends multiple titles in one LLM call.

    Titles are processed as a numbered list in a single prompt.
    Returns results in the same order as input.
    """
    if not titles:
        return []

    if settings.relay_dry_run:
        return [_rule_extract(t) for t in titles]

    try:
        return await _llm_extract_batch(titles, session)
    except Exception as e:
        log.warning("entity_extract_batch_failed", error=str(e)[:200], count=len(titles))
        return [_rule_extract(t) for t in titles]


# ── LLM path ─────────────────────────────────────────────────────────────────

async def _llm_extract(
    raw_title: str,
    session: AsyncSession | None = None,
) -> dict[str, Any]:
    """Single-title LLM extraction."""
    messages = [
        {"role": "system", "content": _SYSTEM_PROMPT},
        {"role": "user", "content": raw_title[:300]},
    ]
    resp = await llm.complete(
        task_name="i1.entity_extract",
        messages=messages,
        session=session,
        template_version="v1",
        agent="trend_scout",
    )

    if isinstance(resp.content, dict) and resp.content.get("_dry_run"):
        return _rule_extract(raw_title)

    return _validate_entity(resp.content, raw_title)


async def _llm_extract_batch(
    titles: list[str],
    session: AsyncSession | None = None,
) -> list[dict[str, Any]]:
    """Batch LLM extraction - multiple titles in one call."""
    numbered = "\n".join(f"{i+1}. {t[:200]}" for i, t in enumerate(titles))
    messages = [
        {"role": "system", "content": _SYSTEM_PROMPT},
        {"role": "user", "content": f"Extract entities for each of the following product titles. Return a JSON array with {len(titles)} objects:\n\n{numbered}"},
    ]
    resp = await llm.complete(
        task_name="i1.entity_extract",
        messages=messages,
        session=session,
        template_version="v1",
        agent="trend_scout",
    )

    if isinstance(resp.content, dict) and resp.content.get("_dry_run"):
        return [_rule_extract(t) for t in titles]

    # Response should be an array of entities
    if isinstance(resp.content, list):
        return [_validate_entity(e, titles[i] if i < len(titles) else "") for i, e in enumerate(resp.content)]
    if isinstance(resp.content, dict) and "results" in resp.content:
        return [_validate_entity(e, titles[i] if i < len(titles) else "") for i, e in enumerate(resp.content["results"])]

    # Fallback: return rule-based for all
    return [_rule_extract(t) for t in titles]


def _apply_rules_to_name(product_name: str) -> str:
    """Apply SEO/spec cleanup rules to an already-extracted product name.

    This is a lightweight post-process step: it strips residual promotional
    noise that LLM may have left in product_name, without doing full
    entity extraction (brand/category/attributes come from LLM).
    """
    name = product_name.strip()
    for pattern in _SEO_PATTERNS:
        name = re.sub(pattern, '', name, flags=re.IGNORECASE)
    for pattern in _SPEC_PATTERNS:
        name = re.sub(pattern, '', name, flags=re.IGNORECASE)
    name = re.sub(r'\s+', ' ', name).strip()
    name = name.strip(' /｜-〜~')
    # Remove orphaned empty brackets that may remain
    name = name.replace('【】', '').replace('（）', '').replace('()', '')
    # Remove orphaned opening brackets with everything after them
    name = re.sub(r'\s*[\[\[「（][^\]\]」）]*$', '', name)
    # Remove orphaned closing brackets
    name = name.strip('[]「」（）()')
    # Remove orphaned decimal fragments (e.g., "0. 0." from stripped "0.89L 0.59L")
    name = re.sub(r'\b\d+\.\s+\d+\.\s*\d*\.?\s*\d*', '', name)
    name = re.sub(r'\s+', ' ', name).strip()
    return name[:60]


def _validate_entity(content: dict[str, Any] | str, raw_title: str) -> dict[str, Any]:
    """Coerce LLM output into the canonical schema, with rule-based fallback.

    IMPORTANT: LLM batch extraction often ignores cleanup instructions. We always
    run _apply_rules_to_name() on the LLM's product_name as a safety net.
    """
    if isinstance(content, str):
        return _rule_extract(raw_title)

    llm_name = str(content.get("product_name", ""))[:60]
    cleaned_name = _apply_rules_to_name(llm_name) if llm_name else ""

    # If LLM returned empty/meaningless name, fall back to full rule extraction
    if not cleaned_name:
        return _rule_extract(raw_title)

    return {
        "product_name": cleaned_name,
        "brand": str(content.get("brand", ""))[:40] or _rule_extract(raw_title)["brand"],
        "category_guess": str(content.get("category_guess", "other"))[:20],
        "attributes": content.get("attributes", []) if isinstance(content.get("attributes"), list) else [],
    }


# ── Rule-based fallback ──────────────────────────────────────────────────────

def _rule_extract(raw_title: str) -> dict[str, Any]:
    """Rule-based entity extraction - no LLM needed.

    Strategy: strip SEO noise, then strip specs, keep the remaining core.
    """
    title = raw_title.strip()

    # 1. Strip SEO/promotional patterns
    for pattern in _SEO_PATTERNS:
        title = re.sub(pattern, '', title, flags=re.IGNORECASE)

    # 2. Strip parentheses contents that are pure specs
    title = re.sub(r'[(（][0-9a-zmlリットル%s]+[)）]' % ''.join(['入', '個', '点']), '', title)

    # 3. Strip model numbers and specs
    for pattern in _SPEC_PATTERNS:
        title = re.sub(pattern, '', title, flags=re.IGNORECASE)

    # 4. Clean up whitespace
    title = re.sub(r'\s+', ' ', title).strip()
    title = title.strip(' /｜-〜~')
    # Remove orphaned empty brackets
    title = title.replace('【】', '').replace('（）', '').replace('()', '')
    title = re.sub(r'\s+', ' ', title).strip()

    # 5. Identify brand
    brand = ""
    title_lower = title.lower()
    for eng, jp in _KNOWN_BRANDS.items():
        if eng in title_lower or jp in title:
            brand = jp
            break

    # 6. Category guess based on keyword matching
    category = _guess_category(title)

    # 7. Extract simple attributes
    attributes = _extract_attributes(raw_title)

    return {
        "product_name": title[:60] if title else raw_title[:60],
        "brand": brand[:40],
        "category_guess": category,
        "attributes": attributes,
    }


def _guess_category(title: str) -> str:
    """Coarse category classification by keyword matching."""
    title_lower = title.lower()
    rules = [
        ("kitchen", ['キッチン', '水筒', '料理', '調理', '食器', 'ボトル', '保存容器', 'タッパー', 'フライパン', '鍋', '水切り']),
        ("stationery", ['文房具', 'デスク', 'ペン', 'ノート', 'オーガナイザー', '事務用品']),
        ("hobby", ['ホビー', 'フィギュア', 'プラモデル', 'コレクション', '玩具']),
        ("outdoor", ['アウトドア', 'キャンプ', 'アウトドア', '登山', 'ジャグ', 'スポーツ']),
        ("electronics", ['電子', '充電', 'usb', 'bluetooth', 'ワイヤレス', 'イヤホン']),
        ("beauty", ['美容', 'スキンケア', 'ヘア', 'メイク']),
        ("fashion", ['服', 'バッグ', 'シューズ', '時計', 'アクセサリー']),
        ("home", ['ホーム', 'インテリア', '収納', '掃除', 'ブラシ', 'タオル']),
        ("pet", ['ペット', '犬', '猫', 'ドッグ', 'キャット']),
        ("baby", ['ベビー', '赤ちゃん', '育児', '乳幼児', 'キッズ', '子供']),
        ("automotive", ['自動車', 'カー', 'バイク', '自転車']),
    ]
    for cat, keywords in rules:
        if any(kw in title_lower for kw in keywords):
            return cat
    return "other"


def _extract_attributes(title: str) -> list[str]:
    """Extract simple attributes from title (color, material, features)."""
    attrs = []
    title_lower = title.lower()

    color_map = {
        'ブラック': 'black', '黒': 'black', 'black': 'black',
        'ホワイト': 'white', '白': 'white', 'white': 'white',
        'レッド': 'red', '赤': 'red',
        'ブルー': 'blue', '青': 'blue',
        'グレー': 'gray', '灰': 'gray',
        'ピンク': 'pink', 'ベージュ': 'beige',
        'ブラウン': 'brown', '茶': 'brown',
    }
    for jp, en in color_map.items():
        if jp in title_lower:
            attrs.append(en)
            break

    material_map = {
        'ステンレス': 'stainless_steel', '真空断熱': 'vacuum_insulated',
        'セラミック': 'ceramic', 'プラスチック': 'plastic',
        'シリコン': 'silicone', 'ガラス': 'glass',
        '木': 'wood', 'ダマスク': 'damask',
    }
    for jp, en in material_map.items():
        if jp in title_lower:
            attrs.append(en)

    feature_map = {
        '保冷': 'cold_retention', '保温': 'heat_retention',
        '食洗機': 'dishwasher_safe', '電子レンジ': 'microwave_safe',
        '直飲み': 'direct_drink', 'ワンタッチ': 'one_touch',
        '折りたたみ': 'foldable', '軽量': 'lightweight',
        '大容量': 'large_capacity', 'BPA': 'bpa_free',
    }
    for jp, en in feature_map.items():
        if jp in title_lower:
            attrs.append(en)

    return attrs[:5]
