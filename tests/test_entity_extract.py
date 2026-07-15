"""Unit tests for entity extraction (T0 LLM + rule-based cleanup).

Tests cover:
- _apply_rules_to_name: strips SEO noise, coupons, brackets, ranking claims
- _rule_extract: full rule-based extraction (no LLM)
- _validate_entity: LLM output + rule post-processing
- extract_entities: DRY_RUN path (rule-based)
- extract_entities_batch: DRY_RUN path (rule-based)

No DB or LLM needed — all tests use pure functions or DRY_RUN mode.
"""

from __future__ import annotations

import os

import pytest

# Force DRY_RUN so no LLM calls happen
os.environ["RELAY_DRY_RUN"] = "1"

from relay.intelligence.entity_extract import (  # noqa: E402
    _apply_rules_to_name,
    _extract_attributes,
    _guess_category,
    _rule_extract,
    _validate_entity,
    extract_entities,
    extract_entities_batch,
)


# ── _apply_rules_to_name ─────────────────────────────────────────────────────

class TestApplyRulesToName:
    """Test the rule-based cleanup applied to LLM output."""

    def test_removes_ranking_claims(self):
        name = "＼楽天ランキング受賞 ／ タンブラー 水筒"
        result = _apply_rules_to_name(name)
        assert "ランキング" not in result
        assert "受賞" not in result
        assert "タンブラー" in result

    def test_removes_full_bracket_contents(self):
        name = "【先着★12点セットが10780円⇒9280円！】CAROTE フライパン"
        result = _apply_rules_to_name(name)
        assert "【" not in result
        assert "】" not in result
        assert "先着" not in result
        assert "CAROTE" in result
        assert "フライパン" in result

    def test_removes_coupon_text(self):
        name = "【15%OFFクーポン】象印 水筒 ワンタッチ"
        result = _apply_rules_to_name(name)
        assert "クーポン" not in result
        assert "OFF" not in result
        assert "象印" in result
        assert "水筒" in result

    def test_removes_shop_tags(self):
        name = "【タイガー魔法瓶 楽天市場店】水筒 食洗機対応"
        result = _apply_rules_to_name(name)
        assert "楽天市場店" not in result
        assert "水筒" in result

    def test_removes_media_mentions(self):
        name = "辻ちゃんネル紹介記念500円offクーポン TYESO タンブラー"
        result = _apply_rules_to_name(name)
        assert "辻ちゃんネル" not in result
        assert "TYESO" in result

    def test_removes_price_ranges(self):
        name = "12点セットが10780円⇒9280円 鍋セット"
        result = _apply_rules_to_name(name)
        assert "10780" not in result
        assert "9280" not in result
        assert "鍋セット" in result

    def test_removes_capacity_specs(self):
        name = "タンブラー 600ml 750ml 水筒"
        result = _apply_rules_to_name(name)
        assert "600ml" not in result
        assert "750ml" not in result
        assert "タンブラー" in result

    def test_removes_point_multiplier(self):
        name = "【ポイント10倍】水筒 サーモス"
        result = _apply_rules_to_name(name)
        assert "ポイント" not in result
        assert "10倍" not in result
        assert "水筒" in result

    def test_removes_empty_brackets(self):
        name = "【】 水筒 サーモフラスクA"
        result = _apply_rules_to_name(name)
        assert "【】" not in result
        assert "水筒" in result

    def test_handles_clean_title_unchanged(self):
        name = "VAKUEN 真空保存容器 電動"
        result = _apply_rules_to_name(name)
        assert result == "VAKUEN 真空保存容器 電動"

    def test_truncates_to_60_chars(self):
        name = "A" * 80
        result = _apply_rules_to_name(name)
        assert len(result) <= 60


# ── _rule_extract ────────────────────────────────────────────────────────────

class TestRuleExtract:
    """Test full rule-based extraction (DRY_RUN fallback)."""

    def test_extracts_brand_from_known_list(self):
        result = _rule_extract("thermos 水筒 真空断熱 JNL-500")
        assert result["brand"] == "サーモス"
        assert "JNL" not in result["product_name"]

    def test_category_kitchen(self):
        result = _rule_extract("タンブラー 水筒 真空断熱 保温 保冷")
        assert result["category_guess"] == "kitchen"

    def test_category_electronics(self):
        result = _rule_extract("ワイヤレス イヤホン bluetooth 充電")
        assert result["category_guess"] == "electronics"

    def test_attributes_extracted(self):
        result = _rule_extract("ステンレス 真空断熱 保冷 保温 水筒")
        attrs = result["attributes"]
        assert "stainless_steel" in attrs
        assert "vacuum_insulated" in attrs
        assert "cold_retention" in attrs

    def test_strips_all_seo_noise(self):
        result = _rule_extract(
            "＼楽天ランキング受賞 ／ 【辻ちゃんネル紹介記念500円OFFクーポン】"
            "TYESO 新登場ストロー付き タンブラー 水筒 蓋付き 直飲み 600ml 750ml"
        )
        name = result["product_name"]
        assert "楽天ランキング" not in name
        assert "受賞" not in name
        assert "辻ちゃんネル" not in name
        assert "クーポン" not in name
        assert "600ml" not in name
        assert "TYESO" in name
        assert "タンブラー" in name


# ── _validate_entity ─────────────────────────────────────────────────────────

class TestValidateEntity:
    """Test LLM output validation + rule post-processing."""

    def test_cleans_llm_output_with_noise(self):
        """LLM returns noisy product_name — rules should clean it."""
        llm_output = {
            "product_name": "【先着★12点セットが10780円⇒9280円！】CAROTE カローテ フライパン",
            "brand": "CAROTE",
            "category_guess": "kitchen",
            "attributes": ["black"],
        }
        result = _validate_entity(llm_output, "raw title")
        assert "【" not in result["product_name"]
        assert "先着" not in result["product_name"]
        assert "CAROTE" in result["product_name"]
        assert "フライパン" in result["product_name"]
        assert result["brand"] == "CAROTE"

    def test_falls_back_to_rules_on_empty_name(self):
        llm_output = {
            "product_name": "",
            "brand": "",
            "category_guess": "other",
            "attributes": [],
        }
        result = _validate_entity(llm_output, "thermos 水筒 真空断熱")
        # Should fall back to rule extraction
        assert len(result["product_name"]) > 0
        assert result["brand"] == "サーモス"

    def test_handles_string_content(self):
        """If LLM returns a string instead of dict, fall back to rules."""
        result = _validate_entity("not a dict", "thermos 水筒")
        assert result["brand"] == "サーモス"

    def test_preserves_good_llm_output(self):
        """Clean LLM output should pass through mostly unchanged."""
        llm_output = {
            "product_name": "VAKUEN 真空保存容器 電動",
            "brand": "VAKUEN",
            "category_guess": "kitchen",
            "attributes": ["bpa_free"],
        }
        result = _validate_entity(llm_output, "raw")
        assert result["product_name"] == "VAKUEN 真空保存容器 電動"
        assert result["brand"] == "VAKUEN"


# ── extract_entities (DRY_RUN) ───────────────────────────────────────────────

class TestExtractEntitiesDryRun:
    """Test single-title extraction in DRY_RUN mode."""

    async def test_basic_extraction(self):
        result = await extract_entities("thermos 水筒 真空断熱 JNL-500", None)
        assert result["brand"] == "サーモス"
        assert result["category_guess"] == "kitchen"
        assert "JNL" not in result["product_name"]

    async def test_noisy_title_cleanup(self):
        result = await extract_entities(
            "＼楽天ランキング受賞 ／ 【15%OFFクーポン】象印 水筒 ワンタッチ 600ml",
            None,
        )
        name = result["product_name"]
        assert "楽天ランキング" not in name
        assert "クーポン" not in name
        assert "600ml" not in name
        assert "象印" in name


class TestExtractEntitiesBatchDryRun:
    """Test batch extraction in DRY_RUN mode."""

    async def test_batch_returns_same_count(self):
        titles = [
            "thermos 水筒 真空断熱",
            "【15%OFFクーポン】象印 水筒",
            "VAKUEN 真空保存容器 電動",
        ]
        results = await extract_entities_batch(titles, None)
        assert len(results) == 3

    async def test_batch_cleans_all(self):
        titles = [
            "＼楽天ランキング受賞 ／ TYESO タンブラー",
            "【先着★12点セットが10780円⇒9280円！】CAROTE フライパン",
        ]
        results = await extract_entities_batch(titles, None)
        for r in results:
            assert "【" not in r["product_name"]
            assert "＼" not in r["product_name"]
            assert "ランキング" not in r["product_name"]

    async def test_empty_list(self):
        results = await extract_entities_batch([], None)
        assert results == []


# ── _guess_category ──────────────────────────────────────────────────────────

class TestGuessCategory:
    def test_kitchen(self):
        assert _guess_category("タンブラー 水筒 保存容器") == "kitchen"

    def test_electronics(self):
        assert _guess_category("ワイヤレス イヤホン usb") == "electronics"

    def test_pet(self):
        assert _guess_category("ペット 犬 おもちゃ") == "pet"

    def test_other(self):
        assert _guess_category("謎の商品 何か") == "other"


# ── _extract_attributes ──────────────────────────────────────────────────────

class TestExtractAttributes:
    def test_color(self):
        attrs = _extract_attributes("ステンレス ブラック 水筒")
        assert "black" in attrs

    def test_material(self):
        attrs = _extract_attributes("ステンレス 真空断熱 水筒")
        assert "stainless_steel" in attrs
        assert "vacuum_insulated" in attrs

    def test_features(self):
        attrs = _extract_attributes("保冷 保温 食洗機 直飲み")
        assert "cold_retention" in attrs
        assert "dishwasher_safe" in attrs
        assert "direct_drink" in attrs

    def test_max_five(self):
        attrs = _extract_attributes("ブラック ステンレス 真空断熱 保冷 保温 食洗機 直飲み 軽量")
        assert len(attrs) <= 5
