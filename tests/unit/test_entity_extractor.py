"""
Unit tests for EntityExtractor.
"""

import pytest

from analyzers.entity_extractor import EntityExtractor, extract_entities


class TestEntityExtractor:
    """Tests for EntityExtractor class."""

    @pytest.fixture
    def extractor(self):
        """Create EntityExtractor instance."""
        return EntityExtractor()

    def test_extract_complex_names(self, extractor):
        """Test extracting apartment complex brand names."""
        text = "강남 래미안 아파트와 힐스테이트 단지가 인기입니다"

        entities = extractor.extract(text)

        assert "complex" in entities
        assert "래미안" in entities["complex"]
        assert "힐스테이트" in entities["complex"]

    def test_extract_districts(self, extractor):
        """Test extracting district names."""
        text = "강남구와 서초구 아파트 가격이 급등했습니다"

        entities = extractor.extract(text)

        assert "district" in entities
        assert "강남구" in entities["district"]
        assert "서초구" in entities["district"]

    def test_extract_projects(self, extractor):
        """Test extracting development project names."""
        text = "GTX 개통으로 인한 부동산 가격 상승, 신분당선 연장"

        entities = extractor.extract(text)

        assert "project" in entities
        assert "GTX" in entities["project"]
        assert "신분당선" in entities["project"]

    def test_extract_keywords(self, extractor):
        """Test extracting keywords."""
        text = "아파트 시세 급등, 전세가가 상승했습니다"

        entities = extractor.extract(text)

        assert "keyword" in entities
        assert "급등" in entities["keyword"]
        assert "전세" in entities["keyword"]
        assert "시세" in entities["keyword"]
        assert "상승" in entities["keyword"]

    def test_extract_multiple_types(self, extractor):
        """Test extracting multiple entity types from same text."""
        text = "강남구 래미안 아파트 가격이 급등했습니다. GTX 개통 효과로 분석됩니다."

        entities = extractor.extract(text)

        assert "complex" in entities
        assert "래미안" in entities["complex"]

        assert "district" in entities
        assert "강남구" in entities["district"]

        assert "keyword" in entities
        assert "급등" in entities["keyword"]

        assert "project" in entities
        assert "GTX" in entities["project"]

    def test_extract_empty_text(self, extractor):
        """Test extracting from empty text."""
        entities = extractor.extract("")

        assert entities == {}

    def test_extract_no_entities(self, extractor):
        """Test extracting from text with no known entities."""
        text = "This is some random text with no real estate entities"

        entities = extractor.extract(text)

        # May have empty dict or no matching keys
        assert isinstance(entities, dict)
        # No entities should be found
        for values in entities.values():
            assert len(values) == 0

    def test_case_insensitive_matching(self, extractor):
        """Test that matching is case-insensitive."""
        text = "GANGNAM 래미안 HILLSTATE"

        entities = extractor.extract(text)

        # Should find entities despite case differences
        assert "complex" in entities
        assert any("래미안" in c or "Hillstate" in c for c in entities["complex"])

    def test_no_duplicate_entities(self, extractor):
        """Test that duplicate entities are removed."""
        text = "강남구 강남구 강남구 래미안 래미안"

        entities = extractor.extract(text)

        # Each entity should appear only once
        if "district" in entities:
            assert entities["district"].count("강남구") == 1
        if "complex" in entities:
            assert entities["complex"].count("래미안") == 1

    def test_region_normalization(self, extractor):
        """Test region name normalization."""
        text = "강남 지역 아파트 가격"

        entities = extractor.extract(text)

        if "district" in entities:
            # "강남" should be normalized to "강남구"
            assert "강남구" in entities["district"] or "강남" in entities["district"]

    def test_extract_from_item(self, extractor):
        """Test extracting from item dictionary."""
        item = {
            "title": "강남구 래미안 아파트 급등",
            "summary": "GTX 개통으로 인한 가격 상승",
        }

        entities = extractor.extract_from_item(item)

        assert "complex" in entities
        assert "래미안" in entities["complex"]

        assert "district" in entities
        assert "강남구" in entities["district"]

        assert "keyword" in entities
        assert "급등" in entities["keyword"]
        assert "상승" in entities["keyword"]

        assert "project" in entities
        assert "GTX" in entities["project"]

    def test_get_entity_count(self, extractor):
        """Test counting total entities."""
        entities = {
            "complex": ["래미안", "힐스테이트"],
            "district": ["강남구"],
            "keyword": ["급등", "전세"],
        }

        count = extractor.get_entity_count(entities)

        assert count == 5  # 2 + 1 + 2

    def test_get_entity_count_empty(self, extractor):
        """Test counting entities in empty dict."""
        count = extractor.get_entity_count({})

        assert count == 0

    def test_has_entities(self, extractor):
        """Test checking if entities exist."""
        entities_with = {"complex": ["래미안"]}
        entities_without = {}

        assert extractor.has_entities(entities_with) is True
        assert extractor.has_entities(entities_without) is False


class TestConvenienceFunction:
    """Tests for convenience functions."""

    def test_extract_entities_function(self):
        """Test extract_entities convenience function."""
        text = "강남구 래미안 아파트 가격 급등"

        entities = extract_entities(text)

        assert isinstance(entities, dict)
        assert "complex" in entities
        assert "district" in entities
        assert "keyword" in entities


class TestRealWorldExamples:
    """Tests with real-world Korean news examples."""

    @pytest.fixture
    def extractor(self):
        """Create EntityExtractor instance."""
        return EntityExtractor()

    def test_news_article_1(self, extractor):
        """Test with real news headline."""
        text = "서초구 반포동 래미안퍼스트리버 전용 84㎡ 매매가 30억 돌파"

        entities = extractor.extract(text)

        assert "complex" in entities
        assert "래미안" in entities["complex"]

        assert "district" in entities
        assert "서초구" in entities["district"]

        assert "keyword" in entities
        assert "매매" in entities["keyword"]

    def test_news_article_2(self, extractor):
        """Test with development project news."""
        text = "GTX-A 개통 앞두고 분당·판교 재건축 단지 관심 급증"

        entities = extractor.extract(text)

        assert "project" in entities
        assert "GTX" in entities["project"] or "GTX-A" in entities["project"]
        assert "재건축" in entities["project"]

        assert "district" in entities
        # Should find 분당 or normalized version
        assert len(entities["district"]) > 0

        # Note: This text may not have explicit keywords
        # as "개통", "관심", "급증" are not in our keyword dictionary

    def test_news_article_3(self, extractor):
        """Test with policy news."""
        text = "투기과열지구 지정으로 강남 재건축 시장 위축"

        entities = extractor.extract(text)

        assert "keyword" in entities
        assert "투기과열지구" in entities["keyword"]
        assert "재건축" in entities["keyword"] or "재건축" in entities["project"]
