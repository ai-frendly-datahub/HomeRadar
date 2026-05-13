from __future__ import annotations

from homeradar.cross_reference import (
    PropertyRecord,
    cross_reference_sources,
    group_by_property,
    normalize_complex_name,
    normalize_region,
    property_key,
)


def test_normalize_complex_name_strips_phase_and_suffix() -> None:
    assert normalize_complex_name("래미안 1차 아파트") == "래미안"
    assert normalize_complex_name("힐스테이트 2단지") == "힐스테이트"
    assert normalize_complex_name("푸르지오") == "푸르지오"
    assert normalize_complex_name("") == ""
    assert normalize_complex_name(None) == ""


def test_normalize_complex_name_handles_special_chars() -> None:
    assert normalize_complex_name("e-편한세상 (1차)") == "e 편한세상"


def test_normalize_region_aliases() -> None:
    assert normalize_region("서울") == "서울특별시"
    assert normalize_region("서울시") == "서울특별시"
    assert normalize_region("경기") == "경기도"
    assert normalize_region("강원") == "강원특별자치도"
    assert normalize_region("") == ""


def test_property_key_format() -> None:
    rec = PropertyRecord(
        si_do="서울",
        si_gun_gu="강남구",
        dong="역삼동",
        complex_name="래미안 1차",
        source="molit_apt_trade",
    )
    assert property_key(rec) == "서울특별시|강남구|역삼동|래미안"


def test_group_by_property_joins_same_complex_across_sources() -> None:
    records = [
        PropertyRecord("서울", "강남구", "역삼동", "래미안 1차", "molit_apt_trade"),
        PropertyRecord("서울특별시", "강남구", "역삼동", "래미안 2차", "reb_subscription"),
        PropertyRecord("서울", "강남구", "삼성동", "푸르지오", "onbid_auction"),
    ]
    grouped = group_by_property(records)
    assert len(grouped) == 2
    yeoksam = grouped["서울특별시|강남구|역삼동|래미안"]
    assert len(yeoksam) == 2
    sources = {r.source for r in yeoksam}
    assert sources == {"molit_apt_trade", "reb_subscription"}


def test_cross_reference_sources_layered_dict() -> None:
    records = [
        PropertyRecord("서울", "강남구", "역삼동", "래미안 1차", "molit_apt_trade"),
        PropertyRecord("서울특별시", "강남구", "역삼동", "래미안 2차", "molit_apt_trade"),
        PropertyRecord("서울특별시", "강남구", "역삼동", "래미안 3차", "reb_subscription"),
    ]
    layered = cross_reference_sources(records)
    key = "서울특별시|강남구|역삼동|래미안"
    assert layered[key].keys() == {"molit_apt_trade", "reb_subscription"}
    assert len(layered[key]["molit_apt_trade"]) == 2


def test_records_with_missing_fields_dropped() -> None:
    bad = [
        PropertyRecord("", "", "", "", "molit_apt_trade"),
        PropertyRecord("서울", "강남구", "", "래미안", "reb_subscription"),
    ]
    assert group_by_property(bad) == {}
