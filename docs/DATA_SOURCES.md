# HomeRadar Data Sources

부동산 시장 데이터 수집을 위한 데이터 소스 현황 및 사용 가이드입니다.

**Last Updated**: 2025-11-24

## 📊 데이터 소스 현황 요약

| Category | Count | Status | Notes |
|----------|-------|--------|-------|
| **Government APIs** | 3 | 2 enabled, 1 planned | Requires API key from data.go.kr |
| **Professional News** | 7 | 6 enabled, 1 optional | RSS feeds - no auth required |
| **Market Platforms** | 5 | All disabled | No official APIs available |
| **Total** | **15** | **8 enabled** | - |

## 🏛️ TIER 1: Government & Official Sources

### 국토교통부 실거래가 API (MOLIT Transaction APIs)

공공데이터포털을 통해 제공되는 공식 부동산 거래 데이터입니다.

#### 1. Apartment Sales Transaction (아파트 매매 실거래가)

- **ID**: `molit_apt_transaction`
- **Status**: ✅ Enabled
- **URL**: [공공데이터포털](https://www.data.go.kr/data/15126469/openapi.do)
- **Data Type**: API (XML/JSON)
- **Update Frequency**: Daily
- **Coverage**: 전국 아파트 매매 실거래가

**인증키 발급 방법**:
1. [공공데이터포털](https://www.data.go.kr) 회원가입
2. '국토교통부_아파트 매매 실거래가' 검색
3. 활용신청 클릭 (자동승인, 1-2시간 소요)
4. 마이페이지 > 오픈API > 운영계정에서 인증키 확인

**참고 자료**:
- [사용 가이드](https://github.com/WooilJeong/PublicDataReader/blob/main/assets/docs/portal/TransactionPrice.md)
- [Python 예제](https://datadoctorblog.com/2025/03/17/Py-Crawling-API-gov-APT-trade/)
- [PublicDataReader 라이브러리](https://wooiljeong.github.io/python/public_data_reader_01/)

#### 2. Apartment Rent Transaction (아파트 전월세 실거래가)

- **ID**: `molit_apt_rent`
- **Status**: ✅ Enabled
- **URL**: Same as above
- **Data Type**: API (XML/JSON)
- **Coverage**: 전국 아파트 전세/월세 실거래가

**특징**:
- 매매 API와 동일한 인증키 사용
- 전세(lease) 및 월세(monthly rent) 데이터 모두 포함

#### 3. Subscription Information (청약홈 분양정보)

- **ID**: `reb_subscription`
- **Status**: 🚧 Planned (Phase 2)
- **URL**: [공공데이터포털](https://www.data.go.kr/data/15098547/openapi.do)
- **Data Type**: API (XML/JSON)
- **Coverage**: 전국 아파트/오피스텔 청약 분양 정보

**제공 데이터**:
- 분양 일정 및 조건
- 청약 경쟁률
- 공공지원 민간임대
- 신혼희망타운 정보

## 📰 TIER 2: Professional Media & News

RSS 피드 기반 부동산 뉴스 소스입니다. 인증 불필요, 무료 사용 가능합니다.

### 1. 매일경제 (Maeil Business Newspaper)

- **ID**: `mk_realestate`
- **Status**: ✅ Enabled
- **URL**: `http://file.mk.co.kr/news/rss/rss_50300009.xml`
- **Type**: RSS Feed
- **Coverage**: 부동산 시장 분석, 정책, 시세 동향

**특징**:
- 국내 주요 경제신문
- 매부리레터(부동산 뉴스레터) 운영
- 매부리TV(유튜브 채널) 운영

### 2. 한국경제 (Korea Economic Daily)

- **ID**: `hankyung_realestate`
- **Status**: ✅ Enabled
- **URL**: `http://rss.hankyung.com/estate.xml`
- **Type**: RSS Feed
- **Section**: 집코노미 부동산

**특징**:
- 시장 분석 중심
- 정책 해설
- 투자 트렌드 분석

### 3-6. 헤럴드경제 (Herald Economy)

헤럴드경제는 부동산 섹션을 세분화하여 4개의 RSS 피드를 제공합니다.

| ID | Name | URL | Focus |
|----|------|-----|-------|
| `herald_realestate_all` | 부동산 전체 | `http://biz.heraldm.com/rss/010300000000.xml` | All articles |
| `herald_realestate_policy` | 부동산 정책 | `http://biz.heraldm.com/rss/010303000000.xml` | Government policy |
| `herald_realestate_investment` | 부동산 재테크 | `http://biz.heraldm.com/rss/010305000000.xml` | Investment tips |
| `herald_realestate_market` | 시세/분양정보 | `http://biz.heraldm.com/rss/010306000000.xml` | Prices & launches |

**권장 설정**:
- 전체 뉴스 수집: `herald_realestate_all` 사용
- 주제별 분석: 개별 피드 4개 모두 활성화

### 7. 조선비즈 (Chosun Biz)

- **ID**: `chosunbiz_realestate`
- **Status**: ✅ Enabled
- **URL**: `http://biz.chosun.com/site/data/rss/estate.xml`
- **Type**: RSS Feed

### 8. 뉴스데일리 (News Daily)

- **ID**: `newsdaily_realestate`
- **Status**: ⚪ Optional (현재 비활성화)
- **URL**: `http://www.newsdaily.kr/rss/S1N50.xml`
- **Type**: RSS Feed
- **Notes**: 2차 뉴스 소스로 필요시 활성화

## 🏢 TIER 3: Market Data Platforms

### KB부동산 데이터허브

- **ID**: `kb_kbland`
- **Status**: 🔬 Research Phase
- **URL**: https://data.kbland.kr/
- **Type**: API (PublicDataReader library)

**제공 데이터**:
- KB주택가격지수
- 아파트 평균가격
- 전세가율
- 시장 동향 분석

**접근 방법**:
```python
# PublicDataReader 라이브러리 사용
pip install PublicDataReader

from PublicDataReader import kbland
# 인증키 불필요
```

**참고**: [PublicDataReader - KB부동산 가이드](https://wooiljeong.github.io/python/pdr-kbland/)

### 네이버 부동산 (Naver Land)

- **ID**: `naver_land`
- **Status**: 🚫 No Official API
- **URL**: https://land.naver.com
- **Notes**:
  - 공식 API 미제공
  - 웹 스크래핑 가능하나 ToS 확인 필요
  - 데이터 품질은 매우 높음

### 부동산114 (R114)

- **ID**: `r114`
- **Status**: 🚫 No Official API
- **URL**: https://www.r114.com/
- **Notes**: 국내 주요 부동산 포털, 공식 API 미확인

### 직방 / 다방 (Zigbang / Dabang)

- **IDs**: `zigbang`, `dabang`
- **Status**: 🚫 No Public API
- **URLs**:
  - Zigbang: https://www.zigbang.com
  - Dabang: https://www.dabangapp.com
- **Notes**: 주요 부동산 중개 플랫폼, 공개 API 미제공

## 📈 사용 우선순위

### Phase 1 (현재 - MVP)
1. ✅ **국토부 실거래가 API** (매매 + 전월세)
2. ✅ **주요 경제신문 RSS** (매일경제, 한국경제, 헤럴드경제)

### Phase 2 (확장)
3. 🚧 **청약홈 분양정보 API**
4. 🚧 **KB부동산 데이터허브**
5. 🚧 **추가 뉴스 소스** (연합뉴스, 아시아경제 등)

### Phase 3 (고급)
6. 🔮 **민간 플랫폼 데이터** (네이버/직방/다방 - 파트너십 필요)

## 🔑 API 키 관리

### 환경 변수 설정

```bash
# .env 파일
MOLIT_API_KEY=your_data_go_kr_api_key_here
HOMERADAR_DB_PATH=./data/homeradar.duckdb
```

### API 키 신청 링크

| Service | Portal | Approval Time |
|---------|--------|---------------|
| 국토부 실거래가 | [data.go.kr](https://www.data.go.kr/data/15126469/openapi.do) | Auto (1-2 hours) |
| 청약홈 분양정보 | [data.go.kr](https://www.data.go.kr/data/15098547/openapi.do) | Auto (1-2 hours) |

## 📚 추가 참고 자료

### 공식 문서
- [부동산 실거래가 공개시스템](https://rt.molit.go.kr/)
- [한국부동산원](https://www.reb.or.kr/)
- [청약홈](https://www.applyhome.co.kr/)

### 개발 가이드
- [PublicDataReader 문서](https://github.com/WooilJeong/PublicDataReader)
- [Korean News RSS URLs (GitHub Gist)](https://gist.github.com/koorukuroo/330a644fcc3c9ffdc7b6d537efd939c3)
- [부동산 API 연동 가이드](https://incmblog.com/부동산-실거래가-조회-api/)

### 커뮤니티 & 블로그
- [데이터 닥터 블로그](https://datadoctorblog.com/)
- [정우일 블로그](https://wooiljeong.github.io/)

## 🔄 업데이트 이력

| Date | Changes |
|------|---------|
| 2025-11-24 | Initial data sources research and configuration |
| | - Added 8 enabled sources (2 API, 6 RSS) |
| | - Documented 15 total sources across 3 tiers |
| | - Created usage guide and API key instructions |

---

**Note**: This document is maintained alongside [config/sources.yaml](../config/sources.yaml).
When adding new sources, update both files.
