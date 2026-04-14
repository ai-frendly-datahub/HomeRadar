# HOMERADAR

부동산 시장 데이터 수집·분석 레이더. RSS 뉴스 + MOLIT(국토교통부) 실거래가 API → 엔티티 추출 → DuckDB 그래프 저장.

## STRUCTURE

```
HomeRadar/
├── collectors/
│   ├── base.py               # BaseCollector ABC
│   ├── rss_collector.py      # RSS 뉴스 수집
│   ├── molit_collector.py    # 국토교통부 실거래가 API
│   └── registry.py           # CollectorRegistry.create_collector()
├── analyzers/
│   ├── entity_extractor.py   # EntityExtractor — 부동산 엔티티 추출
│   └── realestate_entities_data.py  # 지역/건물/가격대 키워드 사전
├── graph/
│   ├── graph_store.py        # GraphStore — DuckDB 노드/엣지 저장
│   ├── graph_queries.py      # 관계 기반 쿼리
│   └── search_index.py       # SQLite FTS5 전문 검색
├── reporters/                # 리포트 생성
├── pushers/                  # 외부 전송 (확장용)
├── mcp_server/               # MCP 서버 (server.py + tools.py)
├── raw_logger.py             # JSONL 원시 로깅
├── nl_query.py               # 자연어 쿼리 파서
├── demo_molit.py             # MOLIT API 데모
├── demo_pipeline.py          # 전체 파이프라인 데모
├── demo_rss.py               # RSS 수집 데모
├── config/sources.yaml       # 소스 설정 (categories/ 대신 sources.yaml)
└── main.py                   # --mode once|scheduler --sources molit,rss
```

## WHERE TO LOOK

| Task | Location | Notes |
|------|----------|-------|
| 새 소스 추가 | `collectors/`, `config/sources.yaml` | BaseCollector 상속 + registry 등록 |
| 엔티티 키워드 확장 | `analyzers/realestate_entities_data.py` | 지역명/건물유형/가격대 딕셔너리 |
| 그래프 쿼리 | `graph/graph_queries.py` | DuckDB SQL 기반 노드/엣지 탐색 |
| MOLIT API 키 | 환경변수 `MOLIT_SERVICE_KEY` | main.py에서 os.environ 참조 |

## DEVIATIONS FROM TEMPLATE

- **Config**: `config/sources.yaml` 사용 (categories/ 패턴 아님)
- **Storage**: RadarStorage 대신 `GraphStore` (노드/엣지 관계형)
- **Entry point**: `--category` 대신 `--mode once|scheduler --sources <id,...>`
- **Collector 패턴**: `CollectorRegistry.create_collector(source_id, config)` 팩토리
- **로깅**: structlog 대신 stdlib `logging` + 파일 핸들러 (`logs/homeradar.log`)

## COMMANDS

```bash
python main.py --mode once
python main.py --mode scheduler --interval 24
python main.py --sources molit_apt_transaction,hankyung_realestate
MOLIT_SERVICE_KEY=<key> python main.py --mode once

pytest tests/unit -m unit
pytest tests/ -m "not network"
```
