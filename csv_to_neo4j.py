"""
조달청 대금종결집계내역(납품요구) CSV → Neo4j 그래프 적재 스크립트

노드:
  - Contract  : 조달 계약 건
  - Company   : 납품 업체
  - Organization : 발주 기관
  - Department : 기관 내 담당 부서
  - Payment   : 대금 지급 내역

관계:
  - (Company)-[:납품]->(Contract)
  - (Organization)-[:발주]->(Contract)
  - (Department)-[:소속]->(Organization)
  - (Payment)-[:지급대상]->(Contract)
"""

import csv
import os
import sys
import time
from pathlib import Path

from neo4j import GraphDatabase

# ──────────────────────────── 설정 ────────────────────────────

NEO4J_URI = os.getenv("NEO4J_URI", "bolt://localhost:7687")
NEO4J_USER = os.getenv("NEO4J_USER", "neo4j")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD", "password")

CSV_PATH = Path(__file__).parent / "조달청_대금종결집계내역_납품요구_20240802.csv"

BATCH_SIZE = 1000  # 한 번에 커밋할 행 수


# ──────────────────────────── 인덱스/제약조건 생성 ────────────────────────────

CONSTRAINT_QUERIES = [
    # UNIQUE 제약조건 (자동으로 인덱스 생성)
    "CREATE CONSTRAINT IF NOT EXISTS FOR (c:Contract) REQUIRE c.대금지급번호 IS UNIQUE",
    "CREATE CONSTRAINT IF NOT EXISTS FOR (p:Payment) REQUIRE p.대금지급번호 IS UNIQUE",
    # 인덱스
    "CREATE INDEX IF NOT EXISTS FOR (co:Company) ON (co.업체명)",
    "CREATE INDEX IF NOT EXISTS FOR (o:Organization) ON (o.기관명)",
    "CREATE INDEX IF NOT EXISTS FOR (d:Department) ON (d.부서명, d.기관명)",
]


# ──────────────────────────── Cypher 쿼리 ────────────────────────────

UPSERT_QUERY = """
UNWIND $rows AS row

// ① Contract 노드
MERGE (contract:Contract {대금지급번호: row.대금지급번호})
  ON CREATE SET
    contract.계약납품요구번호 = row.계약납품요구번호,
    contract.계약금액        = toInteger(row.계약금액),
    contract.계약건명        = row.계약건명,
    contract.업무구분명      = row.업무구분명,
    contract.계약납품구분    = row.계약납품구분,
    contract.조달구분        = row.조달구분,
    contract.대금지급처리구분 = row.대금지급처리구분

// ② Company 노드
MERGE (company:Company {업체명: row.업체명})
  ON CREATE SET
    company.대표자명 = row.대표자명

// ③ Organization 노드
MERGE (org:Organization {기관명: row.기관명})

// ④ Department 노드 (부서명+기관명 조합으로 구분)
MERGE (dept:Department {부서명: row.기관담당부서명, 기관명: row.기관명})
  ON CREATE SET
    dept.담당자명 = row.기관담당자명

// ⑤ Payment 노드
MERGE (payment:Payment {대금지급번호: row.대금지급번호})
  ON CREATE SET
    payment.지출구분명      = row.지출구분명,
    payment.지체일수        = toInteger(row.지체일수),
    payment.지출금액        = toInteger(row.지출금액),
    payment.지급요청금액    = toInteger(row.지급요청금액),
    payment.지급결정금액    = toInteger(row.지급결정금액),
    payment.차감금액        = toInteger(row.차감금액),
    payment.선급금금액      = toInteger(row.선급금금액),
    payment.선급금청산금액  = toInteger(row.선급금청산금액),
    payment.지체상금금액    = toInteger(row.지체상금금액),
    payment.입력일자        = date(row.입력일자)

// ⑥ 관계
MERGE (company)-[:납품]->(contract)
MERGE (org)-[:발주]->(contract)
MERGE (dept)-[:소속]->(org)
MERGE (payment)-[:지급대상]->(contract)
"""


# ──────────────────────────── CSV 로드 ────────────────────────────

def load_csv(path: Path) -> list[dict]:
    """CSV 파일을 dict 리스트로 읽어 반환한다."""
    rows = []
    with open(path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            rows.append(row)
    return rows


# ──────────────────────────── 배치 적재 ────────────────────────────

def create_constraints(session):
    """인덱스 및 제약조건을 생성한다."""
    for q in CONSTRAINT_QUERIES:
        session.run(q)
    print(f"[✓] 인덱스/제약조건 {len(CONSTRAINT_QUERIES)}개 생성 완료")


def ingest_batch(tx, rows: list[dict]):
    """트랜잭션 내에서 배치 UPSERT를 실행한다."""
    tx.run(UPSERT_QUERY, rows=rows)


def ingest(driver, rows: list[dict]):
    """전체 행을 배치로 나누어 Neo4j에 적재한다."""
    total = len(rows)
    print(f"[i] 총 {total:,}행 적재 시작 (배치 크기: {BATCH_SIZE})")

    t0 = time.time()
    ingested = 0

    for start in range(0, total, BATCH_SIZE):
        batch = rows[start : start + BATCH_SIZE]
        with driver.session() as session:
            session.execute_write(ingest_batch, batch)
        ingested += len(batch)
        elapsed = time.time() - t0
        rate = ingested / elapsed if elapsed > 0 else 0
        print(f"  ↳ {ingested:>8,} / {total:,}  ({ingested*100/total:5.1f}%)  [{rate:,.0f} rows/s]")

    elapsed = time.time() - t0
    print(f"[✓] 적재 완료: {total:,}행, {elapsed:.1f}초 소요")


# ──────────────────────────── 통계 출력 ────────────────────────────

def print_stats(driver):
    """적재 후 노드/관계 통계를 출력한다."""
    queries = [
        ("Contract",     "MATCH (n:Contract) RETURN count(n) AS cnt"),
        ("Company",      "MATCH (n:Company) RETURN count(n) AS cnt"),
        ("Organization", "MATCH (n:Organization) RETURN count(n) AS cnt"),
        ("Department",   "MATCH (n:Department) RETURN count(n) AS cnt"),
        ("Payment",      "MATCH (n:Payment) RETURN count(n) AS cnt"),
        ("납품 관계",     "MATCH ()-[r:납품]->() RETURN count(r) AS cnt"),
        ("발주 관계",     "MATCH ()-[r:발주]->() RETURN count(r) AS cnt"),
        ("소속 관계",     "MATCH ()-[r:소속]->() RETURN count(r) AS cnt"),
        ("지급대상 관계", "MATCH ()-[r:지급대상]->() RETURN count(r) AS cnt"),
    ]
    print("\n── Neo4j 그래프 통계 ──")
    with driver.session() as session:
        for label, q in queries:
            result = session.run(q).single()
            cnt = result["cnt"] if result else 0
            print(f"  {label:<16} : {cnt:>10,}")


# ──────────────────────────── 메인 ────────────────────────────

def main():
    # 1) CSV 읽기
    if not CSV_PATH.exists():
        print(f"[✗] CSV 파일을 찾을 수 없습니다: {CSV_PATH}")
        sys.exit(1)

    print(f"[i] CSV 읽는 중: {CSV_PATH.name}")
    rows = load_csv(CSV_PATH)
    print(f"[✓] CSV 읽기 완료: {len(rows):,}행")

    # 2) Neo4j 연결
    print(f"[i] Neo4j 연결 중: {NEO4J_URI}")
    driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
    try:
        driver.verify_connectivity()
        print("[✓] Neo4j 연결 성공")
    except Exception as e:
        print(f"[✗] Neo4j 연결 실패: {e}")
        sys.exit(1)

    try:
        # 3) 인덱스/제약조건
        with driver.session() as session:
            create_constraints(session)

        # 4) 데이터 적재
        ingest(driver, rows)

        # 5) 통계 출력
        print_stats(driver)

    finally:
        driver.close()
        print("[i] Neo4j 연결 종료")


if __name__ == "__main__":
    main()
