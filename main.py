"""
Knowledge Graph Generation & Neo4j Storage Pipeline

Flow: input.txt → KGGen → Cypher Queries → MCP → Neo4j
"""

import asyncio
import json
import os
import re
import threading
import time
import functools
from datetime import datetime
from pathlib import Path

from kg_gen import KGGen
from mcp.client.streamable_http import streamablehttp_client
from mcp import ClientSession


# ──────────────────────────── Progress Monitoring ──────────────────────

class ProgressMonitor:
    """KGGen 파이프라인 진행 상황을 추적하고 하트비트를 출력하는 모니터."""

    def __init__(self):
        self._lock = threading.Lock()
        self._current_phase = ""
        self._detail = ""
        self._start_time = time.time()
        self._phase_start = time.time()
        self._heartbeat_thread: threading.Thread | None = None
        self._stop_event = threading.Event()
        self._llm_call_count = 0
        self._chunk_done = 0
        self._chunk_total = 0
        self._batch_info = ""  # 배치 진행률 문자열

    def start(self):
        """하트비트 스레드 시작."""
        self._start_time = time.time()
        self._stop_event.clear()
        self._heartbeat_thread = threading.Thread(target=self._heartbeat_loop, daemon=True)
        self._heartbeat_thread.start()

    def stop(self):
        """하트비트 스레드 중지."""
        self._stop_event.set()
        if self._heartbeat_thread:
            self._heartbeat_thread.join(timeout=2)

    def set_phase(self, phase: str, detail: str = ""):
        with self._lock:
            self._current_phase = phase
            self._detail = detail
            self._phase_start = time.time()
        elapsed = self._elapsed_total()
        print(f"  [{elapsed}] ▶ {phase}" + (f" — {detail}" if detail else ""))

    def update_detail(self, detail: str):
        with self._lock:
            self._detail = detail
        elapsed = self._elapsed_total()
        print(f"  [{elapsed}]   {detail}")

    def inc_llm_calls(self, label: str = ""):
        with self._lock:
            self._llm_call_count += 1
            count = self._llm_call_count
        elapsed = self._elapsed_total()
        msg = f"  [{elapsed}]   LLM 호출 #{count}"
        if label:
            msg += f" ({label})"
        print(msg)

    def set_batch_info(self, info: str):
        with self._lock:
            self._batch_info = info

    def set_chunk_progress(self, done: int, total: int):
        with self._lock:
            self._chunk_done = done
            self._chunk_total = total

    def _elapsed_total(self) -> str:
        secs = time.time() - self._start_time
        m, s = divmod(int(secs), 60)
        return f"{m:02d}:{s:02d}"

    def _heartbeat_loop(self):
        """15초마다 현재 상태를 출력하는 루프."""
        while not self._stop_event.wait(timeout=15):
            with self._lock:
                phase = self._current_phase
                detail = self._detail
                phase_elapsed = time.time() - self._phase_start
                llm_calls = self._llm_call_count
                chunk_done = self._chunk_done
                chunk_total = self._chunk_total
                batch_info = self._batch_info

            elapsed = self._elapsed_total()
            phase_m, phase_s = divmod(int(phase_elapsed), 60)
            parts = [f"  [{elapsed}] ♥ 살아있음"]
            if phase:
                parts.append(f"현재: {phase} ({phase_m:02d}:{phase_s:02d})")
            if batch_info:
                parts.append(batch_info)
            elif detail:
                parts.append(detail)
            parts.append(f"LLM 호출 누적: {llm_calls}회")
            if chunk_total > 0:
                parts.append(f"청크: {chunk_done}/{chunk_total}")
            print(" | ".join(parts))


# 글로벌 모니터 인스턴스
_monitor = ProgressMonitor()


def _install_kggen_hooks():
    """KGGen 내부 함수들에 진행률 로깅을 위한 몽키패치 적용."""
    import kg_gen.steps._1_get_entities as ent_mod
    import kg_gen.steps._2_get_relations as rel_mod
    import kg_gen.steps._3_cluster_graph as cluster_mod

    # ── get_entities 래핑 ──
    _orig_get_entities = ent_mod.get_entities
    @functools.wraps(_orig_get_entities)
    def _patched_get_entities(*args, **kwargs):
        _monitor.inc_llm_calls("엔티티 추출")
        result = _orig_get_entities(*args, **kwargs)
        _monitor.update_detail(f"  → 엔티티 {len(result)}개 추출됨")
        return result
    ent_mod.get_entities = _patched_get_entities

    # ── get_relations 래핑 ──
    _orig_get_relations = rel_mod.get_relations
    @functools.wraps(_orig_get_relations)
    def _patched_get_relations(*args, **kwargs):
        _monitor.inc_llm_calls("관계 추출")
        result = _orig_get_relations(*args, **kwargs)
        _monitor.update_detail(f"  → 관계 {len(result)}개 추출됨")
        return result
    rel_mod.get_relations = _patched_get_relations

    # ── cluster_items 래핑 ──
    _orig_cluster_items = cluster_mod.cluster_items
    @functools.wraps(_orig_cluster_items)
    def _patched_cluster_items(dspy_ref, items, item_type: str = "entities", context: str = ""):
        total = len(items)
        _monitor.set_phase(
            f"클러스터링: {item_type}",
            f"대상 {total}개 (while 루프 최대 {8}회 → 이후 배치 {total//10}~{total//10+10}회 예상)"
        )
        result = _orig_cluster_items(dspy_ref, items, item_type, context)  # type: ignore[arg-type]
        new_items, clusters_dict = result
        _monitor.update_detail(
            f"  → {item_type} 클러스터링 완료: {total}개 → {len(new_items)}개 대표, {len(clusters_dict)}개 클러스터"
        )
        return result
    cluster_mod.cluster_items = _patched_cluster_items

    # ── _process_batch 래핑 ──
    _batch_counter = {"entity_total": 0, "entity_done": 0, "edge_total": 0, "edge_done": 0, "current_type": "entities"}
    _orig_process_batch = cluster_mod._process_batch
    @functools.wraps(_orig_process_batch)
    def _patched_process_batch(batch, clusters, context, validate):
        # context 문자열에서 현재 타입을 추론
        btype = _batch_counter["current_type"]
        _batch_counter[f"{btype}_done"] = _batch_counter.get(f"{btype}_done", 0) + 1
        done = _batch_counter[f"{btype}_done"]
        total = _batch_counter[f"{btype}_total"]
        pct = f" ({done*100//total}%)" if total > 0 else ""
        _monitor.inc_llm_calls(
            f"배치 {done}/{total}{pct} — {btype} {len(batch)}개 → 클러스터 {len(clusters)}개"
        )
        _monitor.set_batch_info(f"배치 {done}/{total}{pct} ({btype})")
        return _orig_process_batch(batch, clusters, context, validate)
    cluster_mod._process_batch = _patched_process_batch

    # ── 원본 cluster_items 내부에서 배치 총 수를 미리 알기 위해 한층 더 래핑 ──
    # cluster_items → while loop 끝난 뒤 remaining_items를 배치 처리하는데,
    # 그 시점의 remaining count를 알기 위해 while loop 이후 호출되는 _process_batch의
    # 첫 호출에서 total을 계산합니다.
    _orig_cluster_items_inner = cluster_mod.cluster_items  # 이미 패치된 버전
    @functools.wraps(_orig_cluster_items_inner)
    def _patched_cluster_items_with_batch_count(dspy_ref, items, item_type: str = "entities", context: str = ""):
        # 배치 카운터 초기화
        btype = "entity" if "entit" in item_type else "edge"
        _batch_counter["current_type"] = btype
        total_batches = max(1, len(items) // 10 + (1 if len(items) % 10 else 0))
        _batch_counter[f"{btype}_total"] = total_batches
        _batch_counter[f"{btype}_done"] = 0
        return _orig_cluster_items_inner(dspy_ref, items, item_type, context)
    cluster_mod.cluster_items = _patched_cluster_items_with_batch_count

    # ── cluster_graph 래핑 ──
    _orig_cluster_graph = cluster_mod.cluster_graph
    @functools.wraps(_orig_cluster_graph)
    def _patched_cluster_graph(graph, context=""):
        _monitor.set_phase(
            "그래프 클러스터링 시작",
            f"엔티티 {len(graph.entities)}개, 엣지 {len(graph.edges)}개를 클러스터링합니다"
        )
        result = _orig_cluster_graph(graph, context)
        _monitor.set_phase(
            "그래프 클러스터링 완료",
            f"엔티티 {len(graph.entities)}→{len(result.entities)}개, 엣지 {len(graph.edges)}→{len(result.edges)}개"
        )
        return result
    cluster_mod.cluster_graph = _patched_cluster_graph

    print("[Monitor] KGGen 내부 함수에 진행률 로깅 패치 적용 완료")


# ──────────────────────────── Configuration ────────────────────────────
MCP_URL = "https://oasis-dev.haiqv.ai/mcp-server-neo4j/"

KG_CONFIG = {
    "model": "openai/GPT-OSS-120B",
    "temperature": 1.0,
    "api_base": "https://atf-gpt120.platform.haiqv.ai/v1",
    "api_key": "1"
}

INPUT_FILE = "input.txt"
OUTPUT_DIR = "output"


# ──────────────────────────── Persistence ──────────────────────────────

def _timestamp() -> str:
    return datetime.now().strftime("%Y%m%d_%H%M%S")


def save_graph(graph, output_dir: str = OUTPUT_DIR) -> str:
    """KGGen graph 객체를 JSON 텍스트 파일로 저장."""
    Path(output_dir).mkdir(parents=True, exist_ok=True)
    path = os.path.join(output_dir, f"graph_{_timestamp()}.json")
    data = {
        "entities": sorted(graph.entities),
        "edges": sorted(graph.edges),
        "relations": sorted(
            [list(r) for r in graph.relations], key=lambda x: (x[0], x[2])
        ),
        "entity_clusters": {
            k: sorted(v) for k, v in graph.entity_clusters.items()
        },
        "edge_clusters": {
            k: sorted(v) for k, v in graph.edge_clusters.items()
        },
    }
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    print(f"[Save] graph → {path}")
    return path


def save_queries(queries: list[str], output_dir: str = OUTPUT_DIR) -> str:
    """Cypher 쿼리 리스트를 텍스트 파일로 저장 (한 줄에 하나씩)."""
    Path(output_dir).mkdir(parents=True, exist_ok=True)
    path = os.path.join(output_dir, f"queries_{_timestamp()}.cypher")
    with open(path, "w", encoding="utf-8") as f:
        f.write("\n".join(queries) + "\n")
    print(f"[Save] queries → {path}")
    return path


# ──────────────────────────── KG Generation ────────────────────────────

def read_input(path: str) -> str:
    with open(path, "r", encoding="utf-8") as f:
        return f.read().strip()


def generate_kg(text: str):
    """KGGen을 사용하여 텍스트에서 지식 그래프 생성. 클러스터링 실패 시 자동 fallback."""
    kg = KGGen(**KG_CONFIG)

    # 텍스트 크기에 따른 예상 정보 출력
    est_chunks = max(1, len(text) // 5000 + (1 if len(text) % 5000 else 0))
    print(f"[KGGen] 텍스트 {len(text)}자, 예상 청크 ~{est_chunks}개 (chunk_size=5000)")
    print(f"[KGGen] 예상 최소 LLM 호출: 청크당 ~2회 × {est_chunks}청크 = ~{est_chunks * 2}회 (클러스터링 제외)")
    print("[KGGen] cluster=True → 클러스터링 시 수십 회 추가 LLM 호출이 발생합니다")
    print("[KGGen] ⏳ 전체 소요 시간: 수 분 ~ 수십 분 (텍스트 크기 및 API 응답 속도에 따라 변동)")

    _monitor.set_phase("KGGen generate()", f"cluster=True, chunk_size=5000, 텍스트 {len(text)}자")
    t0 = time.time()

    try:
        result = kg.generate(input_data=text, chunk_size=5000, cluster=True)
        elapsed = time.time() - t0
        print(f"[KGGen] ✓ generate() 완료 ({elapsed:.1f}초 소요)")
        return result
    except Exception as e:
        elapsed = time.time() - t0
        print(f"[KGGen] ✗ 클러스터링 실패 ({elapsed:.1f}초 후): {type(e).__name__}: {e}")
        print("[KGGen] fallback → cluster=False 로 재시도합니다...")
        _monitor.set_phase("KGGen fallback", "cluster=False로 재생성")
        t1 = time.time()
        result = kg.generate(input_data=text, chunk_size=5000, cluster=False)
        print(f"[KGGen] ✓ fallback 완료 ({time.time() - t1:.1f}초 소요)")
        return result


# ──────────────────────────── Cypher Builder ───────────────────────────

def sanitize_rel_type(predicate: str) -> str:
    """Predicate를 Neo4j 관계 타입으로 변환 (대문자, 공백→언더스코어)."""
    cleaned = re.sub(r"\W", "_", predicate.upper())
    cleaned = re.sub(r"_+", "_", cleaned).strip("_")
    return cleaned or "RELATED_TO"


def escape(s: str) -> str:
    """Cypher 문자열 이스케이프."""
    return s.replace("\\", "\\\\").replace("'", "\\'")


def build_cypher_queries(graph) -> list[str]:
    """KGGen 결과를 Cypher 쿼리 리스트로 변환."""
    queries: list[str] = []

    # 1) 엔티티 생성 (클러스터 정보 포함)
    for entity in graph.entities:
        cluster_name = None
        for cluster, members in graph.entity_clusters.items():
            if entity in members:
                cluster_name = cluster
                break

        if cluster_name and cluster_name != entity:
            queries.append(
                f"MERGE (e:Entity {{name: '{escape(entity)}'}}) "
                f"SET e.cluster = '{escape(cluster_name)}'"
            )
        else:
            queries.append(
                f"MERGE (e:Entity {{name: '{escape(entity)}'}})"
            )

    # 2) 관계 생성
    for subject, predicate, obj in graph.relations:
        rel_type = sanitize_rel_type(predicate)
        queries.append(
            f"MATCH (a:Entity {{name: '{escape(subject)}'}}), "
            f"(b:Entity {{name: '{escape(obj)}'}}) "
            f"MERGE (a)-[:{rel_type}]->(b)"
        )

    return queries


# ──────────────────────────── MCP Execution ────────────────────────────

_TOOL_PRIORITY = {"write": 0, "run": 1, "execute": 1, "query": 2, "cypher": 2}
_QUERY_PARAM_NAMES = {"query", "cypher", "statement", "input"}


def _rank_tool(tool) -> int | None:
    """tool 이름 기반 우선순위 반환. 매칭 안 되면 None."""
    name_lower = tool.name.lower()
    for keyword, priority in _TOOL_PRIORITY.items():
        if keyword in name_lower:
            return priority
    return None


def _extract_param_name(tool) -> str:
    """tool의 inputSchema에서 쿼리 파라미터명 추출."""
    props = (tool.inputSchema or {}).get("properties", {})
    for key in props:
        if key.lower() in _QUERY_PARAM_NAMES:
            return key
    # fallback: 첫 번째 string 파라미터 또는 기본값
    for key, schema in props.items():
        if schema.get("type") == "string":
            return key
    return "query"


def find_write_tool(tools) -> tuple[str, str] | None:
    """MCP 서버에서 Cypher 쿼리 실행에 적합한 tool과 파라미터명을 탐색."""
    ranked = [(r, t) for t in tools if (r := _rank_tool(t)) is not None]
    if not ranked:
        return None
    ranked.sort(key=lambda x: x[0])
    tool = ranked[0][1]
    return tool.name, _extract_param_name(tool)


def _print_tools(tools) -> None:
    """사용 가능한 MCP tools 목록 출력."""
    print("\n[MCP] 사용 가능한 tools:")
    for tool in tools:
        desc = (tool.description or "")[:80]
        props = (tool.inputSchema or {}).get("properties", {})
        schema_keys = ", ".join(props.keys())
        print(f"  - {tool.name}({schema_keys}): {desc}")


def _extract_text(result) -> str | None:
    """MCP 결과에서 텍스트 추출."""
    texts = [
        item.text for item in (result.content or [])
        if hasattr(item, "text") and isinstance(item.text, str)
    ]
    return texts[0][:200] if texts else None


async def _run_queries(session: ClientSession, tool_name: str, param_name: str, queries: list[str]):
    """Cypher 쿼리들을 MCP session을 통해 순차 실행."""
    success, fail = 0, 0
    for i, query in enumerate(queries, 1):
        print(f"\n[{i}/{len(queries)}] {query[:100]}{'...' if len(query) > 100 else ''}")
        try:
            result = await session.call_tool(tool_name, {param_name: query})
            text = _extract_text(result)
            if text:
                print(f"  ✓ {text}")
            success += 1
        except Exception as e:
            print(f"  ✗ Error: {e}")
            fail += 1
    print(f"\n[MCP] 완료: {success} 성공, {fail} 실패 / 총 {len(queries)} 쿼리")


async def execute_via_mcp(queries: list[str]):
    """MCP를 통해 Cypher 쿼리들을 Neo4j에 실행."""
    #async with streamablehttp_client(url=MCP_URL, headers=MCP_HEADERS) as (
    #    read_stream, write_stream, _,
    #):
    async with streamablehttp_client(url=MCP_URL) as (
        read_stream, write_stream, _,
    ):
        async with ClientSession(read_stream, write_stream) as session:
            await session.initialize()

            tools_result = await session.list_tools()
            _print_tools(tools_result.tools)

            result = find_write_tool(tools_result.tools)
            if not result:
                print("\n[MCP] Error: 쿼리 실행 가능한 tool을 찾을 수 없습니다.")
                print("  사용 가능한 tools:", [t.name for t in tools_result.tools])
                return

            tool_name, param_name = result
            print(f"\n[MCP] 사용할 tool: {tool_name} (param: {param_name})")
            await _run_queries(session, tool_name, param_name, queries)


# ──────────────────────────── Main ─────────────────────────────────────

async def main():
    pipeline_start = time.time()
    print("=" * 60)
    print(" Knowledge Graph Generation Pipeline")
    print(f" 시작: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)

    # 몽키패치 적용 및 모니터 시작
    _install_kggen_hooks()
    _monitor.start()

    try:
        # 1. 입력 텍스트 읽기
        text = read_input(INPUT_FILE)
        if not text:
            print(f"Error: {INPUT_FILE}이 비어있습니다. 텍스트를 입력해주세요.")
            return

        print(f"\n[Input] {len(text)}자 읽음 ({len(text)/1000:.1f}KB)")
        print(f"[Input] 미리보기: {text[:300]}{'...' if len(text) > 300 else ''}")

        # 2. 지식 그래프 생성
        print("\n" + "─" * 60)
        print("[Step 2/4] 지식 그래프 생성")
        print("─" * 60)
        step_start = time.time()
        graph = generate_kg(text)
        step_elapsed = time.time() - step_start
        print(f"[KGGen] 결과: 엔티티 {len(graph.entities)}개, 관계 {len(graph.relations)}개, 엣지 {len(graph.edges)}개")
        if graph.entity_clusters:
            print(f"[KGGen]   엔티티 클러스터: {len(graph.entity_clusters)}개")
        if graph.edge_clusters:
            print(f"[KGGen]   엣지 클러스터: {len(graph.edge_clusters)}개")
        print(f"[KGGen] ⏱ Step 2 소요: {step_elapsed:.1f}초")
        save_graph(graph)

        # 3. Cypher 쿼리 생성
        print("\n" + "─" * 60)
        print("[Step 3/4] Cypher 쿼리 변환")
        print("─" * 60)
        queries = build_cypher_queries(graph)
        print(f"[Cypher] {len(queries)}개 쿼리 생성")
        save_queries(queries)

        # 4. MCP를 통해 Neo4j에 저장
        print("\n" + "─" * 60)
        print("[Step 4/4] Neo4j 저장 (MCP)")
        print("─" * 60)
        _monitor.set_phase("MCP Neo4j 저장")
        await execute_via_mcp(queries)

    finally:
        _monitor.stop()

    total_elapsed = time.time() - pipeline_start
    m, s = divmod(int(total_elapsed), 60)
    print("\n" + "=" * 60)
    print(f" ✓ 완료! 총 소요 시간: {m}분 {s}초")
    print(f" LLM 호출 횟수: {_monitor._llm_call_count}회")
    print("=" * 60)


if __name__ == "__main__":
    asyncio.run(main())
