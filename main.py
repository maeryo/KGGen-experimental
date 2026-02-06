"""
Knowledge Graph Generation & Neo4j Storage Pipeline

Flow: input.txt → KGGen → Cypher Queries → MCP → Neo4j
"""

import asyncio
import json
import os
import re
from datetime import datetime
from pathlib import Path

from kg_gen import KGGen
from mcp.client.streamable_http import streamablehttp_client
from mcp import ClientSession


# ──────────────────────────── Configuration ────────────────────────────
MCP_URL = "https://oasis-dev.haiqv.ai/mcp-server-neo4j/"

KG_CONFIG = {
    #"model": "openai/MiniMax-M2.1-AWQ",
    "model": "openai/GPT-OSS-120B",
    "temperature": 1.0,
    #"api_base": "https://atf-minimax-m21-awq.platform.haiqv.ai/v1",
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
    try:
        return kg.generate(input_data=text, chunk_size=5000, cluster=True)
    except Exception as e:
        print(f"[KGGen] 클러스터링 실패 (fallback → cluster=False): {type(e).__name__}: {e}")
        return kg.generate(input_data=text, chunk_size=5000, cluster=False)


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
    # 1. 입력 텍스트 읽기
    text = read_input(INPUT_FILE)
    if not text:
        print(f"Error: {INPUT_FILE}이 비어있습니다. 텍스트를 입력해주세요.")
        return

    print(f"[Input] {len(text)}자 읽음")
    print(f"[Input] 미리보기: {text[:300]}{'...' if len(text) > 300 else ''}")

    # 2. 지식 그래프 생성
    print("\n[KGGen] 지식 그래프 생성 중...")
    graph = generate_kg(text)
    print(f"[KGGen] 엔티티 {len(graph.entities)}개, 관계 {len(graph.relations)}개")
    save_graph(graph)

    # 3. Cypher 쿼리 생성
    queries = build_cypher_queries(graph)
    print(f"\n[Cypher] {len(queries)}개 쿼리 생성")
    save_queries(queries)

    # 4. MCP를 통해 Neo4j에 저장
    print("\n[Neo4j] MCP 서버 연결 중...")
    await execute_via_mcp(queries)
    print("\n완료! 지식 그래프가 Neo4j에 저장되었습니다.")


if __name__ == "__main__":
    asyncio.run(main())
