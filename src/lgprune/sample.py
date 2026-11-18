"""Example database made by REAL LangGraph graphs (no LLM: plain function nodes).

The conversation text is synthetic (`msg-<n>-xxxx...`), but every row is a real
checkpoint written by SqliteSaver / PostgresSaver, so pruning it behaves like
pruning your own database.

    A  chat graph, 20 turns  -> 60 checkpoints, messages grow every turn
    B  chat graph, 5 turns
    C  approval graph, stopped at interrupt("approve?")  -> waiting for a human
    D  DeltaChannel graph, 12 turns (snapshot every 5 updates)
"""

from __future__ import annotations

import operator
import sqlite3
from pathlib import Path
from typing import Annotated, Any, TypedDict

from langgraph.channels.delta import DeltaChannel
from langgraph.graph import END, START, StateGraph
from langgraph.types import interrupt

SAMPLE_THREADS = ("A", "B", "C", "D")
A_TURNS, B_TURNS, D_TURNS = 20, 5, 12


class ChatState(TypedDict):
    messages: Annotated[list, operator.add]


def _chat(state: ChatState) -> dict[str, Any]:
    n = len(state["messages"])
    # 합성 대화: 메시지마다 400자 — 매 턴 messages 전체가 다시 저장되어 크기가 제곱으로 는다
    return {"messages": [f"msg-{n}-" + "x" * 400]}


def _approve(state: ChatState) -> dict[str, Any]:
    answer = interrupt("approve?")
    return {"messages": [f"approved:{answer}"]}


def build_chat(checkpointer: Any) -> Any:
    g = StateGraph(ChatState)
    g.add_node("chat", _chat)
    g.add_edge(START, "chat")
    g.add_edge("chat", END)
    return g.compile(checkpointer=checkpointer)


def build_approval(checkpointer: Any) -> Any:
    g = StateGraph(ChatState)
    g.add_node("chat", _chat)
    g.add_node("approve", _approve)
    g.add_edge(START, "chat")
    g.add_edge("chat", "approve")
    g.add_edge("approve", END)
    return g.compile(checkpointer=checkpointer)


def _delta_reducer(state: list | None, batches: list) -> list:
    out = list(state or [])
    for b in batches:
        out.extend(b if isinstance(b, list) else [b])
    return out


class DeltaState(TypedDict):
    # DeltaChannel: 체크포인트에 값 대신 표지만 남기고, 조상 체크포인트의 쓰기로 값을 다시 만든다
    messages: Annotated[list, DeltaChannel(_delta_reducer, snapshot_frequency=5)]


def _delta_chat(state: DeltaState) -> dict[str, Any]:
    return {"messages": [f"d-{len(state['messages'])}"]}


def build_delta(checkpointer: Any) -> Any:
    g = StateGraph(DeltaState)
    g.add_node("chat", _delta_chat)
    g.add_edge(START, "chat")
    g.add_edge("chat", END)
    return g.compile(checkpointer=checkpointer)


def graph_for(thread_id: str, checkpointer: Any) -> Any:
    """The graph that wrote a sample thread (needed to read it back with get_state)."""
    if thread_id == "C":
        return build_approval(checkpointer)
    if thread_id == "D":
        return build_delta(checkpointer)
    return build_chat(checkpointer)


def config(thread_id: str) -> dict[str, Any]:
    return {"configurable": {"thread_id": thread_id}}


def fill(checkpointer: Any) -> None:
    """Run the four sample threads against any checkpointer."""
    chat, approval, delta = build_chat(checkpointer), build_approval(checkpointer), build_delta(checkpointer)
    for i in range(A_TURNS):
        chat.invoke({"messages": [f"user-{i}"]}, config("A"))
    for i in range(B_TURNS):
        chat.invoke({"messages": [f"user-{i}"]}, config("B"))
    approval.invoke({"messages": ["need approval"]}, config("C"))  # interrupt 에서 멈춤
    for i in range(D_TURNS):
        delta.invoke({"messages": [f"u{i}"]}, config("D"))


def create_sample(path: str | Path) -> Path:
    """Create a new SQLite sample database. Refuses to overwrite an existing file."""
    from langgraph.checkpoint.sqlite import SqliteSaver

    from .errors import LgpruneError

    target = Path(path).expanduser().resolve()
    if target.exists():
        raise LgpruneError(f"{target} already exists", key="err.exists", path=str(target))
    target.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(target), check_same_thread=False)
    try:
        fill(SqliteSaver(conn))
    finally:
        conn.close()  # 마지막 연결이 닫히면 WAL 이 본 파일로 합쳐진다
    return target


def get_state_values(db_path: str | Path, thread_id: str) -> dict[str, Any]:
    """Read a sample thread back through LangGraph (`graph.get_state(...).values`)."""
    from langgraph.checkpoint.sqlite import SqliteSaver

    conn = sqlite3.connect(str(db_path), check_same_thread=False)
    try:
        return dict(graph_for(thread_id, SqliteSaver(conn)).get_state(config(thread_id)).values)
    finally:
        conn.close()
