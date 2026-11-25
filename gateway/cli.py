"""网关命令行入口。

用法：

    python -m gateway.cli replay data/in-doubt-while-draining.jsonl
    python -m gateway.cli replay data/failover-deadline.jsonl --json
    python -m gateway.cli restart-demo data/in-doubt-while-draining.jsonl --store /tmp/gw.json
    python -m gateway.cli inspect data/in-doubt-while-draining.jsonl --session rounds
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from trace_io import read_trace

from .replay import EVENT_TYPES, ReplayEngine
from .service import Gateway
from .store import DurableStore
from .synthetic import SyntheticBackend


def _print_record(record: dict, as_json: bool) -> None:
    if as_json:
        print(json.dumps(record, ensure_ascii=False))
        return
    response = record["response"]
    status = response.get("status", "ok")
    label = response.get("attempt") or response.get("instance") or ""
    reason = response.get("reason", "")
    print(
        f"t={record['timestamp']:>3} {record['type']:<18} "
        f"{status:<24} {label:<10} {reason}"
    )


def cmd_replay(args: argparse.Namespace) -> None:
    engine = ReplayEngine()
    for record in engine.run_path(Path(args.trace)):
        _print_record(record, args.json)
    if not args.json:
        print("\n最终检查视图：")
        sessions = list(engine.gateway.sessions)
        if sessions:
            print(json.dumps(engine.gateway.inspect(sessions[0]), ensure_ascii=False, indent=2))


def cmd_inspect(args: argparse.Namespace) -> None:
    engine = ReplayEngine()
    engine.run_path(Path(args.trace))
    print(json.dumps(engine.gateway.inspect(args.session), ensure_ascii=False, indent=2))


def cmd_restart_demo(args: argparse.Namespace) -> None:
    """把演练从中间切开：前半段持久化后丢弃进程，后半段用新进程恢复。"""

    base = Path(args.store)
    gw_store = DurableStore(base.with_suffix(".gw.json"))
    backend_store = DurableStore(base.with_suffix(".backend.json"))
    events = list(read_trace(Path(args.trace), EVENT_TYPES))
    cut = len(events) // 2

    backend = SyntheticBackend(store=backend_store)
    gateway = Gateway(store=gw_store, backend=backend)
    engine = ReplayEngine(gateway)
    for event in events[:cut]:
        engine.handle(event)
    before = gateway.attempt_counter
    print(
        f"前半段结束，已授权 attempt:{before}，快照写入 "
        f"{gw_store.path} 与 {backend_store.path}"
    )

    # 模拟进程重启：新建全部内存对象，网关与合成后端分别从各自快照
    # 恢复（真实后端独立于网关进程存活）。
    backend2 = SyntheticBackend(store=DurableStore(backend_store.path))
    restored = Gateway(
        store=DurableStore(gw_store.path), backend=backend2
    )
    engine2 = ReplayEngine(restored)
    for event in events[cut:]:
        engine2.handle(event)
    reconciled = restored.reconcile()
    print(f"恢复后消解到 {len(reconciled)} 条迟到事实，attempt 总数仍为 {restored.attempt_counter}")
    print(json.dumps(restored.inspect(events and _first_session(events)), ensure_ascii=False, indent=2))


def _first_session(events: list[dict]) -> str:
    for event in events:
        if event["type"] == "open":
            return event["data"]["session"]
    raise ValueError("演练中没有 open 事件")


def main() -> None:
    parser = argparse.ArgumentParser(description="推理会话网关本地演练工具")
    sub = parser.add_subparsers(dest="command", required=True)

    replay = sub.add_parser("replay", help="回放整条演练并打印网关响应")
    replay.add_argument("trace")
    replay.add_argument("--json", action="store_true", help="输出 JSON 行")
    replay.set_defaults(func=cmd_replay)

    inspect_cmd = sub.add_parser("inspect", help="回放后打印会话检查视图")
    inspect_cmd.add_argument("trace")
    inspect_cmd.add_argument("--session", required=True)
    inspect_cmd.set_defaults(func=cmd_inspect)

    demo = sub.add_parser("restart-demo", help="中途重启恢复演示")
    demo.add_argument("trace")
    demo.add_argument("--store", required=True)
    demo.set_defaults(func=cmd_restart_demo)

    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
