"""用 data/ 下的两条完整演练验证端到端行为。"""

from __future__ import annotations

import unittest
from pathlib import Path

from gateway.models import TurnState
from gateway.replay import run_trace
from gateway.service import Gateway
from gateway.store import DurableStore
from gateway.synthetic import SyntheticBackend

DATA = Path(__file__).resolve().parent.parent / "data"


class InDoubtScenarioTests(unittest.TestCase):
    def setUp(self) -> None:
        self.engine_journal = run_trace(DATA / "in-doubt-while-draining.jsonl")
        # 再单独跑一份以便直接检查网关对象。
        self.gateway = Gateway()
        from gateway.replay import ReplayEngine

        ReplayEngine(self.gateway).run_path(DATA / "in-doubt-while-draining.jsonl")
        self.session = self.gateway.sessions["rounds"]

    def test_journal_shape(self) -> None:
        self.assertEqual(len(self.engine_journal), 19)
        self.assertTrue(all("response" in row for row in self.engine_journal))

    def test_new_session_picks_weighted_ready_instance(self) -> None:
        open_row = next(r for r in self.engine_journal if r["type"] == "open")
        self.assertEqual(open_row["response"]["instance"], "a")
        self.assertEqual(open_row["response"]["route_revision"], 1)

    def test_draining_does_not_move_session_and_unknown_blocks_next_turn(self) -> None:
        turn3_first = next(
            r
            for r in self.engine_journal
            if r["type"] == "request"
            and r["response"].get("turn") == 3
            and r["timestamp"] == 12
        )
        self.assertEqual(turn3_first["response"]["status"], "waiting")
        self.assertEqual(
            turn3_first["response"]["reason"], "previous_turn_open"
        )
        # t=12 排队时没有给 turn 3 发过任何 attempt；
        # 直到 t=22 前序轮次结算且迁移准入满足后才授权 attempt:3。
        self.assertEqual(
            self.session.turns[3].attempts[0].sent_at, 22
        )

    def test_cancel_pending_then_late_commit_wins(self) -> None:
        turn2 = self.session.turns[2]
        self.assertEqual(turn2.state, TurnState.COMMITTED)
        self.assertTrue(turn2.cancel_requested)
        self.assertEqual(turn2.result, "比较完成")
        self.assertEqual(turn2.checkpoint, "cp2")
        self.assertEqual(turn2.attempts[0].ref, "attempt:2")
        self.assertEqual(self.session.last_checkpoint, "cp2")

    def test_migration_after_down_requires_cp2_visibility(self) -> None:
        turn3 = self.session.turns[3]
        self.assertEqual(len(turn3.attempts), 1)
        attempt = turn3.attempts[0]
        self.assertEqual(attempt.ref, "attempt:3")
        self.assertEqual((attempt.instance_id, attempt.boot), ("b", 1))
        self.assertEqual(attempt.base_checkpoint, "cp2")
        self.assertEqual(attempt.base_format, "f2")
        self.assertEqual((self.session.pinned_instance, self.session.pinned_boot), ("b", 1))

    def test_inspect_locates_revision_wait_and_next_step(self) -> None:
        view = self.gateway.inspect("rounds")
        self.assertEqual(view["opened_revision"], 1)
        self.assertEqual(view["current_revision"], 2)
        self.assertEqual(view["waiting_on"]["attempt"], "attempt:3")
        self.assertEqual(view["waiting_on"]["instance"], "b")
        self.assertEqual(view["next_action"], "await_commit_confirmation")

    def test_attempt_numbering_is_global_sequence(self) -> None:
        refs = [
            a.ref
            for turn in self.session.turns.values()
            for a in turn.attempts
        ]
        self.assertEqual(refs, ["attempt:1", "attempt:2", "attempt:3"])


class FailoverDeadlineScenarioTests(unittest.TestCase):
    def setUp(self) -> None:
        self.gateway = Gateway()
        from gateway.replay import ReplayEngine

        ReplayEngine(self.gateway).run_path(DATA / "failover-deadline.jsonl")
        self.session = self.gateway.sessions["short"]

    def test_boot_change_keeps_old_confirmation_separate(self) -> None:
        view = self.gateway.inspect("short")
        self.assertFalse(view["pinned"]["identity_current"])
        self.assertEqual(view["pinned"]["instance"], "a")
        self.assertEqual(view["pinned"]["boot"], 1)

    def test_not_committed_proof_then_deadline_blocks_redispatch(self) -> None:
        turn = self.session.turns[1]
        self.assertEqual(turn.state, TurnState.QUEUED)
        self.assertEqual(len(turn.attempts), 1)
        self.assertEqual(turn.attempts[0].ref, "attempt:1")
        self.assertEqual(turn.wait_reason, "deadline_cannot_fit_minimum_runtime")
        self.assertEqual(self.gateway.attempt_counter, 1)

    def test_inspect_says_retry_pending(self) -> None:
        view = self.gateway.inspect("short")
        self.assertIsNone(view["waiting_on"])
        self.assertEqual(view["next_action"], "retry_pending_turn")


class RestartPersistenceTests(unittest.TestCase):
    def test_snapshot_restart_never_issues_duplicate_attempt(self) -> None:
        events_path = DATA / "in-doubt-while-draining.jsonl"
        from trace_io import read_trace
        from gateway.replay import EVENT_TYPES, ReplayEngine

        events = list(read_trace(events_path, EVENT_TYPES))
        base = Path("/tmp") / "gw-restart-test"
        gw_store = DurableStore(base.with_suffix(".gw.json"))
        backend_store = DurableStore(base.with_suffix(".backend.json"))
        for store_path in (gw_store.path, backend_store.path):
            if store_path.exists():
                store_path.unlink()

        backend = SyntheticBackend(store=backend_store)
        gateway = Gateway(store=gw_store, backend=backend)
        engine = ReplayEngine(gateway)
        for event in events[:11]:  # 截止到 cancel，attempt:2 仍在途
            engine.handle(event)
        self.assertEqual(gateway.attempt_counter, 2)

        # 全新进程：仅从各自快照恢复。
        backend2 = SyntheticBackend(store=DurableStore(backend_store.path))
        restored = Gateway(
            store=DurableStore(gw_store.path), backend=backend2
        )
        engine2 = ReplayEngine(restored)
        for event in events[11:]:
            engine2.handle(event)

        self.assertEqual(restored.attempt_counter, 3)
        turn2 = restored.sessions["rounds"].turns[2]
        self.assertEqual(turn2.state, TurnState.COMMITTED)
        self.assertEqual(
            [a.ref for a in turn2.attempts], ["attempt:2"]
        )  # 没有为旧轮次另发新调用

    def test_reconcile_resolves_in_flight_from_backend_query(self) -> None:
        from tests.factories import build_topology, make_instance, make_route

        # 模拟网关重启：后端独立存活并持久化到自己的快照文件，
        # 新网关靠提交查询消解而非重发。
        base = Path("/tmp") / "gw-reconcile-test"
        gw_store = DurableStore(base.with_suffix(".gw.json"))
        backend_store = DurableStore(base.with_suffix(".backend.json"))
        for store_path in (gw_store.path, backend_store.path):
            if store_path.exists():
                store_path.unlink()
        live = Gateway(store=gw_store, backend=SyntheticBackend(store=backend_store))
        build_topology(live, [make_instance("a")], [make_route()])
        live.open_session("plant", "s9", 0)
        live.submit("s9", 1, "t1", "body", deadline=20, now=1)
        attempt_ref = live.sessions["s9"].turns[1].attempts[0].ref
        live.backend.report(attempt_ref, "a", 1, "committed", "r", "cp9")

        revived_backend = SyntheticBackend(
            store=DurableStore(backend_store.path)
        )
        revived = Gateway(
            store=DurableStore(gw_store.path), backend=revived_backend
        )
        applied = revived.reconcile()
        self.assertEqual(len(applied), 1)
        self.assertEqual(applied[0]["status"], "committed")
        self.assertEqual(revived.attempt_counter, 1)
        self.assertEqual(revived.sessions["s9"].last_checkpoint, "cp9")

    def test_reconcile_keeps_unknown_in_flight(self) -> None:
        gateway = Gateway()
        from tests.factories import build_topology, make_instance, make_route

        build_topology(gateway, [make_instance("a")], [make_route()])
        gateway.open_session("plant", "s10", 0)
        gateway.submit("s10", 1, "t1", "body", deadline=20, now=1)
        applied = gateway.reconcile()
        self.assertEqual(applied, [])
        self.assertEqual(
            gateway.sessions["s10"].turns[1].state, TurnState.IN_FLIGHT
        )


if __name__ == "__main__":
    unittest.main()
