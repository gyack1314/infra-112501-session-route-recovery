"""单轮次推进边界：在途重试、未知消解、顺序与取消。"""

from __future__ import annotations

import unittest

from gateway.models import (
    WAIT_DEADLINE,
    WAIT_IN_DOUBT,
    WAIT_TURN_ORDER,
    TurnState,
)
from gateway.service import Gateway
from tests.factories import build_topology, make_instance, make_route


class TurnSemanticsTests(unittest.TestCase):
    def setUp(self) -> None:
        self.gw = Gateway()
        build_topology(
            self.gw,
            [
                make_instance("a", fmt="f2", reads=("f2",)),
                make_instance("b", fmt="f3", reads=("f2", "f3")),
            ],
            [make_route(weights={"a": 100, "b": 0})],
        )
        self.gw.open_session("plant", "s1", 0)

    def _dispatch_turn_one(self) -> dict:
        return self.gw.submit("s1", 1, "t1", "body", deadline=20, now=1)

    def test_retry_while_in_flight_returns_same_attempt(self) -> None:
        first = self._dispatch_turn_one()
        self.assertEqual(first["attempt"], "attempt:1")
        retry = self.gw.submit("s1", 1, "t1", "body", deadline=20, now=2)
        self.assertEqual(retry["status"], "in_flight")
        self.assertEqual(retry["attempt"], "attempt:1")
        self.assertEqual(self.gw.attempt_counter, 1)

    def test_unknown_is_not_evidence_of_non_execution(self) -> None:
        self._dispatch_turn_one()
        doubt = self.gw.record_result("attempt:1", "a", 1, "unknown", now=2)
        self.assertEqual(doubt["status"], "in_doubt")
        # 重试仍指向同一个 attempt，不允许改发。
        retry = self.gw.submit("s1", 1, "t1", "body", deadline=20, now=3)
        self.assertEqual(retry["status"], "in_flight")
        self.assertEqual(retry["attempt"], "attempt:1")
        self.assertEqual(retry["reason"], WAIT_IN_DOUBT)
        self.assertEqual(self.gw.attempt_counter, 1)

    def test_late_commit_after_unknown_is_kept(self) -> None:
        self._dispatch_turn_one()
        self.gw.record_result("attempt:1", "a", 1, "unknown", now=2)
        committed = self.gw.record_result(
            "attempt:1", "a", 1, "committed", "结果", "cp1", now=4
        )
        self.assertEqual(committed["status"], "committed")
        turn = self.gw.sessions["s1"].turns[1]
        self.assertEqual(turn.state, TurnState.COMMITTED)
        # 结果与检查点来自同一执行。
        self.assertEqual(turn.result, "结果")
        self.assertEqual(turn.checkpoint, "cp1")
        self.assertEqual(self.gw.sessions["s1"].last_checkpoint, "cp1")

    def test_not_committed_allows_one_new_attempt_only(self) -> None:
        self._dispatch_turn_one()
        outcome = self.gw.record_result(
            "attempt:1", "a", 1, "not_committed", now=2
        )
        self.assertEqual(outcome["status"], "not_committed")
        resent = self.gw.submit("s1", 1, "t1", "body", deadline=20, now=3)
        self.assertEqual(resent["status"], "dispatched")
        self.assertEqual(resent["attempt"], "attempt:2")
        # 再次重试时新 attempt 在途，复用 attempt:2 而非再发。
        retry = self.gw.submit("s1", 1, "t1", "body", deadline=20, now=4)
        self.assertEqual(retry["attempt"], "attempt:2")
        self.assertEqual(self.gw.attempt_counter, 2)

    def test_parallel_turns_cannot_overtake(self) -> None:
        self._dispatch_turn_one()
        # turn 2 抢先到达：排队等待，不产生 attempt。
        blocked = self.gw.submit("s1", 2, "t2", "body", deadline=30, now=2)
        self.assertEqual(blocked["status"], "waiting")
        self.assertEqual(blocked["reason"], WAIT_TURN_ORDER)
        self.assertEqual(self.gw.attempt_counter, 1)

        self.gw.record_result("attempt:1", "a", 1, "committed", "r1", "cp1", now=3)
        # 原 turn 2 请求重试后才派发，且建立在 cp1 之上。
        dispatched = self.gw.submit("s1", 2, "t2", "body", deadline=30, now=4)
        self.assertEqual(dispatched["status"], "dispatched")
        self.assertEqual(dispatched["attempt"], "attempt:2")
        self.assertEqual(dispatched["base_checkpoint"], "cp1")

    def test_gap_turn_number_is_rejected(self) -> None:
        response = self.gw.submit("s1", 3, "t3", "body", deadline=30, now=1)
        self.assertEqual(response["reason"], WAIT_TURN_ORDER)
        self.assertNotIn(3, self.gw.sessions["s1"].turns)

    def test_cancel_queued_turn_never_dispatches(self) -> None:
        # 先占住 turn 1，让 turn 2 排队。
        self._dispatch_turn_one()
        self.gw.submit("s1", 2, "t2", "body", deadline=30, now=2)
        cancel = self.gw.request_cancel("s1", "t2", now=3)
        self.assertEqual(cancel["status"], "cancelled")
        self.gw.record_result("attempt:1", "a", 1, "committed", "r1", "cp1", now=4)
        # 结算前一轮后，turn 2 已取消，重试不再派发。
        retry = self.gw.submit("s1", 2, "t2", "body", deadline=30, now=5)
        self.assertEqual(retry["status"], "waiting")
        self.assertEqual(self.gw.attempt_counter, 1)

    def test_cancel_in_flight_is_pending_until_confirmation(self) -> None:
        self._dispatch_turn_one()
        cancel = self.gw.request_cancel("s1", "t1", now=2)
        self.assertEqual(cancel["status"], "cancel_pending")
        self.gw.record_result("attempt:1", "a", 1, "unknown", now=3)
        pending = self.gw.inspect("s1")["turns"][0]
        self.assertEqual(pending["state"], TurnState.CANCEL_PENDING.value)

        # 迟到提交优先：已完成的轮次可查，取消不能吞掉结果。
        committed = self.gw.record_result(
            "attempt:1", "a", 1, "committed", "最终结果", "cp1", now=5
        )
        self.assertEqual(committed["status"], "committed")
        turn = self.gw.sessions["s1"].turns[1]
        self.assertEqual(turn.state, TurnState.COMMITTED)
        self.assertTrue(turn.cancel_requested)

    def test_cancel_in_flight_settles_when_not_committed(self) -> None:
        self._dispatch_turn_one()
        self.gw.request_cancel("s1", "t1", now=2)
        outcome = self.gw.record_result(
            "attempt:1", "a", 1, "not_committed", now=3
        )
        self.assertEqual(outcome["status"], "not_committed")
        self.assertEqual(
            self.gw.sessions["s1"].turns[1].state, TurnState.CANCELLED
        )

    def test_duplicate_committed_facts_do_not_rewrite_history(self) -> None:
        self._dispatch_turn_one()
        self.gw.record_result(
            "attempt:1", "a", 1, "committed", "第一版", "cp1", now=2
        )
        duplicate = self.gw.record_result(
            "attempt:1", "a", 1, "committed", "伪造版", "cpX", now=3
        )
        self.assertEqual(duplicate["status"], "duplicate_ignored")
        turn = self.gw.sessions["s1"].turns[1]
        self.assertEqual(turn.result, "第一版")
        self.assertEqual(turn.checkpoint, "cp1")

    def test_wrong_identity_cannot_confirm_old_attempt(self) -> None:
        self._dispatch_turn_one()
        # a 重启为 boot 2 后，不能替 boot 1 的 attempt 作证。
        self.gw.apply_instance(make_instance("a", boot=2, state="down"))
        rejected = self.gw.record_result(
            "attempt:1", "a", 2, "committed", "r", "cp1", now=3
        )
        self.assertEqual(rejected["status"], "rejected")
        self.assertEqual(rejected["reason"], "identity_mismatch")
        # 旧 boot 的事实仍可正常结算。
        ok = self.gw.record_result(
            "attempt:1", "a", 1, "not_committed", now=4
        )
        self.assertEqual(ok["status"], "not_committed")

    def test_abandon_unblocks_turn_for_redispatch(self) -> None:
        self._dispatch_turn_one()
        abandoned = self.gw.abandon_attempt("attempt:1")
        self.assertEqual(abandoned["status"], "abandoned")
        resent = self.gw.submit("s1", 1, "t1", "body", deadline=20, now=3)
        self.assertEqual(resent["attempt"], "attempt:2")


class DeadlineTests(unittest.TestCase):
    def setUp(self) -> None:
        self.gw = Gateway()
        build_topology(
            self.gw,
            [
                make_instance("a", minimum_runtime=2),
                make_instance(
                    "b",
                    fmt="f3",
                    reads=("f2", "f3"),
                    minimum_runtime=6,
                ),
            ],
            [make_route(weights={"a": 100, "b": 0})],
        )
        self.gw.open_session("plant", "short", 0)

    def test_minimum_runtime_gate_blocks_new_dispatch(self) -> None:
        # t=3 派发 a，随后 a 换 boot 下线且证明未提交。
        self.gw.submit("short", 1, "t1", "生成", deadline=10, now=3)
        self.gw.apply_instance(make_instance("a", boot=2, state="down"))
        self.gw.apply_route(
            make_route(revision=2, weights={"a": 0, "b": 100})
        )
        self.gw.record_result("attempt:1", "a", 1, "not_committed", now=6)
        # 7 + 6 > 10：过期可以停止发送，不产生新 attempt。
        response = self.gw.submit("short", 1, "t1", "生成", deadline=10, now=7)
        self.assertEqual(response["status"], "waiting")
        self.assertEqual(response["reason"], WAIT_DEADLINE)
        self.assertEqual(self.gw.attempt_counter, 1)

    def test_deadline_expiry_keeps_observed_commit(self) -> None:
        self.gw.submit("short", 1, "t1", "生成", deadline=10, now=3)
        committed = self.gw.record_result(
            "attempt:1", "a", 1, "committed", "已生成", "cp1", now=9
        )
        self.assertEqual(committed["status"], "committed")
        view = self.gw.inspect("short")
        self.assertEqual(view["last_checkpoint"], "cp1")
        self.assertEqual(view["turns"][0]["state"], TurnState.COMMITTED.value)

    def test_fitting_deadline_dispatches_after_migration(self) -> None:
        self.gw.submit("short", 1, "t1", "生成", deadline=20, now=3)
        self.gw.record_result(
            "attempt:1", "a", 1, "committed", "r", "cp1", now=4
        )
        self.gw.apply_instance(make_instance("a", state="down"))
        self.gw.apply_route(
            make_route(revision=2, weights={"a": 0, "b": 100})
        )
        self.gw.mark_checkpoint_visible("b", 1, "short", "cp1")
        # 5 + 6 <= 20，可以迁到 b。
        response = self.gw.submit("short", 2, "t2", "继续", deadline=20, now=5)
        self.assertEqual(response["status"], "dispatched")
        self.assertEqual(response["instance"], "b")


if __name__ == "__main__":
    unittest.main()
