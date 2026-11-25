"""补充测试：加权选路、合成后端查询与检查视图细节。"""

from __future__ import annotations

import random
import unittest

from gateway.models import TurnState
from gateway.service import Gateway
from gateway.synthetic import SyntheticBackend
from tests.factories import build_topology, make_instance, make_route


class WeightedSelectionTests(unittest.TestCase):
    def test_weights_guide_new_session_distribution(self) -> None:
        gw = Gateway()
        build_topology(
            gw,
            [
                make_instance("a"),
                make_instance("b", fmt="f3", reads=("f2", "f3")),
            ],
            [make_route(weights={"a": 80, "b": 20})],
        )
        picks = [
            gw.open_session("plant", f"s{i}", 0)["instance"] for i in range(200)
        ]
        b_share = picks.count("b") / len(picks)
        # 80/20 权重下 b 的份额应落在 20% 附近，且两个实例都会被选中。
        self.assertGreater(b_share, 0.05)
        self.assertLess(b_share, 0.45)
        self.assertEqual(set(picks), {"a", "b"})

    def test_equal_weights_use_injected_randomness(self) -> None:
        gw = Gateway()
        build_topology(
            gw,
            [
                make_instance("a"),
                make_instance("b", fmt="f3", reads=("f2", "f3")),
            ],
            [make_route(weights={"a": 50, "b": 50})],
        )
        gw.selector._random = random.Random(1234)
        picks = [
            gw.open_session("plant", f"s{i}", 0)["instance"] for i in range(8)
        ]
        self.assertEqual(set(picks), {"a", "b"})

    def test_region_allow_list_governs_migration_targets(self) -> None:
        gw = Gateway()
        build_topology(
            gw,
            [
                make_instance("a", region="local"),
                make_instance("b", region="local", fmt="f3", reads=("f2", "f3")),
                make_instance("c", region="remote", fmt="f3", reads=("f2", "f3")),
            ],
            [make_route(weights={"a": 100, "b": 0, "c": 0})],
        )
        gw.open_session("plant", "s1", 0)
        gw.submit("s1", 1, "t1", "body", deadline=20, now=1)
        gw.record_result("attempt:1", "a", 1, "committed", "r1", "cp1", now=2)
        gw.apply_instance(make_instance("a", region="local", state="down"))
        gw.apply_route(
            make_route(
                revision=2,
                allow_regions=("local",),
                weights={"a": 0, "b": 100, "c": 100},
            )
        )
        # c 先可见也不会被选：地域不允许。
        gw.mark_checkpoint_visible("c", 1, "s1", "cp1")
        gw.mark_checkpoint_visible("b", 1, "s1", "cp1")
        response = gw.submit("s1", 2, "t2", "body", deadline=30, now=3)
        self.assertEqual(response["instance"], "b")

    def test_first_turn_without_history_needs_no_visibility(self) -> None:
        gw = Gateway()
        build_topology(
            gw,
            [make_instance("a"), make_instance("b", fmt="f3", reads=("f2", "f3"))],
            [make_route(weights={"a": 0, "b": 100})],
        )
        gw.open_session("plant", "fresh", 0)
        # 没有任何检查点历史时，无需 checkpoint_visible 即可派发首轮。
        response = gw.submit("fresh", 1, "t1", "body", deadline=20, now=1)
        self.assertEqual(response["status"], "dispatched")
        self.assertIsNone(response["base_checkpoint"])


class SyntheticBackendTests(unittest.TestCase):
    def test_query_unknown_distinguishes_silence_from_not_committed(self) -> None:
        backend = SyntheticBackend()
        backend.deliver("attempt:9", "a", 1)
        # 无响应：查询得到 unknown，而不是 not_committed。
        self.assertEqual(backend.query("attempt:9")["state"], "unknown")
        backend.report("attempt:9", "a", 1, "unknown")
        self.assertEqual(backend.query("attempt:9")["state"], "unknown")
        # unknown 之后迟到提交。
        backend.report("attempt:9", "a", 1, "committed", "r", "cp")
        fact = backend.query("attempt:9")
        self.assertEqual(fact["state"], "committed")
        self.assertEqual(fact["checkpoint"], "cp")

    def test_report_rejects_unknown_attempt_and_wrong_identity(self) -> None:
        backend = SyntheticBackend()
        with self.assertRaises(KeyError):
            backend.report("attempt:x", "a", 1, "not_committed")
        backend.deliver("attempt:1", "a", 1)
        with self.assertRaises(ValueError):
            backend.report("attempt:1", "a", 2, "not_committed")

    def test_committed_requires_payload(self) -> None:
        backend = SyntheticBackend()
        backend.deliver("attempt:1", "a", 1)
        with self.assertRaises(ValueError):
            backend.report("attempt:1", "a", 1, "committed")


class InspectViewTests(unittest.TestCase):
    def test_settled_session_offers_next_turn(self) -> None:
        gw = Gateway()
        build_topology(gw, [make_instance("a")], [make_route()])
        gw.open_session("plant", "s1", 0)
        gw.submit("s1", 1, "t1", "body", deadline=20, now=1)
        gw.record_result("attempt:1", "a", 1, "committed", "r", "cp1", now=2)
        view = gw.inspect("s1")
        self.assertIsNone(view["waiting_on"])
        self.assertEqual(view["next_action"], "accept_next_turn")
        self.assertEqual(view["last_checkpoint"], "cp1")
        self.assertEqual(view["turns"][0]["state"], TurnState.COMMITTED.value)

    def test_unknown_session_and_unknown_request(self) -> None:
        gw = Gateway()
        self.assertEqual(gw.inspect("missing")["status"], "unknown_session")
        self.assertEqual(
            gw.request_cancel("missing", "t1", 0)["status"], "unknown_session"
        )
        build_topology(gw, [make_instance("a")], [make_route()])
        gw.open_session("plant", "s1", 0)
        self.assertEqual(
            gw.request_cancel("s1", "nope", 0)["status"], "unknown_request"
        )


if __name__ == "__main__":
    unittest.main()
