"""路由、版本粘滞与迁移准入测试。"""

from __future__ import annotations

import unittest

from gateway.models import (
    WAIT_DRAIN_MIGRATION,
    WAIT_NO_CAPACITY,
    WAIT_PINNED_DOWN,
)
from gateway.service import Gateway
from tests.factories import build_topology, make_instance, make_route


class RoutingTests(unittest.TestCase):
    def setUp(self) -> None:
        self.gw = Gateway()

    def test_new_session_ignores_draining_and_zero_weight(self) -> None:
        build_topology(
            self.gw,
            [
                make_instance("a", state="draining"),
                make_instance("b", fmt="f3", reads=("f2", "f3")),
            ],
            [make_route(weights={"a": 100, "b": 100})],
        )
        # a 在排空且权重为正也不能接新会话；b 同权被选中。
        response = self.gw.open_session("plant", "s1", 0)
        self.assertEqual(response["status"], "opened")
        self.assertEqual(response["instance"], "b")

    def test_zero_weight_instance_never_picked_for_new_session(self) -> None:
        build_topology(
            self.gw,
            [make_instance("a"), make_instance("b", fmt="f3", reads=("f2", "f3"))],
            [make_route(weights={"a": 100, "b": 0})],
        )
        response = self.gw.open_session("plant", "s1", 0)
        self.assertEqual(response["instance"], "a")

    def test_no_capacity_returns_explicit_wait_not_empty_session(self) -> None:
        build_topology(
            self.gw,
            [make_instance("a", state="down")],
            [make_route(weights={"a": 100})],
        )
        response = self.gw.open_session("plant", "s1", 0)
        self.assertEqual(response["status"], "waiting")
        self.assertEqual(response["reason"], WAIT_NO_CAPACITY)
        self.assertNotIn("s1", self.gw.sessions)  # 没有偷偷建空会话

    def test_region_restriction_blocks_selection(self) -> None:
        build_topology(
            self.gw,
            [make_instance("a", region="remote")],
            [make_route(allow_regions=("local",), weights={"a": 100})],
        )
        response = self.gw.open_session("plant", "s1", 0)
        self.assertEqual(response["reason"], WAIT_NO_CAPACITY)

    def test_draining_keeps_existing_session_sticky(self) -> None:
        build_topology(
            self.gw,
            [make_instance("a"), make_instance("b", fmt="f3", reads=("f2", "f3"))],
            [make_route(weights={"a": 100, "b": 0})],
        )
        self.gw.open_session("plant", "s1", 0)
        # a 进入排空，路由权重调零；已存在会话仍粘滞到 a。
        self.gw.apply_instance(make_instance("a", state="draining"))
        self.gw.apply_route(make_route(revision=2, weights={"a": 0, "b": 100}))
        response = self.gw.submit("s1", 1, "t1", "body", deadline=20, now=5)
        self.assertEqual(response["status"], "dispatched")
        self.assertEqual(response["instance"], "a")

    def test_migration_requires_checkpoint_visibility_and_format_and_region(self) -> None:
        build_topology(
            self.gw,
            [
                make_instance("a", fmt="f2", reads=("f2",)),
                make_instance("b", fmt="f3", reads=("f2", "f3")),
                make_instance("c", fmt="f4", reads=("f4",), region="remote"),
            ],
            [make_route(weights={"a": 100, "b": 0, "c": 0})],
        )
        self.gw.open_session("plant", "s1", 0)
        self.gw.submit("s1", 1, "t1", "body", deadline=20, now=1)
        self.gw.record_result("attempt:1", "a", 1, "committed", "r1", "cp1", now=2)

        # a 停用；b 还见不到 cp1，c 格式不兼容且地域不允许。
        self.gw.apply_instance(make_instance("a", state="down"))
        self.gw.apply_route(
            make_route(
                revision=2,
                allow_regions=("local",),
                weights={"a": 0, "b": 100, "c": 100},
            )
        )
        waiting = self.gw.submit("s1", 2, "t2", "body", deadline=30, now=3)
        self.assertEqual(waiting["status"], "waiting")
        self.assertEqual(waiting["reason"], WAIT_PINNED_DOWN)

        # 仅 c 可见不够：地域与格式双重不兼容。
        self.gw.mark_checkpoint_visible("c", 1, "s1", "cp1")
        waiting = self.gw.submit("s1", 2, "t2", "body", deadline=30, now=4)
        self.assertEqual(waiting["reason"], WAIT_PINNED_DOWN)

        # b 证明可见且能读 f2、地域允许后才能迁移。
        self.gw.mark_checkpoint_visible("b", 1, "s1", "cp1")
        dispatched = self.gw.submit("s1", 2, "t2", "body", deadline=30, now=5)
        self.assertEqual(dispatched["status"], "dispatched")
        self.assertEqual(dispatched["instance"], "b")
        self.assertEqual(dispatched["base_checkpoint"], "cp1")
        view = self.gw.inspect("s1")
        self.assertEqual(view["pinned"]["instance"], "b")

    def test_boot_change_splits_identity_and_requires_visibility(self) -> None:
        build_topology(
            self.gw,
            [make_instance("a"), make_instance("b", fmt="f3", reads=("f2", "f3"))],
            [make_route(weights={"a": 100, "b": 0})],
        )
        self.gw.open_session("plant", "s1", 0)
        self.gw.submit("s1", 1, "t1", "body", deadline=20, now=1)
        self.gw.record_result("attempt:1", "a", 1, "committed", "r1", "cp1", now=2)

        # a 重启换 boot：旧身份的确认与新地址接收能力分别判断。
        self.gw.apply_instance(make_instance("a", boot=2, state="ready"))
        self.gw.apply_route(make_route(revision=2, weights={"a": 100, "b": 0}))
        view_before = self.gw.inspect("s1")
        self.assertFalse(view_before["pinned"]["identity_current"])

        waiting = self.gw.submit("s1", 2, "t2", "body", deadline=30, now=3)
        self.assertEqual(waiting["reason"], WAIT_DRAIN_MIGRATION)

        # 新 boot 必须重新证明能看到 cp1，旧 boot 的可见性不作数。
        self.gw.mark_checkpoint_visible("a", 1, "s1", "cp1")
        waiting = self.gw.submit("s1", 2, "t2", "body", deadline=30, now=4)
        self.assertEqual(waiting["reason"], WAIT_DRAIN_MIGRATION)
        self.gw.mark_checkpoint_visible("a", 2, "s1", "cp1")
        dispatched = self.gw.submit("s1", 2, "t2", "body", deadline=30, now=5)
        self.assertEqual(dispatched["status"], "dispatched")
        self.assertEqual((dispatched["instance"], dispatched["boot"]), ("a", 2))

    def test_health_jitter_only_changes_future_routing(self) -> None:
        build_topology(self.gw, [make_instance("a")], [make_route()])
        self.gw.open_session("plant", "s1", 0)
        self.gw.submit("s1", 1, "t1", "body", deadline=20, now=1)
        # 在途期间实例短暂抖动再恢复，attempt 身份不变、确认照常结算。
        self.gw.apply_instance(make_instance("a", state="draining"))
        self.gw.apply_instance(make_instance("a", state="ready"))
        settled = self.gw.record_result(
            "attempt:1", "a", 1, "committed", "r1", "cp1", now=2
        )
        self.assertEqual(settled["status"], "committed")


if __name__ == "__main__":
    unittest.main()
