"""选路与迁移准入。

规则汇总：

- 新会话只在当前路由权重为正、状态 ready、地域允许的实例中选择，
  已经进入 draining 的实例不能再接新会话；
- 已存在会话在 draining 期间粘滞到原实例（即使权重已调零）；
- 实例 down 或重启换身份后才需要迁移。备用实例必须同时满足：
  地域允许、可读取会话当前检查点的格式（reads）、且部署代理已证明
  该 boot 确实能看到会话最后已提交的检查点；
- 健康抖动与权重变化只改变后续选路，不影响在途 attempt 的确认。
"""

from __future__ import annotations

import random as random_module

from .models import Instance, InstanceState, Route, Session, WAIT_DRAIN_MIGRATION, WAIT_NO_CAPACITY, WAIT_PINNED_DOWN
from .registry import Registry


class Selector:
    def __init__(self, registry: Registry, random: random_module.Random | None = None) -> None:
        self.registry = registry
        self._random = random or random_module.Random(0)

    def _route_candidates(self, route: Route) -> list[Instance]:
        """当前路由下可承接新会话的候选：权重为正、ready、地域允许。"""

        candidates: list[Instance] = []
        for instance_id, weight in route.weights.items():
            if weight <= 0:
                continue
            instance = self.registry.instance(instance_id)
            if instance is None or instance.state is not InstanceState.READY:
                continue
            if instance.region not in route.allow_regions:
                continue
            candidates.append(instance)
        return candidates

    def choose_for_new_session(self, tenant: str) -> tuple[Instance | None, str | None]:
        route = self.registry.route(tenant)
        if route is None:
            return None, WAIT_NO_CAPACITY
        candidates = self._route_candidates(route)
        if not candidates:
            return None, WAIT_NO_CAPACITY
        return self._weighted_pick(route, candidates), None

    def choose_for_turn(
        self, session: Session
    ) -> tuple[Instance | None, str | None]:
        """为已存在会话的下一次派发选择目标。

        返回 (实例, None) 或 (None, 等待理由)。
        """

        route = self.registry.route(session.tenant)
        if route is None:
            return None, WAIT_NO_CAPACITY

        pinned_id = session.pinned_instance
        if pinned_id is not None:
            pinned = self.registry.instance(pinned_id)
            if pinned is not None and pinned.boot == session.pinned_boot:
                # 同身份粘滞：ready/draining 都可继续承接本会话，
                # 路由权重调零不影响已存在会话；地域限制不再重复约束
                # （会话创建时已校验，且迁移只允许进入允许地域）。
                if pinned.state is not InstanceState.DOWN:
                    return pinned, None
                # 原实例已停用：必须迁往其他实例。
                reason = WAIT_PINNED_DOWN
            else:
                # 重启导致身份变化：旧地址不再具备接收能力，按迁移处理，
                # 历史确认仍由旧 boot 的 attempt 独立结算。
                reason = WAIT_DRAIN_MIGRATION
        else:
            reason = WAIT_NO_CAPACITY

        target = self._choose_migration_target(session, route)
        if target is None:
            return None, reason
        return target, None

    def _choose_migration_target(self, session: Session, route: Route) -> Instance | None:
        eligible: list[Instance] = []
        for instance in self._route_candidates(route):
            # 同 id 新 boot 也可能成为候选，但必须通过下面的准入检查。
            if not self._accepts_session_history(instance, session):
                continue
            eligible.append(instance)
        if not eligible:
            return None
        return self._weighted_pick(route, eligible)

    def _accepts_session_history(self, instance: Instance, session: Session) -> bool:
        """备用实例准入：格式兼容 + 检查点确实可见 + 地域允许。"""

        if instance.region not in self.registry.route(session.tenant).allow_regions:
            return False
        if session.last_checkpoint is None:
            # 尚无已提交检查点：无需历史可见性证明，格式兼容性无从谈起。
            return True
        if session.last_format is not None and session.last_format not in instance.reads:
            return False
        return self.registry.sees_checkpoint(
            instance.id, instance.boot, session.session_id, session.last_checkpoint
        )

    def _weighted_pick(self, route: Route, candidates: list[Instance]) -> Instance:
        weights = [route.weights[i.id] for i in candidates]
        if len(set(weights)) == 1:
            # 同权候选无需固定算法；测试中用注入的随机源保持可复现。
            return self._random.choice(candidates)
        return self._random.choices(candidates, weights=weights, k=1)[0]
