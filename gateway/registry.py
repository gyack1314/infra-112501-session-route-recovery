"""部署事实注册表：实例身份、路由代次与检查点可见性。

部署清单通过 ``instance`` 事件给出实例状态格式、可读检查点（reads）
及地域；``route`` 事件发布租户的路由代次、地域允许列表与权重；
``checkpoint_visible`` 事件证明某个具体 boot 的实例确实能读到
某次会话的某个检查点。
"""

from __future__ import annotations

from .models import Instance, InstanceState, Route


class Registry:
    def __init__(self) -> None:
        self._instances: dict[str, Instance] = {}
        self._routes: dict[str, Route] = {}
        # (instance_id, boot, session_id, checkpoint) -> True
        self._visible: set[tuple[str, int, str, str]] = set()

    # ---- 部署事实录入 -------------------------------------------------

    def upsert_instance(self, instance: Instance) -> None:
        """登记或刷新实例。

        健康抖动（ready/draining 间切换、权重变化）只影响后续选路；
        boot 变化意味着重启后的新身份，旧 boot 的在途确认仍按旧身份判断。
        """

        self._instances[instance.id] = instance

    def publish_route(self, route: Route) -> None:
        published = self._routes.get(route.tenant)
        if published is not None and route.revision < published.revision:
            raise ValueError(
                f"租户 {route.tenant} 路由代次不能回退："
                f"{route.revision} < {published.revision}"
            )
        self._routes[route.tenant] = route

    def mark_checkpoint_visible(
        self, instance_id: str, boot: int, session_id: str, checkpoint: str
    ) -> None:
        self._visible.add((instance_id, boot, session_id, checkpoint))

    # ---- 查询 ---------------------------------------------------------

    def instance(self, instance_id: str) -> Instance | None:
        return self._instances.get(instance_id)

    def route(self, tenant: str) -> Route | None:
        return self._routes.get(tenant)

    def instances(self) -> list[Instance]:
        return list(self._instances.values())

    def sees_checkpoint(
        self, instance_id: str, boot: int, session_id: str, checkpoint: str
    ) -> bool:
        return (instance_id, boot, session_id, checkpoint) in self._visible

    def identity(self, instance_id: str) -> tuple[str, int] | None:
        instance = self._instances.get(instance_id)
        if instance is None:
            return None
        return instance.id, instance.boot

    def is_state(self, instance_id: str, state: InstanceState) -> bool:
        instance = self._instances.get(instance_id)
        return instance is not None and instance.state is state

    # ---- 持久化 -------------------------------------------------------

    def to_dict(self) -> dict:
        return {
            "instances": [i.to_dict() for i in self._instances.values()],
            "routes": [r.to_dict() for r in self._routes.values()],
            "visible": [list(item) for item in sorted(self._visible)],
        }

    def load_dict(self, raw: dict) -> None:
        self._instances = {}
        self._routes = {}
        self._visible = set()
        for item in raw.get("instances", []):
            instance = Instance.from_dict(item)
            self._instances[instance.id] = instance
        for item in raw.get("routes", []):
            route = Route.from_dict(item)
            self._routes[route.tenant] = route
        for item in raw.get("visible", []):
            self._visible.add((item[0], item[1], item[2], item[3]))
