"""测试用的实例与路由工厂。"""

from __future__ import annotations

from gateway.models import Instance, InstanceState, Route


def make_instance(
    instance_id: str = "a",
    *,
    boot: int = 1,
    region: str = "local",
    model: str = "s2",
    fmt: str = "f2",
    reads: tuple[str, ...] | None = None,
    minimum_runtime: int = 3,
    state: str = "ready",
) -> Instance:
    return Instance(
        id=instance_id,
        boot=boot,
        region=region,
        model=model,
        format=fmt,
        reads=reads if reads is not None else (fmt,),
        minimum_runtime=minimum_runtime,
        state=InstanceState(state),
    )


def make_route(
    tenant: str = "plant",
    *,
    revision: int = 1,
    allow_regions: tuple[str, ...] = ("local",),
    weights: dict[str, int] | None = None,
) -> Route:
    return Route(
        tenant=tenant,
        revision=revision,
        allow_regions=allow_regions,
        weights=weights if weights is not None else {"a": 100},
    )


def build_topology(gateway, instances, routes) -> None:
    """批量登记实例与路由。"""

    for instance in instances:
        gateway.apply_instance(instance)
    for route in routes:
        gateway.apply_route(route)
