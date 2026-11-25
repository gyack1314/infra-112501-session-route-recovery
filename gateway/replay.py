"""事件回放引擎。

把 JSONL 演练逐事件喂给 :class:`~gateway.service.Gateway` 与
:class:`~gateway.synthetic.SyntheticBackend`，产出带时间戳的运行日志。
引擎本身不做业务判断：所有选路、在途确认与截止时间逻辑都在网关内。
"""

from __future__ import annotations

from pathlib import Path

from trace_io import read_trace

from .models import Instance, InstanceState, Route
from .service import Gateway

EVENT_TYPES = (
    "cancel",
    "checkpoint_visible",
    "instance",
    "open",
    "request",
    "result",
    "route",
    "inspect",
)


def parse_instance(data: dict) -> Instance:
    return Instance(
        id=data["id"],
        boot=data["boot"],
        region=data["region"],
        model=data["model"],
        format=data["format"],
        reads=tuple(data["reads"]),
        minimum_runtime=data["minimum_runtime"],
        state=InstanceState(data["state"]),
    )


def parse_route(data: dict) -> Route:
    return Route(
        tenant=data["tenant"],
        revision=data["revision"],
        allow_regions=tuple(data["allow_regions"]),
        weights=dict(data["weights"]),
    )


class ReplayEngine:
    def __init__(self, gateway: Gateway | None = None) -> None:
        self.gateway = gateway or Gateway()
        self.backend = self.gateway.backend
        self.journal: list[dict] = []

    def handle(self, event: dict) -> dict:
        timestamp = event["timestamp"]
        event_type = event["type"]
        data = event["data"]
        gateway = self.gateway

        if event_type == "instance":
            response = gateway.apply_instance(parse_instance(data))
        elif event_type == "route":
            response = gateway.apply_route(parse_route(data))
        elif event_type == "open":
            response = gateway.open_session(data["tenant"], data["session"], timestamp)
        elif event_type == "request":
            response = gateway.submit(
                session_id=data["session"],
                turn_no=data["turn"],
                request_id=data["request"],
                body=data["body"],
                deadline=data["deadline"],
                now=timestamp,
            )
        elif event_type == "checkpoint_visible":
            response = gateway.mark_checkpoint_visible(
                data["id"], data["boot"], data["session"], data["checkpoint"]
            )
        elif event_type == "result":
            # 合成后端先收到提交事实，网关再据此结算。
            self.backend.report(
                attempt_ref=data["attempt"],
                instance_id=data["id"],
                boot=data["boot"],
                state=data["state"],
                result=data.get("result"),
                checkpoint=data.get("checkpoint"),
            )
            response = gateway.record_result(
                attempt_ref=data["attempt"],
                instance_id=data["id"],
                boot=data["boot"],
                state=data["state"],
                result=data.get("result"),
                checkpoint=data.get("checkpoint"),
                now=timestamp,
            )
        elif event_type == "cancel":
            response = gateway.request_cancel(data["session"], data["request"], timestamp)
        elif event_type == "inspect":
            response = gateway.inspect(data["session"])
        else:  # 封装校验已在 read_trace 完成，这里只是防御。
            raise ValueError(f"未知事件类型：{event_type}")

        record = {"timestamp": timestamp, "type": event_type, "response": response}
        self.journal.append(record)
        return record

    def run_path(self, path: str | Path) -> list[dict]:
        for event in read_trace(path, EVENT_TYPES):
            self.handle(event)
        return self.journal


def run_trace(path: str | Path, gateway: Gateway | None = None) -> list[dict]:
    return ReplayEngine(gateway).run_path(path)
