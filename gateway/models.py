"""网关核心数据模型。

所有状态对象都可序列化为普通 JSON 兼容字典，便于快照持久化与重启恢复。
状态术语对齐部署清单中的实例状态（ready/draining/down）与结果事实
（committed/not_committed/unknown）。
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum


class InstanceState(str, Enum):
    READY = "ready"
    DRAINING = "draining"
    DOWN = "down"


class AttemptState(str, Enum):
    """一次执行授权（attempt）的生命周期。"""

    IN_FLIGHT = "in_flight"
    UNKNOWN = "unknown"          # 暂时无法确认，仍可能迟到提交
    COMMITTED = "committed"      # 已提交，携带结果与检查点
    NOT_COMMITTED = "not_committed"  # 原实例保证此后不会提交
    ABANDONED = "abandoned"      # 明确放弃，此后不可能再提交


class TurnState(str, Enum):
    """一轮请求在网关侧的结算状态。"""

    QUEUED = "queued"            # 已受理但因顺序/等待原因尚未发出
    IN_FLIGHT = "in_flight"      # 已有执行授权在途
    COMMITTED = "committed"      # 本轮已确认提交
    NOT_COMMITTED = "not_committed"  # 本轮确认未执行（取消或未提交）
    CANCEL_PENDING = "cancel_pending"  # 已申请取消，但在途结果仍待确认
    CANCELLED = "cancelled"      # 取消已结算，本轮不会再执行


# 非派发时给终端的明确等待理由。终端不应自行挑选后端。
WAIT_NO_CAPACITY = "no_eligible_instance"
WAIT_DRAIN_MIGRATION = "waiting_checkpoint_on_standby"
WAIT_IN_DOUBT = "attempt_in_doubt"
WAIT_TURN_ORDER = "previous_turn_open"
WAIT_DEADLINE = "deadline_cannot_fit_minimum_runtime"
WAIT_PINNED_DOWN = "pinned_instance_down"
WAIT_CANCELLED = "turn_cancelled"


@dataclass(frozen=True)
class Instance:
    """部署清单描述的一个推理实例身份。

    boot 在实例重启后变化：相同 id、不同 boot 是不同身份，历史确认
    （旧 boot 上的 attempt）与新地址接收能力要分别判断。
    """

    id: str
    boot: int
    region: str
    model: str
    format: str
    reads: tuple[str, ...]
    minimum_runtime: int
    state: InstanceState

    def to_dict(self) -> dict:
        return {
            "id": self.id,
            "boot": self.boot,
            "region": self.region,
            "model": self.model,
            "format": self.format,
            "reads": list(self.reads),
            "minimum_runtime": self.minimum_runtime,
            "state": self.state.value,
        }

    @classmethod
    def from_dict(cls, raw: dict) -> "Instance":
        return cls(
            id=raw["id"],
            boot=raw["boot"],
            region=raw["region"],
            model=raw["model"],
            format=raw["format"],
            reads=tuple(raw["reads"]),
            minimum_runtime=raw["minimum_runtime"],
            state=InstanceState(raw["state"]),
        )


@dataclass(frozen=True)
class Route:
    """一次发布的租户路由配置。"""

    tenant: str
    revision: int
    allow_regions: tuple[str, ...]
    weights: dict[str, int]

    def to_dict(self) -> dict:
        return {
            "tenant": self.tenant,
            "revision": self.revision,
            "allow_regions": list(self.allow_regions),
            "weights": dict(self.weights),
        }

    @classmethod
    def from_dict(cls, raw: dict) -> "Route":
        return cls(
            tenant=raw["tenant"],
            revision=raw["revision"],
            allow_regions=tuple(raw["allow_regions"]),
            weights=dict(raw["weights"]),
        )


@dataclass
class Attempt:
    """网关对某个实例某次 boot 发出的独立执行授权。"""

    ref: str                     # 演练/服务内全局编号，如 attempt:3
    turn: int
    instance_id: str
    boot: int
    sent_at: int
    deadline: int
    state: AttemptState = AttemptState.IN_FLIGHT
    result: str | None = None
    checkpoint: str | None = None
    # 派发时目标实例已确认可见的会话检查点（None 表示首轮流，无历史）。
    base_checkpoint: str | None = None
    # 派发时目标实例可接受的历史格式与自身输出格式，用于事后审计版本粘滞。
    base_format: str | None = None
    output_format: str | None = None

    def to_dict(self) -> dict:
        return {
            "ref": self.ref,
            "turn": self.turn,
            "instance_id": self.instance_id,
            "boot": self.boot,
            "sent_at": self.sent_at,
            "deadline": self.deadline,
            "state": self.state.value,
            "result": self.result,
            "checkpoint": self.checkpoint,
            "base_checkpoint": self.base_checkpoint,
            "base_format": self.base_format,
            "output_format": self.output_format,
        }

    @classmethod
    def from_dict(cls, raw: dict) -> "Attempt":
        return cls(
            ref=raw["ref"],
            turn=raw["turn"],
            instance_id=raw["instance_id"],
            boot=raw["boot"],
            sent_at=raw["sent_at"],
            deadline=raw["deadline"],
            state=AttemptState(raw["state"]),
            result=raw.get("result"),
            checkpoint=raw.get("checkpoint"),
            base_checkpoint=raw.get("base_checkpoint"),
            base_format=raw.get("base_format"),
            output_format=raw.get("output_format"),
        )


@dataclass
class Turn:
    """一轮客户端请求及其结算轨迹。"""

    turn: int
    request_id: str
    body: str
    deadline: int
    state: TurnState
    # 每一轮只允许串行追加 attempt；在途重试复用第一个在途 attempt。
    attempts: list[Attempt] = field(default_factory=list)
    # 已结算时的结果与检查点（必须来自同一执行，不允许跨 attempt 拼接）。
    result: str | None = None
    checkpoint: str | None = None
    # 最近一次受理/重试时给终端的等待理由。
    wait_reason: str | None = None
    enqueued_at: int | None = None
    cancel_requested: bool = False

    @property
    def current_attempt(self) -> Attempt | None:
        return self.attempts[-1] if self.attempts else None

    @property
    def open(self) -> bool:
        """该轮是否仍占着会话的推进位（未最终结算）。"""

        return self.state in (
            TurnState.QUEUED,
            TurnState.IN_FLIGHT,
            TurnState.CANCEL_PENDING,
        )

    def to_dict(self) -> dict:
        return {
            "turn": self.turn,
            "request_id": self.request_id,
            "body": self.body,
            "deadline": self.deadline,
            "state": self.state.value,
            "attempts": [a.to_dict() for a in self.attempts],
            "result": self.result,
            "checkpoint": self.checkpoint,
            "wait_reason": self.wait_reason,
            "enqueued_at": self.enqueued_at,
            "cancel_requested": self.cancel_requested,
        }

    @classmethod
    def from_dict(cls, raw: dict) -> "Turn":
        return cls(
            turn=raw["turn"],
            request_id=raw["request_id"],
            body=raw["body"],
            deadline=raw["deadline"],
            state=TurnState(raw["state"]),
            attempts=[Attempt.from_dict(a) for a in raw["attempts"]],
            result=raw.get("result"),
            checkpoint=raw.get("checkpoint"),
            wait_reason=raw.get("wait_reason"),
            enqueued_at=raw.get("enqueued_at"),
            cancel_requested=raw.get("cancel_requested", False),
        )


@dataclass
class Session:
    session_id: str
    tenant: str
    # 会话创建时所依据的路由代次。
    opened_revision: int
    # 会话当前粘滞的实例身份；迁移后整体替换。
    pinned_instance: str | None = None
    pinned_boot: int | None = None
    turns: dict[int, Turn] = field(default_factory=dict)
    # 已确认提交的检查点链：checkpoint -> 产出它的轮次与格式。
    last_checkpoint: str | None = None
    last_format: str | None = None

    def to_dict(self) -> dict:
        return {
            "session_id": self.session_id,
            "tenant": self.tenant,
            "opened_revision": self.opened_revision,
            "pinned_instance": self.pinned_instance,
            "pinned_boot": self.pinned_boot,
            "turns": {str(k): v.to_dict() for k, v in sorted(self.turns.items())},
            "last_checkpoint": self.last_checkpoint,
            "last_format": self.last_format,
        }

    @classmethod
    def from_dict(cls, raw: dict) -> "Session":
        return cls(
            session_id=raw["session_id"],
            tenant=raw["tenant"],
            opened_revision=raw["opened_revision"],
            pinned_instance=raw.get("pinned_instance"),
            pinned_boot=raw.get("pinned_boot"),
            turns={int(k): Turn.from_dict(v) for k, v in raw["turns"].items()},
            last_checkpoint=raw.get("last_checkpoint"),
            last_format=raw.get("last_format"),
        )
