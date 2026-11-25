"""网关核心服务。

一次会话同一时间只能推进一轮。``submit`` 的状态机保证：

- 原请求已发送后，重试返回原 attempt 的进度，绝不重复下发；
- 只有原实例证明 ``not_committed``（此后不会提交）或运维明确
  ``abandon`` 之后，同一轮才允许追加新的执行授权；
- ``unknown`` 与无响应都不是未执行的证据，迟到的 ``committed``
  仍会结算进会话历史，且结果与检查点来自同一执行；
- 前一轮未结算时，后一轮排队，不同轮次不会越序；
- 每次派发前先过截止时间闸门：``now + minimum_runtime <= deadline``，
  过期可以不发，但已观察到的提交不会被抹掉。
"""

from __future__ import annotations

from .models import (
    Attempt,
    AttemptState,
    Instance,
    Session,
    Turn,
    TurnState,
    WAIT_CANCELLED,
    WAIT_DEADLINE,
    WAIT_IN_DOUBT,
    WAIT_NO_CAPACITY,
    WAIT_TURN_ORDER,
)
from .registry import Registry
from .selector import Selector
from .store import InMemoryStore
from .synthetic import SyntheticBackend


class Gateway:
    def __init__(
        self,
        store=None,
        backend: SyntheticBackend | None = None,
        registry: Registry | None = None,
        selector: Selector | None = None,
    ) -> None:
        self.store = store or InMemoryStore()
        self.backend = backend or SyntheticBackend()
        self.registry = registry or Registry()
        self.selector = selector or Selector(self.registry)
        self.sessions: dict[str, Session] = {}
        self.attempt_counter = 0
        snapshot = self.store.load()
        if snapshot is not None:
            self._restore(snapshot)

    # ==================================================================
    # 部署事实
    # ==================================================================

    def apply_instance(self, instance: Instance) -> dict:
        self.registry.upsert_instance(instance)
        self._persist()
        return {
            "status": "instance_updated",
            "instance": instance.id,
            "boot": instance.boot,
            "state": instance.state.value,
        }

    def apply_route(self, route) -> dict:
        self.registry.publish_route(route)
        self._persist()
        return {
            "status": "route_published",
            "tenant": route.tenant,
            "revision": route.revision,
        }

    def mark_checkpoint_visible(
        self, instance_id: str, boot: int, session_id: str, checkpoint: str
    ) -> dict:
        self.registry.mark_checkpoint_visible(instance_id, boot, session_id, checkpoint)
        self._persist()
        return {
            "status": "checkpoint_marked",
            "instance": instance_id,
            "boot": boot,
            "session": session_id,
            "checkpoint": checkpoint,
        }

    # ==================================================================
    # 会话生命周期
    # ==================================================================

    def open_session(self, tenant: str, session_id: str, now: int) -> dict:
        if session_id in self.sessions:
            existing = self.sessions[session_id]
            return {
                "status": "already_open",
                "session": session_id,
                "tenant": existing.tenant,
                "route_revision": existing.opened_revision,
            }
        route = self.registry.route(tenant)
        if route is None:
            return {
                "status": "waiting",
                "session": session_id,
                "reason": WAIT_NO_CAPACITY,
                "detail": "租户尚未发布路由",
            }
        target, reason = self.selector.choose_for_new_session(tenant)
        if target is None:
            # 明确告知等待理由，不偷偷建一个没有落点的空会话。
            return {
                "status": "waiting",
                "session": session_id,
                "reason": reason,
                "route_revision": route.revision,
            }
        session = Session(
            session_id=session_id,
            tenant=tenant,
            opened_revision=route.revision,
            pinned_instance=target.id,
            pinned_boot=target.boot,
        )
        self.sessions[session_id] = session
        self._persist()
        return {
            "status": "opened",
            "session": session_id,
            "instance": target.id,
            "boot": target.boot,
            "region": target.region,
            "route_revision": route.revision,
        }

    # ==================================================================
    # 轮次推进
    # ==================================================================

    def submit(
        self,
        session_id: str,
        turn_no: int,
        request_id: str,
        body: str,
        deadline: int,
        now: int,
    ) -> dict:
        session = self.sessions.get(session_id)
        if session is None:
            return {"status": "unknown_session", "session": session_id}

        existing = session.turns.get(turn_no)
        if existing is not None:
            return self._retry_existing(session, existing, now)

        # 新轮次：严格顺序，turn 必须恰好是下一个轮次号。
        expected = max(session.turns, default=0) + 1
        if turn_no != expected:
            return {
                "status": "waiting",
                "session": session_id,
                "turn": turn_no,
                "reason": WAIT_TURN_ORDER,
                "detail": f"下一轮应为 {expected}，并行/越序轮次不受理派发",
            }

        turn = Turn(
            turn=turn_no,
            request_id=request_id,
            body=body,
            deadline=deadline,
            state=TurnState.QUEUED,
            enqueued_at=now,
        )
        session.turns[turn_no] = turn
        response = self._try_dispatch(session, turn, now)
        self._persist()
        return response

    def _retry_existing(self, session: Session, turn: Turn, now: int) -> dict:
        """同一轮次的重试：在途返回原进度，已结算返回结算事实。"""

        if turn.state is TurnState.COMMITTED:
            return {
                "status": "committed",
                "session": session.session_id,
                "turn": turn.turn,
                "attempt": turn.attempts[-1].ref,
                "result": turn.result,
                "checkpoint": turn.checkpoint,
            }
        if turn.state is TurnState.CANCELLED:
            return {
                "status": "waiting",
                "session": session.session_id,
                "turn": turn.turn,
                "reason": WAIT_CANCELLED,
            }
        if turn.state in (TurnState.IN_FLIGHT, TurnState.CANCEL_PENDING):
            attempt = turn.current_attempt
            return {
                "status": "in_flight",
                "session": session.session_id,
                "turn": turn.turn,
                "attempt": attempt.ref,
                "instance": attempt.instance_id,
                "boot": attempt.boot,
                "attempt_state": attempt.state.value,
                "reason": WAIT_IN_DOUBT if attempt.state is AttemptState.UNKNOWN else None,
            }
        if turn.state is TurnState.NOT_COMMITTED:
            # 原实例已证明从未提交：允许改发，走完整选路与截止时间检查。
            response = self._try_dispatch(session, turn, now)
            self._persist()
            return response
        # QUEUED：此前因顺序/选路/截止时间暂缓，重试时重新评估。
        response = self._try_dispatch(session, turn, now)
        self._persist()
        return response

    def _try_dispatch(self, session: Session, turn: Turn, now: int) -> dict:
        # 顺序闸门：任何更早的轮次仍开放时不能派发本轮。
        for earlier_no in sorted(session.turns):
            if earlier_no >= turn.turn:
                break
            if session.turns[earlier_no].open:
                turn.state = TurnState.QUEUED
                turn.wait_reason = WAIT_TURN_ORDER
                return {
                    "status": "waiting",
                    "session": session.session_id,
                    "turn": turn.turn,
                    "reason": WAIT_TURN_ORDER,
                }

        target, reason = self.selector.choose_for_turn(session)
        if target is None:
            turn.state = TurnState.QUEUED
            turn.wait_reason = reason
            return {
                "status": "waiting",
                "session": session.session_id,
                "turn": turn.turn,
                "reason": reason,
            }

        # 截止时间闸门：给目标实例留下声明的最小执行时间。
        if now + target.minimum_runtime > turn.deadline:
            turn.state = TurnState.QUEUED
            turn.wait_reason = WAIT_DEADLINE
            return {
                "status": "waiting",
                "session": session.session_id,
                "turn": turn.turn,
                "reason": WAIT_DEADLINE,
                "detail": (
                    f"now={now} + minimum_runtime={target.minimum_runtime} "
                    f"> deadline={turn.deadline}，停止向 {target.id} 发送"
                ),
            }

        return self._issue_attempt(session, turn, target, now)

    def _issue_attempt(
        self, session: Session, turn: Turn, target: Instance, now: int
    ) -> dict:
        self.attempt_counter += 1
        ref = f"attempt:{self.attempt_counter}"
        attempt = Attempt(
            ref=ref,
            turn=turn.turn,
            instance_id=target.id,
            boot=target.boot,
            sent_at=now,
            deadline=turn.deadline,
            base_checkpoint=session.last_checkpoint,
            base_format=session.last_format,
            output_format=target.format,
        )
        turn.attempts.append(attempt)
        turn.state = TurnState.CANCEL_PENDING if turn.cancel_requested else TurnState.IN_FLIGHT
        turn.wait_reason = None
        self.backend.deliver(ref, target.id, target.boot)

        # 迁移在派发时落定：会话粘滞到新实例身份。
        if (session.pinned_instance, session.pinned_boot) != (target.id, target.boot):
            session.pinned_instance = target.id
            session.pinned_boot = target.boot
        self._persist()
        return {
            "status": "dispatched",
            "session": session.session_id,
            "turn": turn.turn,
            "attempt": ref,
            "instance": target.id,
            "boot": target.boot,
            "region": target.region,
            "base_checkpoint": attempt.base_checkpoint,
        }

    # ==================================================================
    # 提交事实与查询消解
    # ==================================================================

    def record_result(
        self,
        attempt_ref: str,
        instance_id: str,
        boot: int,
        state: str,
        result: str | None = None,
        checkpoint: str | None = None,
        now: int | None = None,
    ) -> dict:
        session, turn, attempt = self._locate_attempt(attempt_ref)
        if attempt is None:
            return {"status": "unknown_attempt", "attempt": attempt_ref}
        if (attempt.instance_id, attempt.boot) != (instance_id, boot):
            # 身份变化后的实例不能替旧身份作证。
            return {
                "status": "rejected",
                "attempt": attempt_ref,
                "reason": "identity_mismatch",
                "detail": f"派发身份为 {attempt.instance_id}/{attempt.boot}",
            }
        applied = self._apply_outcome(session, turn, attempt, state, result, checkpoint)
        self._persist()
        return applied

    def _apply_outcome(
        self,
        session: Session,
        turn: Turn,
        attempt: Attempt,
        state: str,
        result: str | None,
        checkpoint: str | None,
    ) -> dict:
        # 幂等：轮次已结算时，重复/迟到事实不改变历史，只回报现状。
        if not turn.open:
            return {
                "status": "duplicate_ignored",
                "attempt": attempt.ref,
                "turn_state": turn.state.value,
            }

        if state == "unknown":
            attempt.state = AttemptState.UNKNOWN
            # 轮次仍占着推进位：unknown 不是未执行的证据。
            turn.state = (
                TurnState.CANCEL_PENDING if turn.cancel_requested else TurnState.IN_FLIGHT
            )
            turn.wait_reason = WAIT_IN_DOUBT
            return {"status": "in_doubt", "attempt": attempt.ref}

        if state == "not_committed":
            attempt.state = AttemptState.NOT_COMMITTED
            if turn.cancel_requested:
                turn.state = TurnState.CANCELLED
            else:
                turn.state = TurnState.NOT_COMMITTED
            turn.wait_reason = None
            return {"status": "not_committed", "attempt": attempt.ref}

        if state == "committed":
            if result is None or checkpoint is None:
                raise ValueError("committed 事实必须携带 result 与 checkpoint")
            attempt.state = AttemptState.COMMITTED
            attempt.result = result
            attempt.checkpoint = checkpoint
            # 结果与检查点必须来自同一执行，并整体成为会话的新状态。
            turn.state = TurnState.COMMITTED
            turn.result = result
            turn.checkpoint = checkpoint
            turn.wait_reason = None
            session.last_checkpoint = checkpoint
            committed_format = self._committed_format(attempt, session)
            session.last_format = committed_format
            return {
                "status": "committed",
                "attempt": attempt.ref,
                "turn": turn.turn,
                "result": result,
                "checkpoint": checkpoint,
            }

        raise ValueError(f"未知提交事实状态：{state}")

    def _committed_format(self, attempt: Attempt, session: Session) -> str | None:
        # 优先使用派发时冻结的目标实例输出格式；旧快照缺该字段时
        # 以会话最近已知格式兜底。
        if attempt.output_format is not None:
            return attempt.output_format
        instance = self.registry.instance(attempt.instance_id)
        if instance is not None and instance.boot == attempt.boot:
            return instance.format
        return session.last_format

    def reconcile(self, now: int | None = None) -> list[dict]:
        """重启后/周期性消解：向合成后端查询所有在途 attempt。

        只查询、不重发。查询回答 unknown 时保持在途，绝不为旧轮次
        另发一笔新调用。
        """

        applied: list[dict] = []
        for session in list(self.sessions.values()):
            for turn in list(session.turns.values()):
                if not turn.open:
                    continue
                attempt = turn.current_attempt
                if attempt is None or attempt.state not in (
                    AttemptState.IN_FLIGHT,
                    AttemptState.UNKNOWN,
                ):
                    continue
                fact = self.backend.query(attempt.ref)
                if fact["state"] == "unknown":
                    continue
                applied.append(
                    self._apply_outcome(
                        session,
                        turn,
                        attempt,
                        fact["state"],
                        fact.get("result"),
                        fact.get("checkpoint"),
                    )
                )
        if applied:
            self._persist()
        return applied

    def abandon_attempt(self, attempt_ref: str) -> dict:
        """运维明确放弃某次在途执行：此后它不可能再提交。"""

        session, turn, attempt = self._locate_attempt(attempt_ref)
        if attempt is None:
            return {"status": "unknown_attempt", "attempt": attempt_ref}
        if attempt.state is AttemptState.COMMITTED:
            return {"status": "rejected", "attempt": attempt_ref, "reason": "already_committed"}
        if not turn.open:
            return {"status": "duplicate_ignored", "attempt": attempt_ref}
        attempt.state = AttemptState.ABANDONED
        turn.state = TurnState.CANCELLED if turn.cancel_requested else TurnState.NOT_COMMITTED
        turn.wait_reason = None
        self._persist()
        return {"status": "abandoned", "attempt": attempt_ref, "turn_state": turn.state.value}

    def _locate_attempt(self, attempt_ref: str):
        for session in self.sessions.values():
            for turn in session.turns.values():
                for attempt in turn.attempts:
                    if attempt.ref == attempt_ref:
                        return session, turn, attempt
        return None, None, None

    # ==================================================================
    # 取消
    # ==================================================================

    def request_cancel(self, session_id: str, request_id: str, now: int) -> dict:
        session = self.sessions.get(session_id)
        if session is None:
            return {"status": "unknown_session", "session": session_id}
        target = next(
            (t for t in session.turns.values() if t.request_id == request_id), None
        )
        if target is None:
            return {"status": "unknown_request", "request": request_id}

        if target.state is TurnState.QUEUED:
            # 从未发出：直接取消，不产生任何执行授权。
            target.state = TurnState.CANCELLED
            target.cancel_requested = True
            target.wait_reason = None
            self._persist()
            return {"status": "cancelled", "turn": target.turn, "request": request_id}

        target.cancel_requested = True
        if target.state is TurnState.COMMITTED:
            # 已完成的轮次可查，取消不能抹掉提交。
            return {
                "status": "committed",
                "turn": target.turn,
                "request": request_id,
                "result": target.result,
                "checkpoint": target.checkpoint,
            }
        if target.state is TurnState.NOT_COMMITTED:
            target.state = TurnState.CANCELLED
            self._persist()
            return {"status": "cancelled", "turn": target.turn, "request": request_id}
        if target.state is TurnState.CANCELLED:
            return {"status": "cancelled", "turn": target.turn, "request": request_id}

        # IN_FLIGHT / CANCEL_PENDING：只标取消待确认，边界仍由提交事实决定。
        target.state = TurnState.CANCEL_PENDING
        target.wait_reason = WAIT_IN_DOUBT
        self._persist()
        return {
            "status": "cancel_pending",
            "turn": target.turn,
            "request": request_id,
            "attempt": target.current_attempt.ref,
        }

    # ==================================================================
    # 运维检查视图
    # ==================================================================

    def inspect(self, session_id: str) -> dict:
        session = self.sessions.get(session_id)
        if session is None:
            return {"status": "unknown_session", "session": session_id}
        route = self.registry.route(session.tenant)
        turns_view = []
        waiting_on = None
        next_action = "accept_next_turn"
        for turn_no in sorted(session.turns):
            turn = session.turns[turn_no]
            current = turn.current_attempt
            view = {
                "turn": turn_no,
                "request": turn.request_id,
                "state": turn.state.value,
                "cancel_requested": turn.cancel_requested,
                "wait_reason": turn.wait_reason,
                "deadline": turn.deadline,
                "attempts": [a.ref for a in turn.attempts],
                "current_attempt": None
                if current is None
                else {
                    "ref": current.ref,
                    "instance": current.instance_id,
                    "boot": current.boot,
                    "state": current.state.value,
                    "sent_at": current.sent_at,
                    "deadline": current.deadline,
                    "base_checkpoint": current.base_checkpoint,
                },
                "result": turn.result,
                "checkpoint": turn.checkpoint,
            }
            turns_view.append(view)
            if waiting_on is None and turn.open:
                if turn.state is TurnState.QUEUED:
                    next_action = "retry_pending_turn"
                else:
                    next_action = "await_commit_confirmation"
                    waiting_on = (
                        None
                        if current is None
                        else {
                            "attempt": current.ref,
                            "instance": current.instance_id,
                            "boot": current.boot,
                            "attempt_state": current.state.value,
                        }
                    )

        pinned = None
        if session.pinned_instance is not None:
            pinned = {"instance": session.pinned_instance, "boot": session.pinned_boot}
            instance = self.registry.instance(session.pinned_instance)
            if instance is None or instance.boot != session.pinned_boot:
                pinned["identity_current"] = False
            else:
                pinned["identity_current"] = True
                pinned["state"] = instance.state.value

        return {
            "status": "ok",
            "session": session_id,
            "tenant": session.tenant,
            "opened_revision": session.opened_revision,
            "current_revision": None if route is None else route.revision,
            "pinned": pinned,
            "last_checkpoint": session.last_checkpoint,
            "last_format": session.last_format,
            "waiting_on": waiting_on,
            "next_action": next_action,
            "turns": turns_view,
        }

    # ==================================================================
    # 持久化
    # ==================================================================

    def _persist(self) -> None:
        self.store.save(
            {
                "registry": self.registry.to_dict(),
                "sessions": {sid: s.to_dict() for sid, s in self.sessions.items()},
                "attempt_counter": self.attempt_counter,
            }
        )

    def _restore(self, raw: dict) -> None:
        self.registry.load_dict(raw["registry"])
        self.sessions = {
            sid: Session.from_dict(payload) for sid, payload in raw["sessions"].items()
        }
        self.attempt_counter = raw.get("attempt_counter", 0)
        self.selector = Selector(self.registry)
