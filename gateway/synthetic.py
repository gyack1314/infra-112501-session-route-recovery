"""本地合成推理后端。

它扮演不同地域的推理实例集合，供网关在本地完成全部联调：

- ``deliver`` 记录网关对某个实例身份（id + boot）发出的执行授权；
- ``report`` 录入演练中的提交事实（committed/not_committed/unknown），
  unknown 之后仍可能迟到 committed；
- ``query`` 是网关侧的提交结果查询入口，可能回答 unknown；
- 重启换 boot 后，旧 boot 上的事实按旧身份保留，新 boot 没有历史。

无响应（尚未录入任何事实）与 not_committed 严格区分：查询不到事实
返回 unknown，绝不能据此判定未执行。后端独立于网关进程存活，因此
其派发与事实记录可通过同一快照存储持久化。
"""

from __future__ import annotations


class SyntheticBackend:
    def __init__(self, store=None) -> None:
        # attempt_ref -> [instance_id, boot]
        self._dispatch: dict[str, list] = {}
        # attempt_ref -> 最近一次事实：committed/not_committed/unknown
        self._outcome: dict[str, str] = {}
        self._committed: dict[str, dict] = {}
        self.store = store
        if store is not None:
            raw = store.load()
            if raw is not None and "backend" in raw:
                self._restore(raw["backend"])

    def deliver(self, attempt_ref: str, instance_id: str, boot: int) -> None:
        self._dispatch[attempt_ref] = [instance_id, boot]
        self._save()

    def report(
        self,
        attempt_ref: str,
        instance_id: str,
        boot: int,
        state: str,
        result: str | None = None,
        checkpoint: str | None = None,
    ) -> None:
        delivered = self._dispatch.get(attempt_ref)
        if delivered is None:
            raise KeyError(f"后端未收到过 {attempt_ref}，无法报告其提交事实")
        if tuple(delivered) != (instance_id, boot):
            raise ValueError(
                f"{attempt_ref} 的提交事实身份 {instance_id}/{boot} "
                f"与派发身份 {delivered[0]}/{delivered[1]} 不一致"
            )
        self._outcome[attempt_ref] = state
        if state == "committed":
            if result is None or checkpoint is None:
                raise ValueError("committed 事实必须携带 result 与 checkpoint")
            self._committed[attempt_ref] = {"result": result, "checkpoint": checkpoint}
        self._save()

    def query(self, attempt_ref: str) -> dict:
        """提交结果查询。未知与无响应都回答 unknown，不臆断未执行。"""

        state = self._outcome.get(attempt_ref, "unknown")
        payload: dict = {"state": state}
        if state == "committed":
            payload.update(self._committed[attempt_ref])
        return payload

    def delivered_to(self, attempt_ref: str) -> tuple[str, int] | None:
        value = self._dispatch.get(attempt_ref)
        return None if value is None else tuple(value)

    def reset(self) -> None:
        self._dispatch.clear()
        self._outcome.clear()
        self._committed.clear()
        self._save()

    # ---- 持久化 -------------------------------------------------------

    def _save(self) -> None:
        if self.store is None:
            return
        self.store.save({"backend": self.to_dict()})

    def to_dict(self) -> dict:
        return {
            "dispatch": self._dispatch,
            "outcome": self._outcome,
            "committed": self._committed,
        }

    def _restore(self, raw: dict) -> None:
        self._dispatch = dict(raw.get("dispatch", {}))
        self._outcome = dict(raw.get("outcome", {}))
        self._committed = dict(raw.get("committed", {}))
