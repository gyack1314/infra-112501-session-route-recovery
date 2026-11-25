"""网关状态持久化。

服务重启后必须继续消解未确认的请求，而不是为旧轮次另发新调用，
因此路由事实、会话轮次与 attempt 编号都落在可恢复的快照里。
``DurableStore`` 采用临时文件加原子替换，避免重启读到半写状态。
"""

from __future__ import annotations

import json
import os
from pathlib import Path


class InMemoryStore:
    def __init__(self) -> None:
        self._raw: dict | None = None

    def save(self, snapshot: dict) -> None:
        self._raw = json.loads(json.dumps(snapshot))

    def load(self) -> dict | None:
        if self._raw is None:
            return None
        return json.loads(json.dumps(self._raw))


class DurableStore:
    def __init__(self, path: str | os.PathLike[str]) -> None:
        self.path = Path(path)

    def save(self, snapshot: dict) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        tmp = self.path.with_name(f".{self.path.name}.tmp")
        with tmp.open("w", encoding="utf-8") as handle:
            json.dump(snapshot, handle, ensure_ascii=False, separators=(",", ":"))
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(tmp, self.path)

    def load(self) -> dict | None:
        if not self.path.exists():
            return None
        with self.path.open(encoding="utf-8") as handle:
            return json.load(handle)
