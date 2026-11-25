"""推理会话网关。

终端按会话编号连续调用不同地域的推理实例。网关负责：

- 按发布的路由权重与实例可用情况为新会话选站，并在实例排空、下线时
  以检查点可见性、格式兼容与地域限制为前提做粘滞与迁移；
- 保证同一会话同一时间只推进一轮，在途请求的重试返回原进度，
  只有原实例证明从未提交或明确放弃后才允许改发；
- 按客户端截止时间调度，过期不发新调用但保留已观察到的提交；
- 以可持久化的状态在重启后继续消解未确认请求。
"""

from .service import Gateway
from .store import DurableStore, InMemoryStore
from .synthetic import SyntheticBackend

__all__ = ["Gateway", "DurableStore", "InMemoryStore", "SyntheticBackend"]
