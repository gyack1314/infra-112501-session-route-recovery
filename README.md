# 推理会话网关的版本粘滞、故障切换与结果确认

终端通过会话编号连续访问推理实例。部署系统发布路由配置与端点就绪情况，合成后端返回当前轮次的提交事实。本仓库在事件契约之上实现了完整的 Python 服务网关（`gateway/`），所有场景均可在本地用合成角色重新运行。

## 事件格式

每个 JSONL 文件是一段独立演练。每行含整数逻辑秒点 timestamp、事件名称 type 和对象 data。同一秒的行按文件顺序排列。时间从零起算。文件里的业务标识只用于建立输入之间的引用。

- **instance**　实例状态事实，id、boot、region、model、format、reads、minimum_runtime、state。state 为 ready、draining 或 down。
- **route**　发布租户的路由 revision、地域 allow_regions 和候选实例权重 weights。候选值为零时不可用于新会话，多候选同权无需按固定算法。
- **open**　终端请求创建 tenant 下的 session。
- **request**　终端带 session、turn、request、body、deadline 推进一轮。turn 从 1 起算。
- **checkpoint_visible**　部署代理报告某个指定实例 boot 已能读到会话 checkpoint。
- **result**　仿真后端报告指定 attempt 的提交事实。state 为 committed、not_committed 或 unknown，只有 committed 携带 result 和 checkpoint。not_committed 含义为该 attempt 此后不会提交。
- **cancel**　客户端对指定 request 申请停止执行。
- **inspect**　按 session 了解等待与结算状态。attempt 的本地引用采用 attempt:N，代表该场演练中路由服务发出的第 N 个独立执行授权。

## 网关结构

| 模块 | 职责 |
| --- | --- |
| `gateway/models.py` | 实例身份、路由代次、会话/轮次/attempt 状态机，全部可 JSON 序列化 |
| `gateway/registry.py` | 部署事实注册表：实例（含 boot 身份）、路由代次、检查点可见性 |
| `gateway/selector.py` | 新会话加权选站、排空粘滞、迁移准入（地域 + 格式 reads + 检查点可见） |
| `gateway/service.py` | 网关核心：轮次串行、在途确认、取消、截止时间闸门、检查视图 |
| `gateway/synthetic.py` | 本地合成后端：派发登记、提交事实查询（含 unknown 与迟到提交），可独立持久化 |
| `gateway/store.py` | 内存存储与原子快照文件存储，支撑重启恢复 |
| `gateway/replay.py` | 事件回放引擎，把演练逐事件驱动网关与合成后端 |
| `gateway/cli.py` | 命令行：回放、检查视图、中途重启恢复演示 |

### 关键语义

1. **选站与粘滞**：新会话只在权重为正、`ready`、地域允许的实例中选择，`draining` 实例不能接新会话；已存在会话在排空期间继续粘滞原实例。实例 `down` 或重启换 boot 才触发迁移，备用实例必须证明能读到会话最后已提交检查点、`reads` 兼容其格式且地域允许。无可用落点时返回明确等待理由，不建立空会话。
2. **身份分离**：实例重启后 boot 变化即新身份。旧 boot 上的 attempt 确认与新 boot 的接收能力分别判断——新身份必须重新证明检查点可见，且不能替旧身份的 attempt 作证。
3. **单轮次边界**：一次会话同一时间只推进一轮，越序轮次排队且不产生 attempt。在途重试返回原 attempt 进度；只有 `not_committed` 证明或运维 `abandon` 后才允许追加新授权。`unknown` 与无响应都不是未执行的证据，迟到的 `committed` 仍进入历史，结果与检查点来自同一执行。
4. **取消**：排队轮次直接取消且不发调用；在途轮次只标 `cancel_pending`，已完成的提交可查且不被抹掉。
5. **截止时间**：每次新发/改发前检查 `now + minimum_runtime <= deadline`，不满足则停止发送并告知理由；过期前已观察到的提交保留在会话历史中。
6. **重启恢复**：网关状态（含 attempt 全局编号）原子落盘。重启后通过提交查询继续消解在途请求，绝不为旧轮次另发新调用。

## 资料文件

- in-doubt-while-draining.jsonl：排空中的在途疑义、迟到提交、取消边界与 down 后迁移
- failover-deadline.jsonl：重启身份变化、not_committed 改发与最小执行时间截止

## 本地运行

需 Python 3.11，仅使用标准库，无需联网。

```sh
# 资料封装检查
python3 trace_io.py data
python3 -m unittest -v

# 网关单元与场景测试（45 个）
python3 -m unittest discover -s tests -v

# 回放演练，逐事件打印网关响应与最终检查视图
python3 -m gateway.cli replay data/in-doubt-while-draining.jsonl
python3 -m gateway.cli replay data/failover-deadline.jsonl --json

# 按会话编号查看路由代次、等待实例与允许的下一步
python3 -m gateway.cli inspect data/in-doubt-while-draining.jsonl --session rounds

# 中途杀掉网关进程、从快照恢复，验证不会重复授权
python3 -m gateway.cli restart-demo data/in-doubt-while-draining.jsonl --store /tmp/gw-demo
```

资料工具输出读取的文件、事件数量及首尾秒点。业务状态由 `gateway/` 内的网关服务维护。
