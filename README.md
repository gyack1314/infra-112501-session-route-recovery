# 推理会话网关的版本粘滞、故障切换与结果确认

终端通过会话编号连续访问推理实例。部署系统发布路由配置与端点就绪情况，合成后端返回当前轮次的提交事实。

## 事件格式

每个 JSONL 文件是一段独立演练。每行含整数逻辑秒点 timestamp、事件名称 type 和对象 data。同一秒的行按文件顺序排列。时间从零起算。文件里的业务标识只用于建立输入之间的引用。所有场景均可在本地用合成角色重新运行。

- **instance**　实例状态事实，id、boot、region、model、format、reads、minimum_runtime、state。state 为 ready、draining 或 down。
- **route**　发布租户的路由 revision、地域 allow_regions 和候选实例权重 weights。候选值为零时不可用于新会话，多候选同权无需按固定算法。
- **open**　终端请求创建 tenant 下的 session。
- **request**　终端带 session、turn、request、body、deadline 推进一轮。turn 从 1 起算。
- **checkpoint_visible**　部署代理报告某个指定实例 boot 已能读到会话 checkpoint。
- **result**　仿真后端报告指定 attempt 的提交事实。state 为 committed、not_committed 或 unknown，只有 committed 携带 result 和 checkpoint。not_committed 含义为该 attempt 此后不会提交。
- **cancel**　客户端对指定 request 申请停止执行。
- **inspect**　按 session 了解等待与结算状态。attempt 的本地引用采用 attempt:N，代表该场演练中路由服务发出的第 N 个独立执行授权。

## 资料文件

- in-doubt-while-draining.jsonl
- failover-deadline.jsonl

## 本地资料检查

需 Python 3.11。资料工具使用标准库，无需联网。下列命令读取事件文件并检查封装格式。

```sh
python3 trace_io.py data
python3 -m unittest -v
```

资料工具输出读取的文件、事件数量及首尾秒点。业务状态由使用该事件契约的服务维护。
