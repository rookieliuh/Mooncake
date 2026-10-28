# 跨节点测试：A 预热、C 读取、统一清理

批量测试可将下面命令中的 `--io-api buffer` 替换为 `--io-api batch --batch-size 8`；内存需求、统计口径和可选 UB 退出规避见 [批量与 UB 回迁说明](BATCH_UB.zh-CN.md)。

本流程验证 A 写入的对象能由 C 直接读取。B 提供实际存储容量；A、C 使用同一 Master、同一 Metadata 配置和兼容的 SDK。默认单客户端命令不变：不传 `--namespace` 时仍使用随机命名空间，并自动清理本轮对象。

## 代码和部署

| 节点 | 所需内容 |
|---|---|
| B | 已部署的 Master、所选模式的 Metadata 服务、提供容量的 Store 节点 |
| A | 本开发分支的 benchmark，及与 B 相同源码提交构建的 Python binding/原生 SDK |
| C | 与 A 相同的 benchmark、SDK、Python 版本及完全相同的轨迹文件 |

我们已只读核对 `yyyuanhao426-hash/Mooncake` 的 `supercache` 提交 `5a10f3572674416893ff45ee63ed7e4f20a0080a` 的基础接口。此处不修改该仓库，也不将 benchmark 合入它。实际部署若使用另一提交，需记录并核对。源码接口检查及离线测试不替代真实 TCP/UB 联调。

A、C 均拉取我们账号的代码；以下假定新建目录，如已有 checkout 则在对应开发分支执行 `git pull --ff-only`：

```bash
git clone --single-branch --branch benchmark/store-backend-930 \
  https://github.com/rookieliuh/Mooncake.git Mooncake-bench
cd Mooncake-bench/benchmarks/storage_benchmark_v1
python -c 'from mooncake.store import MooncakeDistributedStore; print("binding import OK")'
```

命令示例：B 为 `10.0.0.10`，A 为 `10.0.0.20`，C 为 `10.0.0.30`；B 的 Master 端口为 50051，HTTP Metadata 端口为 8080。替换成实际部署值。若使用 P2P 握手，所有参与者按部署配置使用 `--metadata-server P2PHANDSHAKE`。

A、C 都设置 `--global-segment-size 0`，不贡献容量，B 必须已有足够存储容量。这样 A 预热进程退出不会因撤销 A 的存储内存而丢失预热对象。不要在 A/C 提供存储容量的同时，用其退出后的结果证明 B 远端存储可用。

## 参数与对象身份

| 参数 | 含义 |
|---|---|
| `--namespace` | 一轮实验的人工指定 ID，1–80 个英文字母、数字、`_` 或 `-`。跨节点共享时相同；新实验换一个 ID |
| `--key-scope shared` | 所有参与者共享相同逻辑页面；必须显式填写，默认仍为 independent |
| `--client-id` | 结果中的参与者标识。explicit namespace + independent 模式必填，且各独立参与者须不同；共享模式下不加入页面 key |
| `--action prefill` | 只预热，不回放、不自动清理。需 namespace 和新的 key-manifest 文件；`--verify` 会读回每页校验 |
| `--access-mode read` | 直接读取；指定 namespace 后可不带 warmup，缺页会失败，不会偷偷补写 |
| `--key-manifest` | 写入前落盘的精确计划 key 清单。显式 namespace 下，prefill、cache、write 和 warmup 写入均必须提供 |
| `--action cleanup --cleanup-manifest ...` | 在所有参与者停止后，读取一个或多个写端清单，去重并逐 key 强制删除；不扫描全库或做前缀删除 |

共享页面名为 `mcbench-<实验ID>:v2:<数据集指纹>:<hash_id>`。指纹包含完整轨迹文件 SHA256、模型布局、512-token 粒度及 payload/key schema。不同模型或不同轨迹文件自动隔离，不能仅凭数字 hash 相同就复用。文件换行差异也会改变指纹，因此 A/C 应直接使用相同文件，不能各自重新序列化。

`max-requests` 不进入指纹：C 可以读取 A 已预热轨迹的前缀子集；若 C 读取更多且遇到未预热页，会报缺页。explicit namespace 一次只运行一个 scenario 和一个 replay scale；要扫配置，逐条执行并分开保存结果/清单。复用 namespace 会复用已有对象，做冷启动实验必须用新 ID。

## 1. A 节点：先预检，再预热

先确认前两个请求的工作集容量；正式实验再扩大请求数。GLM5 每页约 53.6 MiB，一个请求可包含多个页面。

```bash
python benchmark.py --backend mooncake --dry-run \
  --namespace ac-demo-001 --key-scope shared \
  --scenario conversation --model glm5 --max-requests 2 \
  --output-json A-sizing.json
```

在 A 执行：

```bash
python benchmark.py --backend mooncake --action prefill \
  --namespace ac-demo-001 --key-scope shared --client-id A \
  --scenario conversation --model glm5 --max-requests 2 --threads 1 \
  --master-server 10.0.0.10:50051 \
  --metadata-server http://10.0.0.10:8080/metadata \
  --local-hostname 10.0.0.20 --protocol tcp \
  --global-segment-size 0 --io-api buffer --verify \
  --key-manifest A-prefill.keys.json --output-json A-prefill.json
```

等待命令完成且退出码为 0。检查 `A-prefill.json`：第一项 `status=ok`、`phase=prefill_complete`，`errors` 和 `cleanup_errors` 为空。预热操作统计位于 `preparation_stats`；正式请求数、正式 I/O 和 QPS 为零，这是仅预热模式的预期结果。

清单在连接/写入之前保存，记录可能写入的所有精确 key；即便 RPC 返回失败但数据已写入，也能后续清理。已有同名清单不会覆盖，重试要使用新的清单文件，并保留所有写端清单。预热失败时不得启动正常测量，应调查错误并用清单清理或重试。

## 2. C 节点：直接读取 A 的数据

确认 A 已成功结束后，在 C 执行；不加 `--warmup`，也不需要 key-manifest：

```bash
python benchmark.py --backend mooncake --action replay \
  --namespace ac-demo-001 --key-scope shared --client-id C \
  --scenario conversation --model glm5 --max-requests 2 --threads 1 \
  --master-server 10.0.0.10:50051 \
  --metadata-server http://10.0.0.10:8080/metadata \
  --local-hostname 10.0.0.30 --protocol tcp \
  --global-segment-size 0 --io-api buffer --verify \
  --access-mode read --replay-scales 0 --progress-interval 0 \
  --output-json C-read.json
```

验收条件：

- 退出码 0、`status=ok`，`errors`、`cleanup_errors` 为空。
- A/C 结果中的 `namespace`、`dataset_id`、`trace_sha256`、`page_size_bytes` 一致。
- C 的 `total_requests=2`、`failed_requests=0`、`write_pages=0`，`read_pages` 等于两个请求的 hash 数之和。
- `storage.verification_failures=0`。纯读模式不查询 exists，hit rate 不适用；读成功且零写入是复用预热数据的证据。

本次示例假定 B 容量足够、无淘汰。预热成功不保证对象永不被淘汰；缺页应记录为失败，不能自动补写后声称纯读成功。

共享 namespace 的 replay 无论成功还是失败都不会自动删除对象，可供其他参与者继续读取。若要测真实 exists + get/put 缓存路径，将 C 改为 `--access-mode cache`，并增加 `--key-manifest C-cache.keys.json`；它可能补写 miss，最后清理时必须同时提供 C 的清单。

## 3. 统一清理：所有读写端停止后，由 A 执行

本工具没有分布式屏障或存活成员登记。**由运行者或外部作业系统确认所有参与者已经停止，再执行 cleanup。** 不能仅看到 A 完成就开始清理仍被 C 使用的对象。

先检查清理范围（无需 native binding，也不连接集群）：

```bash
python benchmark.py --backend mooncake --action cleanup --dry-run \
  --master-server 10.0.0.10:50051 \
  --cleanup-manifest A-prefill.keys.json
```

确认后在 A 清理：

```bash
python benchmark.py --backend mooncake --action cleanup \
  --master-server 10.0.0.10:50051 \
  --metadata-server http://10.0.0.10:8080/metadata \
  --local-hostname 10.0.0.20 --protocol tcp \
  --cleanup-manifest A-prefill.keys.json \
  --output-json cleanup.json
```

cleanup 不需要 trace 或模型参数。清单保存 Master 地址，必须与当前参数字符串一致，避免误清其他集群。清理失败会继续尝试剩余 key，返回非零并保留错误；可用原清单重试。已经不存在的对象计入 `already_missing_keys`，不当作清理错误。成功时 `deleted_keys + already_missing_keys = planned_keys`。

多个写入进程时，将清单集中到运行 cleanup 的机器并一次传入：

```bash
--cleanup-manifest A-prefill.keys.json C-cache.keys.json D-write.keys.json
```

不要在同一 namespace 仍有读写活动时清理任何一份清单。清单包含该写端计划访问的 key，可能与其他写端重叠；它不是该进程独占所有权的证明。

## 并发、UB 和性能测量

- C 的 `--threads 2/4/8` 仍表示每个 worker 回放完整轨迹；shared 模式不会扩大唯一对象工作集。多个压测节点也同样各自完整回放，不自动分片。
- independent + explicit namespace 的 key 包含 client-id 和 worker ID；不同客户端 ID 会形成独立工作集。重用相同 ID 表示有意复用同一客户端数据，勿给并行独立客户端分配相同 ID。
- 纯写模式每个 key 额外包含客户端标识、随机 execution-id、worker 和操作序号，防止跨节点或重启后重复 put 直接成功而不搬运数据。所有写端保留清单并统一清理。
- A/C 切 UB 时，使用匹配 B 的 UB 构建和各自实际设备名，设置 `--protocol ub --device-name <设备名>`，并 `unset MC_FORCE_TCP`。不能将示例 TCP 测试结果当作 UB 结果。
- 正确性通过后再关闭 verify 测性能。预热与测量分离，正式测量不包含预热和清理。
- `measurement_start_unix` / `measurement_end_unix` 记录各进程本地墙钟，便于外部对齐；它们不是同步屏障。多节点并发测试需要时钟同步和外部启动协调，不要直接相加不同时间窗口的 QPS，也不要平均 P99。当前 JSON 只输出每进程统计，不提供精确全局分位数合并。
- 每轮使用新 namespace 或在全部停止后完整清理。共享缓存下重复 put 仍可能是 no-op，成功 API 的逻辑字节数不等于网卡实际流量，需结合 B 的驻留对象与网卡指标确认实际路径。

## 本地回归验证

```bash
python -B -m unittest discover -s tests -v
```

测试覆盖旧用法、新 namespace 身份隔离、独立预热/纯读、写 key 唯一性、清单覆盖保护、部分写入失败后的清理、重复清理与错误传播；另有三个独立 Python 进程通过持久化 FakeStore 完成 CLI 预热、读取、清理。它验证的是跨进程驱动逻辑，不是 native SDK、真实网络或服务端硬件。
