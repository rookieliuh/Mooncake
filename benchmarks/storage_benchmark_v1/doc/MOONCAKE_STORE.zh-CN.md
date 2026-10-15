# Mooncake Store 测试方法与运行说明

本次修改基于用户 fork 的 `supercache-snapshot-930`，将该分支的 KVCache Storage Benchmark v1 接入 Mooncake Store。保留 930 原有磁盘路径、FAST25 轨迹格式、MLA 页面布局和延迟分位数算法，新增 Store I/O、预热、纯读写、失败统计与 JSON 输出。实机部署与测量由使用者执行；离线测试不代表网络或集群性能。

## 源码与兼容性范围

- 仓库：[rookieliuh/Mooncake](https://github.com/rookieliuh/Mooncake/tree/supercache-snapshot-930)。基线 commit：`d869f481e11aed2a6ee4860135646188a50bce5a`。
- 开发分支：`benchmark/store-backend-930`，位于 `rookieliuh/Mooncake` fork 中；`supercache-snapshot-930` 保留为开发基线。
- `setup` 使用七个位置参数：client hostname、metadata、global segment bytes、local buffer bytes、protocol、device、master。已对照 930 的 `mooncake-integration/store/store_py.cpp`、`mooncake-store/src/real_client.cpp` 和 `client_service.cpp` 核对基础 API、返回码和资源释放。
- 930 源码确认支持 `--protocol ub`，依赖启用 `USE_UB` 的构建和 URMA 环境。设备名通过 `--device-name` 传入；实际名称从测试机设备清单确认。接口与源码路径已核对，真实 UB 硬件运行仍由实测验证。
- 930 的磁盘实现只有单文件模式。本次未移入官方较新版本的 `per-file` 功能，也未修改 930 的 `storage/disk.py` 或模型布局。

## v1 使用时必须区分的语义

930 磁盘 v1 的 `max-pages` 是取模物理槽数，逻辑命中判断是“曾经写过”。它不能直接表示容量不足时的真实缓存命中率。多线程模式由每个线程各自回放完整轨迹；本次 Store 路径保持这一负载模型，允许显式选择独立或共享 key。

原生 Store 路径用完整逻辑 ID 构造对象名，`exists` 每次查询服务端，容量由集群控制。`max-pages` 不约束 Store，也不在客户端模拟 LRU。`is_exist` 与 `get` 之间仍存在淘汰竞态；读取失败会计错并停止该轮，不会伪装成 hit、miss 或自动重试成功。

参考：[v1 文档](https://github.com/rookieliuh/Mooncake/blob/d869f481e11aed2a6ee4860135646188a50bce5a/benchmarks/storage_benchmark_v1/doc/README.md)、[磁盘实现](https://github.com/rookieliuh/Mooncake/blob/d869f481e11aed2a6ee4860135646188a50bce5a/benchmarks/storage_benchmark_v1/storage/disk.py)。

## 页面与数据口径

FAST25 的每个 hash ID 表示 512 tokens；时间戳单位为毫秒。Store 路径固定这个粒度，不能仅改页面 tokens 就声称重建了其他块大小的真实前缀关系。输入、输出 token 数用于轨迹统计；没有推理计算，也没有根据 output_length 额外生成可复用输出块。

按 benchmark 当前预设计算，每个对象覆盖所有层：

| 预设 | 计算式 | 对象字节数 | MiB |
|---|---|---:|---:|
| glm5 | 512 × 78 × (512 + 64 + 128) × 2 | 56,229,888 | 53.625 |
| kimi-k2.6 | 512 × 61 × (512 + 64) × 2 | 35,979,264 | 34.3125 |

这些是 benchmark 预设的尺寸，不是对实际部署模型、量化、TP/PP 分片或推理引擎布局的确认。若真实系统按层或按 rank 存储，应另立布局实验，不能直接用这里的整模型对象带宽推算在线收益。

数据为固定随机种子的伪随机负载，加上包含完整对象 key 的校验标记。`--verify` 校验整个读取对象，可发现错 key、截断和尾部损坏。写缓冲准备与读后校验在 API 延迟计时外，但计入请求墙钟时间。此路径只是应用缓冲区注册复用，不宣称整个 SDK/传输链路绝对零拷贝。

参考：[轨迹定义](https://github.com/rookieliuh/Mooncake/blob/d869f481e11aed2a6ee4860135646188a50bce5a/FAST25-release/README.md)、[MLA 预设实现](https://github.com/rookieliuh/Mooncake/blob/d869f481e11aed2a6ee4860135646188a50bce5a/benchmarks/storage_benchmark_v1/layout/mla.py)。

## 先做原生能力摸底

按顺序执行，每一阶段通过后再扩大负载。

1. 在 Linux 测试环境安装与服务端匹配的 Mooncake Python binding，启动 Master、所需 Metadata 服务和提供容量的 Store 节点。记录各组件 commit、启动参数及网络配置。
2. 用 TCP、单客户端、小量请求和 `--verify` 做 put/get/exists/cleanup 正确性检查。默认 cleanup 会在测量后调用 remove；清理错误也会使结果状态失败。
3. 在相同拓扑对比 `--io-api plain` 与 `buffer`，确认注册内存路径工作，验证后关闭 `--verify` 测吞吐。
4. 再切换 RDMA 或目标 UB 构建，保持其余变量一致。确认对象实际位于远端，并核对 NIC、服务端 I/O 指标。默认 `--global-segment-size 0` 不向集群贡献客户端容量；已有远端容量是运行前提。
5. 最后扩大线程数、工作集和回放速度。需要区分传输引擎上限时，单独运行对应版本自带 Transfer Engine benchmark；该结果不等于 Store 端到端性能。

## 最小运行命令

先做容量预检，无需安装原生绑定或填写服务器地址：

```bash
python benchmark.py --backend mooncake --dry-run \
  --trace-dir ../../FAST25-release/traces --scenario all \
  --model glm5 --threads 1 --output-json sizing.json
```

按本地固定版本的完整轨迹静态统计，单客户端、单副本、未计分配器开销：

| 轨迹 | 请求数 | 页面访问次数 | 唯一页面数 | glm5 工作集 GiB | kimi-k2.6 工作集 GiB |
|---|---:|---:|---:|---:|---:|
| conversation | 12,031 | 288,500 | 182,790 | 9,572.38 | 6,124.98 |
| toolagent | 23,608 | 409,616 | 183,300 | 9,599.08 | 6,142.07 |
| synthetic | 3,993 | 121,877 | 43,924 | 2,300.22 | 1,471.82 |

这是整条轨迹无淘汰时保留所有唯一对象的容量，不是实际峰值驻留或服务性能结果。若服务器容量有限，先用 `--max-requests` 截取轨迹，或将实验明确标为原生容量压力测试。独立 key 的多客户端预检会乘上客户端数，共享 key 则不会。

以下为 Linux Bash，替换实际地址与路径。客户端地址须可由服务端访问，不能把远程测试写成 localhost。示例默认已有远端 Store 容量。

```bash
cd /path/to/Mooncake/benchmarks/storage_benchmark_v1
python benchmark.py \
  --backend mooncake \
  --trace-dir /path/to/Mooncake/FAST25-release/traces \
  --scenario conversation --model glm5 --max-requests 2 \
  --master-server 10.0.0.10:50051 \
  --metadata-server http://10.0.0.10:8080/metadata \
  --local-hostname 10.0.0.20 \
  --protocol tcp --global-segment-size 0 \
  --local-buffer-size 134217728 --io-api buffer \
  --warmup --verify --threads 1 --replay-scales 0 \
  --output-json smoke.json
```

`--max-requests 2` 仍可能包含很多约 54 MiB 的对象，先按唯一 hash 数核算容量。每个 worker 还需约三页大小的 Python 数据/缓冲区常驻内存、SDK 本地池和临时校验内存；plain 与 verify 路径有额外分配。全量预热需容纳完整唯一工作集，否则不能保证全命中。

以此命令为基础调整：

| 场景 | 参数变更 | 解释 |
|---|---|---|
| 冷启动自然复用 | 去掉 `--warmup`，保持 `--access-mode cache` | 从空的新命名空间开始，后续重复块仍会命中 |
| 热缓存自然复用 | `--warmup --access-mode cache` | 预热后仍执行 exists + get/put |
| 读 API 基线 | `--warmup --access-mode read` | 每次直接 get，不查询 exists；命中率不适用 |
| 写 API 基线 | `--access-mode write` | 每次使用含 worker 和操作序号的新 key，测真实插入；不支持 warmup |
| 独立客户端扩展 | `--threads 1/2/4/8/16 --key-scope independent` | 每个线程完整回放，工作集随线程数扩大 |
| 共享前缀争用 | `--threads 2/4/8 --key-scope shared` | 每个线程完整回放，共享对象；并发顺序会改变冷启动命中率 |
| 按时间回放 | `--replay-scales 1,2,4,8` | 每档使用新命名空间，若带 warmup 则每档重新预热 |
| 无节拍吞吐 | `--replay-scales 0` | 每个 worker 同步完成后继续提交，属于闭环负载 |
| TCP 对 RDMA | `--protocol rdma --device-name mlx5_0` | 需要 RDMA 构建和正确设备；示例名称仅作占位 |
| UB | `--protocol ub --device-name urma0` | 930 源码已确认该协议；设备名仅作占位，需 USE_UB 构建与实机验证 |

所有场景使用自动生成的 `mcbench-<uuid>` 命名空间。默认在所有 worker 停止后，对本轮尝试写入的精确 key 调用 `remove(key, force=True)`；不使用 `remove_all` 或正则删除。930 普通删除可能被对象租约拒绝，所以 cleanup 使用 force 并将已淘汰的 `OBJECT_NOT_FOUND=-704` 视为已清理。`--keep-keys` 可保留本轮对象，但以后运行仍使用新命名空间，旧 key 不会被下一轮自动清理，可能占用容量。

930 的 `Client::Put` 对已存在对象可能直接成功返回，不传输页面。纯写模式因此改为每次唯一 key；它的保留容量是「页面访问次数 × 客户端数 × 页面大小」，dry-run 会按这个口径计算。cache 模式保留真实 hash 复用关系；多客户端共享 key 时，竞争产生的重复 put 仍可能是 no-op，所以结果字节数是成功 API 的逻辑负载量，不承诺等于物理网络流量。

每个线程建立一个 Store 客户端。显式指定端口时，多线程需用 `--local-hostname '10.0.0.20:500{worker}'` 等模板保证不同端口；更一般地使用 SDK 支持的无端口主机地址。`--global-segment-size` 和 `--local-buffer-size` 均是每客户端字节数。

## 930 的 UB 运行要点

客户端和存储节点必须使用兼容的 930 构建。源码的 `mooncake-common/common.cmake` 中 `USE_UB` 默认关闭，开启后查找 URMA；具体驱动、库和设备配置由测试环境准备。本 benchmark 不自动安装驱动或修改机器配置。

在前面的 TCP smoke 命令中将协议、设备替换为：

```bash
--protocol ub --device-name urma0
```

这里 `ub` 指本分支的 Kunpeng/URMA transport；`ubshmem` 是另一条实现路径，不能混用。930 的 `Client::Init` 在 UB 未指定设备时使用 `bonding_dev_0`，不是保证自动选择正确设备，建议显式传入测试机设备名；源码支持逗号分隔的设备列表。CPU 注册缓冲区会进入 UB 的普通内存注册路径；本 benchmark 不生成 GPU/NPU KVCache，不能据此评估 GPU/NPU staging 成本。

正式对比前记录以下变量的取值或未设置状态，结果 JSON 的 `client_environment` 也会保留白名单快照：

| 环境变量 | 源码中的作用 |
|---|---|
| `MC_FORCE_TCP` | Transfer Engine 的强制 TCP 开关；进行 UB/RDMA 测试前须 unset，设置为字符串 `0` 也不足以规避所有源码路径；benchmark 会拒绝非 TCP 配置与该变量并存 |
| `MC_URMA_TRANS_MODE` | URMA 传输模式，源码接受 RM、RC、UM |
| `MC_URMA_ACTIVE_PORT` | 固定活动端口，非负整数；未设置时由实现选择 |
| `MC_URMA_BONDING_BALANCE` | 检测到变量存在即启用 bonding balance |
| `MC_URMA_BONDING_MULTIPATH_ENABLE` | 多路径开关，启用值 true、1、on |
| `MC_UB_NUMA_AFFINITY_ENABLE` | UB NUMA affinity，启用值 true、1、on |
| `MC_UB_TRANSPORT_CPU_STAGING`、`MC_UB_STAGING_POOL_SIZE` | 设备内存 staging 路径的开关和池配置；CPU benchmark 不代表该路径已测 |
| `MC_LOG_LEVEL`、`MC_HIFREQ_LOG_SAMPLE_RATE` | 日志级别与高频日志采样；930 有细粒度计时日志，需要固定配置后再对比性能 |

源码核对位置：`mooncake-integration/store/store_py.cpp`（绑定）、`mooncake-store/src/client_service.cpp`（UB 选择及 put 幂等语义）、`mooncake-transfer-engine/src/config.cpp`（URMA/NUMA 参数）、`mooncake-transfer-engine/src/transport/kunpeng_transport/ub_transport.cpp`（注册及 staging）。`source_base` 记录 benchmark 的基线，实际安装的 native wheel/服务端 commit 仍需另行记录；包版本号不能证明二进制来自该 commit。

## 缓存场景测试矩阵

第一轮固定单副本、单存储节点、客户端远端访问、相同硬件与模型。先只改变一个变量，避免全因子组合过大。

| 阶段 | 对照变量 | 要回答的问题 |
|---|---|---|
| 正确性 | 三种 trace × 两种模型，少量请求，verify 开启 | 页面尺寸、读写内容与返回码是否正确 |
| 基本路径 | plain/buffer × TCP/可用 RDMA 或 UB | 复制、注册与传输路径的开销 |
| 冷热缓存 | cold/warm × conversation/toolagent/synthetic | 不同复用模式下，实际 hit 与请求成本如何变化 |
| 并发 | 1、2、4、8、16 客户端；独立/共享 key | 瓶颈在客户端、Master 还是数据面，热点是否降低扩展性 |
| 容量压力 | 工作集 W / 可用有效容量 C 为 0.5、0.9、1.1、2.0 | 原生淘汰、写拒绝、尾延迟如何变化 |
| 分层缓存 | 仅在目标分支确认支持后：DRAM、SSD/offload、回填 | 路径与介质变化的实际成本 |
| 扩展与故障 | 后续：多存储节点、副本、受控节点重启 | 吞吐扩展、数据可用性与恢复行为 |

W = 唯一对象数 × 实际页面字节数；独立 key 模式再乘客户端数。C 使用可分配容量并考虑副本、分配器预留和其他租户占用，不能简单拿总物理内存代入。由服务端配置容量，不使用客户端取模。发生读写错误的轮次作为失败/容量边界记录，不能作为成功吞吐结果。首次失败即停止是当前实现策略；持续故障期间的恢复曲线需另做测试驱动。

若要测“全 miss 写入”，应使用全唯一 hash 的受控轨迹；自然 trace 的冷启动不等于全程零命中。若要固定 0/25/50/75/100% 目标命中率，需预填指定集合并生成明确的访问序列，记录实际服务端查询命中率；不能靠吞掉 miss 达成目标。

## 指标定义与复现要求

- 成功请求 QPS = 全部 worker 成功完成请求数 / 同一回放窗口墙钟时间。初始化、预热、cleanup 不计入。失败请求中已经完成的页面 I/O 仍单独保留，并标记整轮失败。
- 读写墙钟带宽 = 成功 API 逻辑负载字节数 / 回放窗口秒数，输出字段 `wall_mib_per_second`。共享 cache 下重复 put 可能不搬运数据，网卡物理带宽需从服务端/网卡侧另测；纯写使用唯一 key 避免该幂等路径。`bytes / summed op time` 只表示按操作耗时归一化的速率，不是并发聚合带宽。
- `request_wall_latency` 从实际开始处理请求计时，包含 exists、缓冲准备、verify。`request_io_latency` 只累计读写 API 耗时；exists 单独统计。
- 节拍回放额外记录 `schedule_lag_latency` 和 `arrival_to_completion_latency`，后者包含调度滞后。当前每个 worker 串行处理，不是无限队列的独立开放环压测；`replay-scale=0` 时这两个分位数字段为零且不适用。
- `page_hit_rate` 在 cache 模式表示成功读取页 / 成功页面操作；强制 read/write 模式该数值只是读占比，不解释为缓存命中率。`metadata_hit_rate` 是成功 exists 查询中的存在比例，错误不算 miss。报告失败轮次时必须同时展示错误数。
- written_pages 是本轮曾成功写入的不同完整 key 数，包含预热，不代表当前驻留页数。服务端驻留量、淘汰数和真实介质命中应采集部署侧指标。
- JSON 记录配置、trace SHA256、SDK 包版本、命名空间、各阶段错误、成功/失败请求、API 调用次数和延迟。另附硬件型号、CPU/NUMA 绑核、网卡/驱动、链路、SSD、文件系统、服务端 commit、启动参数和副本策略。
- 每个配置先预检，再重复至少 5 轮；报告各轮样本量、QPS/带宽中位数与范围，以及各轮 P50/P95/P99。不要平均各线程 P99。本实现合并页面/请求原始样本后计算分位数。
- 正式性能轮使用 `--progress-interval 0` 关闭进度日志，关闭 `--verify`，把带校验的正确性轮单独保存。
- 磁盘对照还需注明 OS page cache、fsync 和冷热状态。Store 的内存成功确认与磁盘 fsync 持久化不具备相同耐久语义；TTFT、TPOT 和真实推理 token/s 均不在本测试测量范围内。

## 验证与交付

```bash
python -B -m unittest discover -s tests -v
python benchmark.py --help
```

离线 FakeStore 测试验证 API 返回语义、内存指针读写、错误传播、数据损坏、预热隔离、命名空间和资源释放，并覆盖 930 的 put 幂等行为、七参数 UB 初始化、租约清理、关闭错误和强制 TCP 配置冲突。它不证明原生扩展、跨机传输、服务端容量或 UB 硬件路径可用。

真实环境建议用上面的 smoke 命令先跑 `buffer` 和 `plain` 两轮，再关闭 verify 扩大规模。进程返回非零、JSON `status != ok` 或任何 cleanup_errors 都需调查。原生 SDK 超时由其实现控制；对故障测试可由外部作业系统设置进程级截止时间，强制杀进程可能留下本轮 key。
