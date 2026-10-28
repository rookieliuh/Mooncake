# 内网批量与 UB 改动回迁

依据用户提供的《Benchmark 代码改动说明（2026-10-27 ～ 2026-10-28）》，在 `237d7028` 基线上实现第 2 节的四个文件改动，并增加回归测试。本文区分用户内网验证记录与本地离线验证，不将内网性能数字当作本次重新测得的结果。

## 使用方式

原有单客户端及 A 预热/C 读取流程保持不变，将命令中的 API 参数替换为：

```bash
--io-api batch --batch-size 8
```

`batch-size` 必须为正整数，默认 8；只在 batch 模式决定槽位数。cache 模式按整页访问分批执行 `batch_is_exist`、`batch_get_into`、`batch_put_from`。部分页面访问回退单页路径（Store backend 本身仍只允许整页；磁盘 backend 保持原语义）。同一请求中的重复 hash 会在重复处拆批，保留先写后读的逻辑。cache 的 batch-size=1 使用单页路径。

纯读、纯写和独立预热均支持批量，尾部不足一批也会执行。纯写仍使用唯一 key；namespace、写端清单、统一清理规则不变。预热开启 verify 时会读回校验。

buffer 和 batch 模式都使用页对齐 mmap。batch 每个方向分配连续区域，仅注册一次，槽位是该区域的切片。注册长度向 OS 页大小取整，映射末尾额外留一页不注册，避免两个注册区间相邻。单页 buffer 模式也使用相同的对齐策略。正常结束时先注销内存和关闭客户端，再释放视图和映射；native 清理报错时不主动释放映射。

每个 batch worker 的收发缓冲约为 `2 × batch-size × page_size`，另有一页伪随机模板、SDK 内存池、校验临时分配及对齐/保护页开销。GLM5、batch-size=8 时，仅收发缓冲约 858 MiB；扩大线程数前需核算内存。预检中的工作集容量仍指 Store 对象容量，不包含客户端缓冲。

## 统计和错误处理

- batch 返回向量必须与请求页数一致；exists 只接受 0/1，负数不转成命中。get 逐项检查完整字节数，put 逐项检查成功码。
- 部分失败保留已确认成功的页数/字节数，该轮仍标记 failed；失败 RPC 的全部尝试 key 保留在清理范围内。异常结果长度不推测成功项。
- verify 校验完整数据，包括 key 标记和尾部负载，不仅检查 marker。
- `storage.batch_calls` 提供 exists/read/write 的整批 API 调用次数及真实批耗时分位数，包含返回了错误向量的调用；抛出异常的调用计入 attempts/errors，但没有完成耗时样本。
- `storage.latency_semantics=amortized_batch_time_per_page` 表示原 read/write 延迟字段是“整批耗时÷该批页数”的摊销值。这不是单页实际完成延迟，不能将其 P99 与单页 API 的 P99 直接比较。
- 请求墙钟延迟、成功逻辑字节数/墙钟时间的带宽仍保留。失败批次的逐页计数只包含确认成功的项，整批耗时可在 batch_calls 查看。attempts/errors 表示 API 调用次数（一次批调用计一次），不等于页面数。
- 独立预热的 batch 统计在 `preparation_stats` 中，不计入正式回放；`MC_BATCH_DEBUG=1` 可输出 cache 请求的窗口和 hash 数，正式测性能时关闭。

## 可选退出规避

用户内网记录报告：部分 UB 绑定在解释器 finalize 阶段出现 double free。按文档提供显式开关：

```bash
MC_EXIT_FAST=1 python benchmark.py ...
```

只在 `main()` 正常返回后刷新 stdout/stderr 并调用 `os._exit(0)`；JSON 写出、业务清理、客户端关闭仍先执行。参数错误、测试失败等非零退出不改成成功。默认不启用，不因协议自动启用。该选项跳过解释器最终清理和 atexit 回调，是临时规避，不是 native 线程亲和性修复；失败退出时也不保证规避 finalize 崩溃。

## 运行时脚本和构建的边界

文档第 3、4 节描述的 `mc-env.sh`、`run_*.sh`、`store_b.py`、`check_ready.sh` 没有提供完整源文件，本次不声称已原样纳入仓库，也不修改机器级端口、驱动、RPATH 或服务部署。后续获取脚本原文后再单独归档。现有内网脚本可继续传递 MODEL/SCENARIO/IO_API、协议和设备；新 benchmark 接受 batch 参数及显式退出开关。

回迁时将 `benchmark.py`、`store_runner.py`、`store_lifecycle.py`、`storage/mooncake_store.py` 同步到各节点对应的 TCP/UB 运行时副本，保持其他 benchmark 文件来自 `237d7028` 或本开发分支。不要覆盖运行时的原生库或向其他人的远程仓库推送。A/C 使用相同 benchmark 版本与匹配 B 的 SDK；升级批量模式不改变已有 namespace 的数据布局。

本地测试包括 mmap 对齐及注册范围、批量完整性/错误码/尾部损坏、三种访问模式和预热、重复 key 顺序、快速退出成功及错误退出码，并回归原单客户端和跨进程生命周期。未在本机重新验证 UB 硬件吞吐或 native double free。
