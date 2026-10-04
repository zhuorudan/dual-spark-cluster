# V2EX 发布帖（分享创造节点）

## 标题（二选一）

**A（数据流）**：双 DGX Spark 跑 320B MoE 大模型：TP=2 双机组网、一键切模型、看门狗自愈，全套开源了

**B（痛点流）**：把两台 DGX Spark 玩成一个生产级 LLM 集群，踩过的坑和工具全开源了

---

## 正文

之前收了两台 DGX Spark（GB10，128GB 统一内存），一直琢磨怎么在家把它们变成一个像样的 LLM 集群。折腾了一个多月，现在稳定跑了一个多月，把整套工具链开源了：

**https://github.com/zhuorudan/dual-spark-cluster**

先说能干什么：

- **TP=2 双机张量并行**：两条 CX7 100G RoCE 直连（MTU 9000 巨帧），跑 DeepSeek-V4-Flash 156GB bf16 无压力
- **一键切模型**：`dspark.sh use glm53`，停旧→等内存释放→起新→健康检查全自动，2-10 分钟
- **看门狗**：每 2 分钟判活，head 挂了自动双机成对重建（单端重启会 NCCL 失配死循环，这个坑用 50 分钟停摆换来的），崩溃现场自动留档 7 天
- **Dify 兼容 shim**：新版 vLLM 砍掉了 legacy functions 协议，Dify 工作流 Agent 会静默卡死。写了个协议翻译层，所有模型的 Dify 构建模式都能用工具调用了
- **统一网关 + Web 面板**：客户端永远只配一个模型名 `dspark`，底下切什么模型无感知；面板能管模型全生命周期（下载/注册/切换/双机缓存一致性）

实测数据（glm53，GLM-5.3-Flash W4A16，320B 总参/18B 激活 MoE，1M 上下文）：

| 并发 | 聚合 tok/s | TTFT |
|---|---|---|
| 1 | 21.1 | 0.39s |
| 4 | 51.2 | 1.6s |
| **6** | **64.3（饱和点）** | ~1s |
| 8 | 56.9（排队了） | 20s |

KV 池 192 万 token（fp8），6 路并发才用 18.8%，长上下文 Agent 会话随便跑。

**踩过的坑（文档里都有）**：

- CX7 网口被 NetworkManager 抢占，netplan 双重占用起不来
- HF 下载 Xet 必 401，要 `HF_HUB_DISABLE_XET=1`
- 300M 家宽下 156GB 模型下载：双机分片并行 + hf-mirror/代理双通道，利用率从 30% 拉到 80%
- GB10 UVM 的 NVRM 内存报错是 warmup 噪声，别当崩溃处理
- warmup 内存峰值会触发 earlyoom 误杀，两机都得停用它

**硬件成本**：两台 Spark + 一台普通路由器就够，推理流量全走直连，不占外网带宽。

适合谁：家里有两台 Spark（或者打算入第二台）、想跑 100B+ 模型、用 Dify 做 Agent 的兄弟。单机用户用 dspark.sh 也有意义（模型注册/下载编排/看门狗逻辑一样适用）。

MIT 协议，欢迎 star、提 issue、贡献新模型配方。下一步打算补 MiniMax-M2.7 NVFP4 的实测数据（131GB 已经下载好了还没验）。

---

## 回复话术预案

- **问功耗**：每台 240W 峰值，待机 ~10-25W，家里跑一个月电费几十块
- **问对比 Mac Studio 集群**：Spark 有 ConnectX-7 原生 100G 直连 + NVIDIA 生态（vLLM/TRT 官方支持），Mac 走 Thunderbolt 桥接带宽差一个量级
- **问为什么不用云 API**：数据不出域 + 长上下文高频调用成本倒挂，跑 Agent 循环本地 token 边际成本≈0
- **问一百万个 bug**：issue 随便提，**真人测试过双机形态**，单机形态社区反馈我也接
- **阴阳怪气"又一个玩具"**：不接招，只回"稳定跑了 37 天，看门狗计数 0，欢迎抄作业"

---

# Reddit r/LocalLLaMA 发布帖（英文）

## Title (pick one)

**A**: I turned 2× DGX Spark into a production LLM cluster — TP=2, model hot-swapping, self-healing watchdog. Full toolchain open-sourced

**B**: Dual DGX Spark (2×128GB unified memory) running 320B MoE at 68 tok/s aggregate — open-sourced the entire ops toolchain

---

## Body

I've been running two NVIDIA DGX Sparks (GB10, 128GB unified memory each) as a home cluster for over a month now. Everything I built to make them behave like production infrastructure is now open source:

**https://github.com/zhuorudan/dual-spark-cluster**

**The stack:**

- **TP=2 tensor parallelism** over dual ConnectX-7 100G RoCE links (MTU 9000). DeepSeek-V4-Flash at 156GB bf16 runs comfortably across both nodes.
- **One-key model switching**: `dspark.sh use glm53` — stop old engine, wait for memory release, start new one, health-check gate. 2–10 minutes depending on model size.
- **A watchdog that actually works**: 2-minute liveness timer. When the head node dies but its container stays "running" (which used to wedge the pair in an NCCL mismatch loop for 50 minutes), it now rebuilds **both nodes in order** automatically and keeps crash postmortems for 7 days.
- **Dify compatibility shim**: newer vLLM removed OpenAI legacy `functions`, which silently breaks Dify workflow agents. The shim translates both directions, so tool calling works on any model.
- **Unified gateway + web panel**: clients always point at model name `dspark`; the gateway routes to whichever engine is active. The panel handles the full model lifecycle including dual-node cache consistency.

**Real numbers** (GLM-5.3-Flash W4A16, 320B total / 18B active MoE, 1M context):

| Concurrency | Aggregate tok/s | TTFT |
|---|---|---|
| 1 | 21.1 | 0.39s |
| 4 | 51.2 | 1.6s |
| **6** | **64.3 (saturation)** | ~1s |
| 8 | 56.9 (queuing starts) | 20s |

KV cache pool: 1.92M tokens (fp8). 6-way concurrent short sessions only touch 18.8% of it.

**War stories in the docs:**

- CX7 ports hijacked by NetworkManager → netplan double-bind failure
- HuggingFace Xet downloads 401 unconditionally → `HF_HUB_DISABLE_XET=1`
- Downloading a 156GB model over a 300M home line: shard-split across both machines + hf-mirror/proxy dual channel pushed utilization from 30% to 80%
- GB10's `NVRM ... NV_ERR_NO_MEMORY` dmesg spam during warmup is harmless noise
- earlyoom will kill vLLM during warmup memory spikes — disable it on both nodes

**Who this is for**: anyone with two Sparks (or considering a second one) who wants to run 100B+ models with agent workloads. Single-node users still get value from the model registry, download orchestration, and watchdog logic.

MIT licensed. Issues and PRs welcome — especially new model recipes with measured benchmarks. Next up: MiniMax-M2.7 NVFP4 benchmarks (131GB downloaded, not yet validated).

---

## Reddit 回复预案

- **"Why not just rent GPU cloud?"**: $0.5-2/hr × 730h/mo per GPU-pair for agent workloads adds up fast; local marginal cost is electricity. Plus data stays home.
- **"Spark is overpriced"**: Fair on pure FLOPS/$, but 244GB unified memory across two boxes with 200G interconnect at this noise/power level (240W each) has no real competitor for home 100B+ serving.
- **"Does it work with single Spark?"**: Yes — model registry, download orchestration, gateway, and shim are node-count agnostic. TP=2 specifics are isolated to the launch recipes.
- **"Windows support?"**: No. NVIDIA's DGX stack is Ubuntu/aarch64; this is glued to that reality.
- **Star-begging accusations**: Ignore. Ship benchmarks, answer technical questions, let the repo speak.
