# 模型说明：GLM-5.3-Flash W4A16-AutoRound + 原生 MTP3 + PMU128（glm53）

面向 **Dify / JoyAgent 工具调用 + 多会话并发** 的 Agent 模型，双 DGX Spark TP2 部署。
来源配方：`florianbrede-ayet/spark-recipes` 的 `tp2_glm53flash_autoround_mtp3_pmu128`（本机适配版）。

## 规格

| 项目 | 值 |
|---|---|
| 模型 | `Intel/GLM-5.3-Flash-W4A16-AutoRound`，固定 revision `5eee1846…`（GPTQ 元数据适配版） |
| 服务名（API 模型 ID） | `Intel/GLM-5.3-Flash-W4A16-AutoRound` |
| 体量 | 320B 总参 / 18B 激活 MoE；权重 169GiB（36 safetensors） |
| 量化 | W4A16 group_size=128（AutoRound，元数据改写为 GPTQ 后由 vLLM Marlin WNA16 后端加载） |
| 投机解码 | 原生 MTP3（`num_speculative_tokens=3`，`disable_eagle_block_drop=true`） |
| 上下文 | 1,048,576；KV fp8_e4m3，**池 1,920,956 tokens**（13.5GB/机） |
| 并发 | `max-num-seqs 6`，block-size 2304，前缀匹配粒度 PMU128 |
| 工具调用 | `--tool-call-parser glm47 --reasoning-parser glm45`，自动工具选择 |
| 镜像 | `spark-recipes/glm53-autoround-mtp3-pmu128:20260903-difyrc`（45.9GB；= 配方镜像 `:20260903` + Dify reasoning 双发补丁，见下文） |
| 镜像底座 | `ghcr.io/tonyd2wild/vllm-glm53-flash@sha256:4def0ef6…`，vLLM `0.1.dev20051`，构建时 fail-closed 打 PR#53388/#53906/scheduler-LCM 三补丁 + SM121 kpool overlay |
| Endpoint | `http://<HEAD_IP>:8000/v1`（与其它模型一致） |

## 实测性能（2026-09-14，内核 6.17.0-1014 / 驱动 580.142）

- 启动（worker 先起、head 后起；权重加载 ~4 分钟，含缓存 warmup ~7-8 分钟出 health）
- 单流：TTFT 0.39s，**24.5 tok/s**（1225 token/50.4s）
- 3 路并发：TTFT 0.79-1.8s，聚合 **43.3 tok/s**（各流 14-15 tok/s）
- MTP3 接受率约 45%（731/1635 草稿 token，每步均接受 1.34 个）
- 工具调用：`finish_reason=tool_calls`，参数 JSON 正确（glm47 parser）
- PMU128 前缀缓存：重复请求 866 prompt 中命中 768（6×128），命中计数在 `usage.prompt_tokens_details.cached_tokens`

### 并发阶梯压测（2026-09-15，reasoning_effort=low，流式，瞬时同发）

固定 prompt、`max_tokens=600`（实际输出 ~290 tok 含思考，全部 `finish=stop` 无截断），
单变量只改并发数，各阶梯成功率 100%：

| 并发 | 聚合 tok/s | 每路 tok/s(中位) | TTFT 中位/最大 | 引擎 running/排队 |
|---|---|---|---|---|
| 1 | 21.1 | 21.1 | 0.39s / 0.39s | 1 / 0 |
| 2 | 37.1 | 18.9 | 0.34s / 0.72s | 2 / 0 |
| 3 | 36.7 | 13.6 | 0.81s / 0.81s | 3 / 0 |
| 4 | 51.2 | 14.2 | 1.6s / 1.6s | 4 / 0 |
| **6** | **64.3（复跑 68.0）** | 11.8-12.0 | 0.93-1.1s / 1.1s | 6 / 0 |
| 8 | 56.9 | 11.3 | 0.89s / **20.3s** | 6 / **2 排队** |

结论：

- **引擎 `--max-num-seqs 6` 恰好是吞吐饱和点**（聚合 ~65-68 tok/s），无需调整。
- 多用户交互建议按 **4-6 路**规划：6 路时每路仍有 ~12 tok/s、准入请求 TTFT ~1s，体感流畅。
- **超过 6 路无收益**：第 7、8 个请求排队，排队者 TTFT 达 20s，聚合反降到 57 tok/s。
  Dify 侧 glm53 的并发上限建议填 **6**，超出的用户走排队而非崩溃。
- KV 池（192 万 fp8 token）远非瓶颈：6 路短会话峰值仅占用 18.8%；长上下文 Agent 会话也充裕。
- Agent 工具循环每轮输出更短且轮间有工具执行间隙，槽位周转比纯生成本测试更快。

## ⚠️ 关于"关闭思考"

**GLM-5.3 / 5.3-Flash 强制思考，不能关闭**（API 传 `thinking.type=disabled` 会被拒绝）。
只能控制推理程度：`reasoning_effort = low | high | max`（默认 max）。
dspark 注册的快捷参数为 `chat_template_kwargs: {"reasoning_effort":"low"}`，用于 ask/bench 与轻量工具轮；
Dify 场景建议在模型配置里用 low（工具调用更跟手），复杂规划任务可在请求侧改 high/max。

## Dify 兼容补丁：reasoning 字段双发（2026-09-15）

**症状**：Dify 构建模式智能体（Agent 应用 / Chatflow 的 Agent 节点，Function Calling 策略）
流式输出卡住、长时间不出字、随后报错并重试；普通短对话与非流式调用正常。
（构建模式 Agent 另有第二层 legacy functions 协议不兼容，见下一节，两补丁叠加后才完全可用。）

**根因**：基底镜像的 vLLM（`0.1.dev20051`）跟随上游变更，已把推理字段从
`reasoning_content` 更名为 `reasoning`（SSE `delta.reasoning`、非流式 `message.reasoning`）；
而 Dify 的 OpenAI-API-compatible 链路按 DeepSeek 惯例只读取 `reasoning_content`。
GLM-5.3 强制思考，思考阶段只发 `reasoning` 分片 → Dify 收不到任何可见内容 → 假死/超时重试。
与 MTP3 投机解码、启动参数无关（实测流式/非流式 tool_calls、并行工具、工具结果二轮、
guided JSON、tool_choice=required 全部正常，25/26 个 Dify 请求为 200，唯一 4xx 是旧模型名）。

**修复**：补丁两个协议文件的序列化逻辑，使每条推理输出**同时携带**
`reasoning` 与 `reasoning_content`（内容一致，无推理时不带冗余字段）：

- `vllm/entrypoints/openai/engine/protocol.py` 的 `DeltaMessage`（流式）
- `vllm/entrypoints/openai/chat_completion/protocol.py` 的 `ChatMessage`（非流式）

补丁源与 Dockerfile 在 spark1 `~/文档/glm53-dify-patch/`（spark2 在 `~/glm53-dify-patch/`），
两机各自构建，新标签 `spark-recipes/glm53-autoround-mtp3-pmu128:20260903-difyrc`；
`launch-dspark.sh` 的 `IMAGE` 默认值与 `dspark.sh` 注册表均已指向新标签。
重建：`docker build -t spark-recipes/glm53-autoround-mtp3-pmu128:20260903-difyrc ~/文档/glm53-dify-patch`。

> 注：网上流传的启动命令带 `--quantization auto_round`，**本镜像不支持**
> （量化注册表只有 gptq/gptq_marlin/awq_marlin），GPTQ 元数据适配 + Marlin 是必需的，勿照抄。

## Dify 兼容层：legacy functions 协议 shim（2026-09-15）

**症状**：上述 reasoning 双发上线后，Dify **构建模式 Agent**（dify-agent build mode，
shell_run/shell_wait 等沙箱工具）仍卡死：模型说"让我先看看环境状态"后无任何工具执行，
下一轮模型自己抱怨"no tool calls were actually made"，界面只见一串省略号点。

**根因（抓包实锤）**：Dify 构建模式的 stub 仍使用 OpenAI **已废弃的 legacy Functions 线协议**
（`python-requests` 直发，非官方 SDK）：

- 请求体形如 `{"functions":[...]}`，而本版 vLLM 已**完全移除** `functions` 字段
  （Pydantic 静默丢弃）→ 模型收不到工具定义，只能照系统提示词瞎猜，输出
  `<tool_call>` 纯文本/省略号，glm47 parser 误抽出垃圾工具名，Dify 侧永远等不到调用；
- 即使抽对了，响应格式也不匹配：legacy 客户端只认 `delta.function_call` 与
  `finish_reason="function_call"`（及历史消息 `role:"function"`），vLLM 只发 `tool_calls`。

**修复**：在 spark1 增加零依赖协议转换代理，不动模型镜像、不重启推理服务：

- 代码/服务文件：`~/文档/dify-functions-shim/`（`shim.py` 纯标准库 + systemd unit）
- 监听 `0.0.0.0:8001` → 上游 `127.0.0.1:8000`；systemd 单元 `dify-functions-shim`
  （`enable` 已设开机自启，`Restart=always`；日志 `/var/log/dify-functions-shim.log`）
- 请求侧：`functions`→`tools`、`function_call`→`tool_choice`、
  `assistant.function_call`+`role:"function"` 历史消息规范化为 `tool_calls`+`role:"tool"`
- 响应侧：流式 `delta.tool_calls`（按 index 聚合）→ `delta.function_call`，
  `finish_reason` 改名；非流式 `message.tool_calls[0]` → `message.function_call`
- 普通 `tools` 请求与 /health、/v1/models 原样透传，`reasoning(_content)` 双字段不受影响
- 已用抓包中的真实 Dify 请求体（10 个 functions / 13.9KB）回放验证，并验证
  legacy 二轮回路（function 结果回传→模型正常总结）与现代 tools 透传无回归

> 维护：`sudo systemctl {status,restart} dify-functions-shim`。
> legacy 协议不支持并行工具调用，shim 只保留首个（构建模式 Agent 本身为串行 shell 作业）。

## 开机自启编排（2026-09-15，双机真实重启验证通过）

两机断电/重启后 glm53 全自动恢复，无需人工按顺序拉服务：

| 单元 | 主机 | 职责 |
|---|---|---|
| `glm53-worker.service` | spark2 | 等 fabric `192.168.0.52` 就绪后 `launch rank 1`（oneshot, enabled, 失败 10s 重试） |
| `glm53-head.service` | spark1 | 等本机 fabric + worker 容器**连续 30s running**，再 sleep 20s 后 `launch rank 0`（enabled, 失败 15s 重试，StartLimitIntervalSec=0 无限重试） |
| `dify-functions-shim.service` | spark1 | 8001 协议 shim（Restart=always，enabled） |

- 单元文件留档在 spark1 `~/文档/glm53-autostart/`；前置脚本 `boot-wait-rank.sh`
  部署于两机配方目录（只用 CX7 fabric，不依赖管理网）。
- 容器重启策略已由 `no` 改为 **`unless-stopped`**：实测双机同时重启时 worker 会在
  master 空窗期分布式 init 崩溃一次，靠 Docker 自动拉起重连；head 侧稳定性门控 +
  单元重试兜底。显式 `docker stop` / `launch-dspark.sh stop` 不会触发重启。
- 实测恢复时间线（19:49:51 双机同时重启）：两机 ~50s 引导完成；worker 崩溃 1 次后
  19:55 自动重连；开机后约 **10.5 分钟** `/health` 返回 200，shim 8001 同步可用，
  legacy functions 工具调用回归通过。
- 手工管理仍用 `launch-dspark.sh rank|both|stop` 与 `dspark.sh`，与开机单元互不冲突
  （同一启动器，内部有 `docker rm -f` 单实例保护）。

## 与其它模型编排方式的区别（重要）

glm53 **不是 sparkrun 配方**，是裸 `docker run` 的双节点 mp 后端：

- rank1（spark2，headless）必须**先起**，约 20 秒后 rank0（spark1，API head）再起
- rendezvous：`192.168.0.51:29531`；NCCL 单 RoCE 口 `rocep1s0f0`，GID 3（沿用原配方，未用双口）
- 容器名两机相同：`spark_glm53_autoround_mtp3_pmu128`
- dspark.sh 已按注册表第 7 字段 `docker-direct` 分流：`use/start/stop/logs/check/status/monitor` 全自动
- 专用启动器：`~/spark-recipes/tp2_glm53flash_autoround_mtp3_pmu128/launch-dspark.sh`
  - `rank 0|1` 单起；`both` 按序起两机；`stop` 停两机
  - 可用环境变量临时覆盖：`PORT`、`MPORT`、`KVMEM`（KV 池字节数，默认 13.5GB）

## 部署踩坑记录（复现时务必注意）

1. **权重下载**：hf-mirror 对 `config.json.autoround.bak` 返回 403 会使整批下载失败；
   适配脚本不需要该文件，下载时加 `--exclude "*autoround.bak"`（或用 mihomo 代理直连 huggingface.co + `HF_HUB_DISABLE_XET=1`）。
2. **GPTQ 元数据改写**：跑配方目录的
   `adapt_autoround_to_gptq.py <snapshot> <目标-gptq>`，只改 config.json（哈希被配方固定校验），
   其余文件硬链接，不额外占盘。产物含 `GPTQ-SURGERY.json` 收据。幂等可重跑。
3. **首次启动必须先"预热"编译缓存**：容器内 flashinfer autotune / TileLang / Triton JIT
   在首次 warmup 时会出现显存峰值，直接用 13.5GB KV 启动会 `NV_ERR_NO_MEMORY` 杀掉 worker。
   启动器已把 `/root/.cache` 持久化到 `~/.cache/intel-glm53-vllm/root`；
   首部署流程：先 `KVMEM=8000000000 ./launch-dspark.sh both` 起一次（health 200 后停掉），
   再正常启动用满 13.5GB KV。缓存命中后 autotune 显示 "Saved 0 configs" 属正常。
4. **冷盘启动会卡死在 Marlin repack**：GPTQ→Marlin 零点打包（`unpack_cols` 等）单线程，
   若权重不在页缓存、主机可用内存又紧张（如连续 deepseek→qwen38→deepseek→glm53 切换后仅剩
   ~5GB 空闲），UVM 反向迁移会让该阶段从秒级变成**硬挂**（单核算满、GPU 0%、无报错、无 IO）。
   2026-09-15 实测：head 卡 25 分钟+，py-spy 显示专家计数器 `e` 停在 150/288 不动；
   停掉后两机空闲 95GB 重启，repack **不到 2 分钟**通过、8.5 分钟 health 200。
   - 判据：health 轮询一直 000，日志停在 `Using MarlinExperts` 超过 5 分钟；
     `py-spy dump --locals --pid <VLLM::Worker_TP0 的宿主PID>` 连续两次（间隔 60s）专家 `e` 相同即挂死。
   - 恢复：按序停 head→worker，`free -g` 确认两机各有几十 GB 空闲后，worker 先起、+20s、head；
     此时刚读入的权重页缓存是热的。**不要干等或只重启单机**（worker 已阻塞在 collective）。
   - 注意 dspark 的 health 等待为 25 分钟，超时报"失败"后**容器仍在继续**，可先看日志再决定是否重启；
     不要在容器停止很久、缓存被冲掉后单独"补跑 repack"。
   - 已内建预防（2026-09-15）：`dspark use/start glm53` 启动前若两机 available <40GiB 会自动
     `drop_caches` 腾页缓存（两机 sudo 免密）；health 等待期间每分钟打印引擎最后一行日志，
     停在 Marlin 阶段连续 5 分钟会直接告警疑似硬挂并给出 py-spy 判据，不用再盲等。
5. **两机已停用 earlyoom**（`systemctl disable --now earlyoom`）：122GiB 统一内存被模型占 ~99%，
   warmup 峰值时主机可用内存瞬时跌破 2%，earlyoom 的 prefer 列表含 vllm，会在 21:37:59 这种时刻
   误杀 worker（SIGTERM，9.9s 后退出，引擎报 "Executor failed"）。保留 16GB swap + 内核 OOM killer 兜底。
6. 切换停服后 dspark 会等两机 available 恢复到 25GiB 以上再启动；本模型运行中每机仅余 ~2-4GiB 属正常（spark1 121/122、spark2 118/122）。
7. **空载时 head 自主退出会演变成单端重启死循环（2026-09-16 事故）**：08:21 head 主进程在无请求、
   无 OOM/Xid/掉卡、内存正常的情况下 exit 0（原因无日志可考，容器被 rm 前未留存引擎日志）；
   `unless-stopped` 只单端拉 head，worker 是旧实例，两端 NCCL 世界不一致，约 10 分钟集合超时
   exit 1，08:21-09:03 自动重启 4 轮均失败，停摆约 50 分钟至人工双机成对重建。**教训：TP2 任何
   一端崩溃都不能只重启单机。** 已部署双机健康看门狗（见下节）自动完成成对恢复。
   另外运行中偶发的 `NVRM ... NV_ERR_NO_MEMORY mem_desc.c` 刷屏（常伴 TileLang JIT 编译，几秒一簇、
   引擎无 ERROR）是 UVM 试探性分配噪声，与崩溃无关，勿误判。

### 双机健康看门狗（2026-09-16 部署）

- 单元：`glm53-watchdog.timer`（spark1，每 2 分钟）+ oneshot service；脚本
  `~/spark-recipes/tp2_glm53flash_autoround_mtp3_pmu128/watchdog.sh`（源在 `~/文档/glm53-autostart/`）。
- 判据：head 容器 running 但 health 持续不通——曾健康过连续 3 次（≈6min）触发；从未健康则等容器
  age>15min 后连续 5 次（≈10min）触发（冷启动宽限，正常 7-10 分钟不干预）。head 容器不存在/非 running
  （人工 stop、切其它模型）一律不干预。
- 动作：双机 stop（head→worker，fabric 优先）→ 等两机 available≥25GiB → 不足 40GiB 自动 drop_caches
  → worker rank1 → 20s → head rank0，与人工铁律完全一致。
- 安全阀：两次恢复最短间隔 20 分钟；1 小时内恢复 4 次熔断 30 分钟并在 journal 告警（防硬件故障反复重启）。
- 互斥：与 `dspark use/start/stop/restart` 共用 flock（`~/.local/state/glm53-watchdog/lock`），
  人工操作期间看门狗跳过。
- 运维：`watchdog.sh status` 看状态；`journalctl -u glm53-watchdog -f` 看判定过程；
  手动触发恢复 `watchdog.sh recover`（`DRY_RUN=1` 演练）。

## 常用操作

```bash
cd ~/文档
./dspark.sh use glm53       # 从其它模型切换（自动停旧的+等内存+双机启动+等health）
./dspark.sh status          # 看两容器/API/KV
./dspark.sh logs            # head 容器日志（worker 日志在 spark2: docker logs spark_glm53_autoround_mtp3_pmu128）
./dspark.sh ask "…"         # 对话（自动带 reasoning_effort=low）
./dspark.sh bench           # 单流测速
./dspark.sh stop            # 停两机
```

## Dify 配置

- 模型供应商：OpenAI 兼容，endpoint **`http://<HEAD_IP>:8001/v1`**（经 legacy functions shim；
  直连 `:8000` 仅普通对话可用，构建模式 Agent 必须走 `:8001`）
- 模型名：`Intel/GLM-5.3-Flash-W4A16-AutoRound`
- 上下文长度填 1,048,576；开启工具调用/Function Calling
- 并发上限建议填 **6**（引擎 max_num_seqs=6 即吞吐饱和点；超出会在服务端排队，排队者等待可达 20s）
- 不支持关闭思考；如 Dify 传 `enable_thinking=false` 无效，请改传 `reasoning_effort=low`
