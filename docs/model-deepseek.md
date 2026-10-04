# 模型说明：DeepSeek-V4-Flash 0731（deepseek）

集群的**默认通用旗舰模型**，中文能力强、上下文最长，适合日常对话、深度分析、长文档处理与 Agent 工具调用。

## 基本信息

| 项 | 值 |
|----|----|
| 脚本短名 | `deepseek`（默认） |
| sparkrun 配方 | `@official/deepseek-v4-flash-0731-b12x-dspark-vllm` |
| API 模型 ID（Dify 填这个） | `deepseek-ai/DeepSeek-V4-Flash-0731` |
| HF 仓库 | `deepseek-ai/DeepSeek-V4-Flash-0731` |
| 运行时 | vllm-distributed，TP=2 双机 |
| 容器镜像 | `ghcr.io/spark-arena/dgx-vllm-eugr-nightly-b12x:latest`（35.9GB） |
| 权重体积 | 156GB（bf16，48 个 safetensors 分片），**两机各一份** |
| 权重路径 | `~/.cache/huggingface/hub/models--deepseek-ai--DeepSeek-V4-Flash-0731` |

## 运行规格

- 上下文窗口：**1,048,576 token**（KV 容量 1,190,617，FP8 KV，block_size 256）
- 投机解码：DSpark，5 token
- gpu_mem 0.85，max_num_seqs 8，max_num_batched_tokens 8192
- NCCL 走双 CX7 100G RoCE（rocep1s0f0 + roceP2p1s0f0）

## 实测性能

| 指标 | 值 |
|------|----|
| TTFT 首 token | 0.33–0.39 s |
| 生成速度（非思考） | **30–33 tok/s** |
| 思考模式 | ~14.8 tok/s（短答案测试，reasoning_effort 默认 high） |
| 单卡统一内存占用 | ~113 / 122 GiB |

## 并发压测（2026-09-15 实测）

口径：同一短问答 prompt（五条要点，输出含少量 low 思考，约 220 token），`reasoning_effort=low`、流式、
瞬时同发，单变量只改并发数；每级全部成功（无截断/无报错）。脚本 `/tmp/conc_sweep_ds.py`。

| 并发 | 聚合吞吐 | 每路速度(中位) | TTFT 中位 / 最大 | 引擎 running / 排队峰值 | KV 池峰值 |
|---|---|---|---|---|---|
| 1 | 40.4 tok/s | 40.4 | 0.27s | 1 / 0 | 0.3% |
| 2 | 46.1 | 24.8 | 0.27s | 2 / 0 | 0.6% |
| 4 | 68.9 | 19.1 | 1.97s | 4 / 0 | 1.1% |
| 6 | 63.8 | 12.3 | 5.48s | 6 / 0 | 1.7% |
| **8** | **87.8（准入饱和点）** | 14.9 | 0.68s | 8 / 0 | 2.3% |
| 10 | 95.4* | 14.2 | 0.58s / **13.5s** | 8 / **2 排队** | 2.3% |

\* C10 聚合略高是因为排队请求在槽位释放后补位，但 2 个排队请求首 token 等 12–13 秒，交互不可接受。

结论：

- **Dify 并发上限填 8**（= `--max-num-seqs`）。第 9、10 个请求进引擎队列、TTFT 飙到 13 秒，应在 Dify 侧排队
- **交互甜点区是 2 路**：每路 24.8 tok/s、TTFT 0.27s；**独占单流最快 40.4 tok/s**（三模型中单流第一，MoE 激活专家少）
- 表中 C4/C6 的 TTFT（1.97/5.48s）与聚合（63.8）非单调是瞬时同发 + MoE 调度/思考长度抖动的单点现象，
  C8 已恢复（0.68s/87.8），不是稳定退化；对延迟敏感的多用户场景按 2–4 路规划
- KV 几乎不构成约束：短会话 8 路仅用 2.3%（池约 115 万 fp8 token）；1M 长上下文场景另算，单条长文就会占显著比例

## 切换与使用

```bash
~/文档/dspark.sh use deepseek     # 切换到本模型（自动停旧服务→启动→等健康）
~/文档/dspark.sh current          # 确认当前模型
~/文档/dspark.sh bench            # 测速
```

Dify（`http://192.168.31.8/`）模型供应商 **OpenAI-API-compatible**：

- 模型名称：`deepseek-ai/DeepSeek-V4-Flash-0731`
- endpoint：`http://<HEAD_IP>:8000/v1`
- 上下文长度 1048576；并发上限（CONCURRENCY）**8**（实测饱和点，见上方压测）；函数调用 ✅、流式函数调用 ✅
- 关思考（API 直调时）：`"chat_template_kwargs": {"thinking": false}`
- 思考强度：`reasoning_effort`（low/medium/high，默认 high）

## 注意事项

- 切换后**首次启动约 5–6 分钟**（加载 156GB 权重 + 编译 CUDA Graph）
- 双机内存均仅剩 ~9GB，运行期间无法再加载第二个模型
- 思考 token 计入输出额度，Dify 里"最大 token 上限"建议 ≥16384，否则可能思考未结束就被截断
