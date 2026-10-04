# 模型说明：Qwen3.8-27B-NVFP4 + DFlash2（qwen38）

**低延迟、高并发的快速模型**：27B 稠密模型 NVFP4 量化，配合 z-lab DFlash2 草稿模型做 8 token 投机解码。适合 Dify Agent 频繁工具调用、日常聊天等追求响应速度的场景；中文与代码能力优秀，但绝对推理深度不及 DeepSeek-V4-Flash。

## 基本信息

| 项 | 值 |
|----|----|
| 脚本短名 | `qwen38` |
| sparkrun 配方 | `@eugr/qwen3.8-27b-nvfp4-dflash2` |
| API 模型 ID（Dify 填这个） | `nvidia/Qwen3.8-27B-NVFP4` |
| 主模型 HF 仓库 | `nvidia/Qwen3.8-27B-NVFP4`（19 个文件） |
| 草稿模型 HF 仓库 | `z-lab/Qwen3.8-27B-DFlash2`（投机解码必需，随主模型一起预下载） |
| 运行时 | vllm-distributed，TP=2 双机 |
| 容器镜像 | **`dgx-vllm-qwen38-patched:local`（两机本地自建补丁镜像，见"部署备注"）**，基于 `dgx-vllm-eugr-nightly-b12x:20260913` 仅热修一个 warmup 文件 |
| 量化 | NVFP4（实测运行时统一内存占用约 94–99 / 122 GiB/机，压力小于 deepseek 的 113GiB） |
| 实测性能 | TTFT 0.14–0.16s，长文生成 **30.5 tok/s**（1262 token / 41.6s，DFlash2 投机已生效） |

## 运行规格（配方默认值）

- 上下文窗口：**262,144 token**（`--max-model-len 262144`）
- 投机解码：DFlash2 方法，**num_speculative_tokens=8**，草稿模型同样 TP=2
- KV cache：FP8；gpu_mem 0.70（实测可容纳 **306 万+ token** KV，262k 上下文/8 路并发余量充足）
- max_num_seqs 8，max_num_batched_tokens 16384
- 已启用：chunked prefill、async scheduling、prefix caching（命中前缀时重复问题更快）
- reasoning parser：`qwen3`；tool parser：`qwen3_xml`（已开 auto tool choice）

## 切换与使用

```bash
~/文档/dspark.sh use qwen38        # 一键切换（实测约 5–6 分钟，含镜像确认/双机加载/自动调优）
~/文档/dspark.sh current
~/文档/dspark.sh ask "用一句话解释RoCE"
~/文档/dspark.sh bench             # 测速（自动按本模型关思考）
```

Dify（`http://192.168.31.8/`）再添加一个 **OpenAI-API-compatible** 模型条目：

- 模型名称：`nvidia/Qwen3.8-27B-NVFP4`
- endpoint：`http://<HEAD_IP>:8000/v1`（与 deepseek 相同，**同一端口，同一时间只跑一个模型**）
- 上下文长度：`262144`
- 并发上限（CONCURRENCY）：**8**（实测饱和点，见下方压测）
- 函数调用 ✅、流式函数调用 ✅
- 关思考（API 直调时，参数名与 DeepSeek 不同）：
  `"chat_template_kwargs": {"enable_thinking": false}`

## 并发压测（2026-09-15 实测）

口径：同一短问答 prompt（五条要点，输出自然结束约 150–220 token），`enable_thinking=false`、流式、
瞬时同发，单变量只改并发数；每级全部成功（无截断/无报错）。脚本 `/tmp/conc_sweep_qwen.py`。

| 并发 | 聚合吞吐 | 每路速度(中位) | TTFT 中位 / 最大 | 引擎 running / 排队峰值 | KV 池峰值 |
|---|---|---|---|---|---|
| 1 | 26.2 tok/s | 26.2 | 0.13s | 1 / 0 | 1.2% |
| 2 | 42.0 | 21.2 | 0.12s / 0.33s | 2 / 0 | 2.3% |
| 4 | 74.5 | 19.6 | 0.39s | 4 / 0 | 4.6% |
| 6 | 75.5 | 15.4 | 0.40s / 0.62s | 6 / 0 | 7.0% |
| **8** | **89.7（饱和点）** | 13.1 | 0.42s | 8 / 0 | 9.3% |
| 10 | 84.2（不升反降） | 12.8 | 0.42s / **11.8s** | 8 / **2 排队** | 9.3% |

结论：

- **Dify 并发上限填 8**（= `--max-num-seqs`）。第 9、10 个请求进引擎队列，排队者首 token 等约 12 秒，
  且聚合吞吐反而下降，应让超额请求在 Dify 侧排队
- **交互甜点区是 4 路**：每路仍有 19.6 tok/s、TTFT 0.4s，聚合 74.5 tok/s；4→8 是用单路速度换总吞吐
- 显存/KV 完全不是约束：8 路短会话 KV 池仅用 9.3%（池共 306 万 fp8 token），瓶颈是计算槽位
- 对比 glm53（27B 更小 + DFlash2 投机）：本模型饱和点更高（8 vs 6）、单流更快（26 vs 21 tok/s）、
  TTFT 更低（0.13 vs 0.39s），适合多用户轻量并发；复杂 Agent 推理仍优先 glm53/deepseek

## 与 deepseek 如何取舍

| 场景 | 选择 |
|------|------|
| Dify Agent、工具调用、日常问答、追求快 | **qwen38** |
| 长文档/超长上下文（>26 万 token）、复杂推理、写作 | **deepseek** |
| 8 路并发高负载 | qwen38（模型小，KV 余量更大） |

## 部署备注

- 两个模型权重与镜像可**共存于磁盘**，切换无需重新下载
- 该配方节点数标注为"1 - unlimited"，本集群用 `--tp 2` 跨双机运行
- `dspark.sh` 注册表第 6 字段为本模型固定了镜像覆盖，`use/start` 会自动带上
  `--image dgx-vllm-qwen38-patched:local`，无需手工指定

### 为什么需要补丁镜像（2026-09-13 当日 nightly 的问题）

配方默认的通用镜像 `dgx-vllm-eugr-nightly:latest` 与 b12x 镜像各有一个致命问题，均无法直接跑本模型：

1. **通用 nightly**：内置 b12x 插件与当日 vLLM 不兼容（`cannot import name 'file_source_tensor'`）；
   回退标准 NCCL 路径后又在 GB10 统一内存上 `ibv_reg_mr_iova2 failed: Cannot allocate memory`，引擎初始化失败
2. **b12x nightly（20260913）**：NCCL/b12x 插件均正常，但 Qwen GDN 混合层的 triton **layernorm warmup 有断言 bug**
   （`qwen_triton_warmup.py` 中 `assert weight.shape == (N,)`，AssertionError 后整个引擎退出）

补丁方案：仅把该 warmup 中的 layer_norm 一项包成 `try/except` 跳过（仅影响首 token 的 JIT 预热时机，
**不影响推理正确性**——实测对话、30.5 tok/s 生成均正常），其余 kernel warmup 保留。
补丁源文件与 Dockerfile 留存于 spark1 `/tmp/qpatch/`（机器重装后可按下面重建）：

```bash
# spark1 上（基础镜像两机都有）
# 1) 从容器拷出 /usr/local/lib/python3.12/dist-packages/vllm/model_executor/warmup/qwen_triton_warmup.py
# 2) 将 "    _warm_layer_norm_kernel(device, gdn_config)" 一行替换为 try/except 包裹
# 3) 构建
#    FROM ghcr.io/spark-arena/dgx-vllm-eugr-nightly-b12x:latest
#    COPY qwen_triton_warmup.py /usr/local/lib/python3.12/dist-packages/vllm/model_executor/warmup/qwen_triton_warmup.py
docker build -t dgx-vllm-qwen38-patched:local /tmp/qpatch
# 4) 走 CX7 同步到 spark2（约 7 分钟，约 88MB/s）
docker save dgx-vllm-qwen38-patched:local | ssh 192.168.0.52 docker load
```

> 上游镜像修复后可改回配方默认镜像：删掉 `dspark.sh` 模型注册表 qwen38 行末尾第 6 字段即可。

### 权重与草稿模型

- 权重只在 spark1 经外网拉取一次（`HF_ENDPOINT=https://hf-mirror.com HF_HUB_DISABLE_XET=1`），
  再 rsync 走 CX7 同步 spark2，两机字节数一致
- 若启动报找不到 `z-lab/Qwen3.8-27B-DFlash2`：说明草稿模型未随主模型缓存，补下载
  `HF_ENDPOINT=https://hf-mirror.com HF_HUB_DISABLE_XET=1 hf download z-lab/Qwen3.8-27B-DFlash2`
