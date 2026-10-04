# eugr/spark-vllm-docker 引流作战手册

> 数据：112 个 open issue 中 **57 个**与 dual-spark-cluster 已验证解法直接相关。
> 完整匹配数据见 `eugr-issues-match.json`。本文档是回复优先级和话术。

## 回复原则

1. **先解题，再留链**：回复必须包含真实可操作的内容（命令/配置/数据），链接只放最后一句。eugr 社区质量高，纯广告会被踩。
2. **每次只回 3-5 个**，间隔 2-3 天，避免被认出是推广模式。
3. **优先 0 回复和少回复的 issue**：楼主还在等答案，回复即被采纳为 solution；热帖（10+回复）里已有共识，插话容易被无视。
4. **英文回复**。语气：另一个踩过同坑的人。

## 第一批（最高优先级，0-2回复 + 你的解法直接命中）

### #420 Docker image consistency across cluster nodes（0回复）
你的解法：`dspark.sh cache` 双机一致性审计 + `model-sync` fabric 双向 rsync
```
We hit the same issue (image/weight drift between nodes breaks TP=2 in subtle ways).
Our open-source tooling does an automated consistency audit — it diffs size/file-count/
hash of the HF cache on both nodes and reports mismatches:

  dspark.sh cache        # dual-node HF cache JSON: size/files/consistency/registered models
  dspark.sh model-sync <org/name>   # 200G fabric bidirectional rsync (~1.8 GB/s)

Full flow: https://github.com/zhuorudan/dual-spark-cluster (MIT). The consistency
check is part of `dspark.sh check` (10-point inspection) so it runs on every boot.
```

### #416 prefix-cache reuse with concurrent long-context GLM-5.3（0回复，问的就是PMU128）
你的实测数据：866 prompt 命中 768（PMU128 粒度 6×128），cached_tokens 在 usage 里可观测
```
We ran GLM-5.3-Flash W4A16 with PMU128 (prefix matching unit = 128) on a 2x Spark pair.
Measured hit rate over 866 prompts: 768 cache hits (~89%), and the hit count is
observable in `usage.prompt_tokens_details.cached_tokens` — no need for external tooling.

Benchmarks + config (1M context, KV pool 1.92M tokens fp8) in our model notes:
https://github.com/zhuorudan/dual-spark-cluster/blob/main/docs/model-glm53.md
```

### #405 How to increase max context len for qwen3.8 cluster（2回复）
你的解法：glm53 1M 上下文完整配置 + KV 池预算方法（fp8 KV，池大小可算）
```
For GLM-5.3-Flash we serve 1M context (max_model_len=1048576) on 2x Spark TP=2 with
fp8 KV — the KV pool ends up at ~1.92M tokens (13.5 GB/node). The math: KV pool bytes ≈
2 (K+V) × layers × kv_heads × head_dim × kv_dtype_bytes × tokens. Budget your
gpu_memory_utilization so weights + activation + this pool fit in 128GB unified memory.

Full config in https://github.com/zhuorudan/dual-spark-cluster/blob/main/docs/model-glm53.md
```

### #404 b12x: engine wedges in cudaStreamSynchronize after NV_ERR_NO_MEMORY（1回复，正是你的坑）
你的解法：这是 UVM warmup 噪声 vs 真挂的判别 + 看门狗
```
We documented this exact pattern. Two different things happen with NV_ERR_NO_MEMORY
on GB10: (a) TileLang JIT/warmup makes speculative allocations that log clusters of
these every few seconds while the engine stays healthy — harmless; (b) a real wedge
where the container stays running but /health stops responding.

For (b) we run a watchdog that detects "container running + health failing for >6min
after having been healthy" and rebuilds BOTH nodes in order (worker first, then head) —
restarting a single node recreates the NCCL mismatch loop and wedges harder.

Watchdog + postmortem logs (7-day retention): https://github.com/zhuorudan/dual-spark-cluster
```

### #383 hf-download.sh no control over --max-workers（0回复）
你的解法：`dspark.sh download` 全参数化 + 双机分片
```
Our download orchestration exposes --workers N (default 8) plus shard-splitting across
nodes: `dspark.sh download org/name --host both` automatically halves the safetensors
shard list between the two Sparks (hf-mirror + HF_HUB_DISABLE_XET=1 by default), then
merges over the CX7 fabric at ~1.8 GB/s. Also takes a recipe name and resolves the
weight repo automatically.

https://github.com/zhuorudan/dual-spark-cluster
```

### #372 Serve locally stored model without contacting HF（7回复，但你的方案更彻底）
```
We serve entirely offline: weights live in the dual-node HF cache and `dspark.sh use
<model>` never touches the network (only downloads do, explicitly). Our download tool
also accepts local paths and does the fabric merge. If your goal is air-gapped serving,
the gateway/shim stack runs fully offline once weights are mirrored:

https://github.com/zhuorudan/dual-spark-cluster
```

## 第二批（3-9回复的热帖，回帖要短而准）

- **#424** Solo Spark hard hangs on qwen38 startup → 回复：先 `dspark.sh status` 分辨"容器活着但health不通"（wedge）vs 容器退出（真崩）——前者用成对重建，附 watchdog 链接
- **#352** B12X inference hangs in block_fp8_linear → 你的 #404 同类经验 + 看门狗
- **#257** restart-on-failure leaves cluster orphaned → 这就是你的看门狗立项原因，讲 9/16 事故（50分钟停摆）
- **#225** high CPU when idle → 排查表：nvidia-smi 94% 是 vLLM 常驻编译态正常现象；真空闲看 monitor 的 KV/queue
- **#196** tool call leak → shim 的 parser 处理（glm47 tool-call-parser + glm45 reasoning-parser 的组合调法）
- **#388** qwen38 solo exhausts memory and may reboot → earlyoom 停用 + swap + OOM killer 策略，附内存预算表方法

## 不回的（避雷）

- 纯 feature request（minimax-m2.5 support、portainer compose）——不是你的战场
- 模型选择咨询类（#138 tuning advice）——需要真跑过那个模型才硬气
- eugr 本人已深度回复的（#114、#327）——专家在场时插话收益低

## 频率计划

| 周 | 动作 |
|---|---|
| 第1周 | #420、#416、#405、#404（4个，0-2回复高转化位）|
| 第2周 | #383、#372、#424、#257 |
| 第3周 | #225、#388、#352、#196 |
| 之后 | 每周监控新 issue（关键词：nccl/wedge/NV_ERR/consistency/download），命中即回 |

## 预期收益

- 57 个匹配 issue ≈ 57 个**精准用户触点**（每个都是双 Spark 集群的真实运维者）
- 按 10% 点击、5% star 转化：**+3~6 star/月** 起步，且这些 star 质量极高（是真用户不是路人）
- 被采纳为 solution 的回复会长期挂在 issue 顶部 = 持续被动引流
