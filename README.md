# Dual-Spark Cluster

> **Two NVIDIA DGX Spark boxes (GB10, 128 GB unified memory each) running large LLMs in
> tensor-parallel TP=2 — with one-command model switching, a watchdog that rebuilds both
> nodes in pairs, a Dify-compatible protocol shim, a unified OpenAI-style gateway, and a
> zero-dependency web ops panel.**

Home-lab grade, production habits. Built and battle-tested on a real dual-Spark cluster.

---

## What you get

| Component | File | What it does |
|---|---|---|
| **Cluster operator** | `scripts/dspark.sh` | One CLI for everything: model registry, one-key switching (`use`), health checks (10-point), download orchestration (shard-split across both nodes, hf-mirror + proxy dual-channel, auto fabric rsync merge), cache consistency, benchmark, monitoring, tunnel management |
| **Protocol shim** | `scripts/shim.py` | Bidirectional OpenAI legacy `functions` ↔ modern `tool_calls` translation. Newer vLLM dropped legacy `functions`, which silently breaks Dify workflow agents — this shim fixes that without touching vLLM. Also normalizes model name to `dspark` regardless of active engine |
| **Unified gateway** | `scripts/gateway.py` | Single OpenAI-compatible endpoint (`:8002`, model name always `dspark`) that routes to whichever engine is active. Client config never changes when you switch models |
| **Ops web panel** | `scripts/ops_web.py` | Zero-dependency (stdlib only) HTTP panel: dashboard, model lifecycle (query/download/register/switch/stop/unregister + dual-node cache consistency), task log. Runs on the admin LAN only |
| **Watchdog** | shipped via `dspark.sh` | 2-minute liveness timer; on head-up-but-health-down it rebuilds **both nodes in order** (worker → head), keeps crash postmortems for 7 days, circuit-breaks after 4 recoveries/hour |

Prebuilt model recipes included: **DeepSeek-V4-Flash** (156 GB bf16, 1M ctx, ~33 tok/s),
**Qwen3.8-27B NVFP4** (low-latency, 262k ctx), **GLM-5.3-Flash W4A16** (320B-A18B MoE,
1M ctx, measured 65–68 tok/s aggregate at 6-way concurrency), MiniMax-M2.7 NVFP4,
DeepSeek-V4-Flash-Vision.

## Why this exists

Two Sparks on a home network is a weirdly powerful thing: 244 GB unified memory, 200G
CX7 direct links, 240 W each. But the stock tooling assumes cloud. This project is the
missing "homelab → production" glue:

- **TP=2 across two boxes** with double 100G RoCE links (MTU 9000 jumbo frames)
- **Model switching in one command** (`dspark.sh use glm53`, ~2–10 min including health wait)
- **Dual-node weight sync** over the 200G fabric at ~1.8 GB/s
- **Crash resilience**: head-exits-while-worker-hangs used to wedge the pair in an NCCL
  mismatch loop for 50 minutes; the watchdog now detects and rebuilds the pair automatically
- **Dify compatibility**: workflow agents get their tool calls back via the shim
- **One public HTTPS entry** (frp + Caddy) with bearer-key auth, fail-closed

## Quick start

```bash
# 0) prerequisites: two DGX Sparks, sparkrun installed, docker, SSH between nodes
# 1) clone & configure (all personal settings are env-overridable, sane defaults inside)
git clone https://github.com/<you>/dual-spark-cluster.git
export DSPARK_HEAD=192.168.31.51          # your head node LAN IP
export DSPARK_WORKER=192.168.31.52        # worker LAN IP
export DSPARK_CX7_WORKER1=192.168.0.52    # CX7 fabric link 1 (worker side)
export DSPARK_CX7_WORKER2=192.168.1.52    # CX7 fabric link 2 (worker side)

# 2) sanity check (10-point inspection)
scripts/dspark.sh check

# 3) register & download a model (splits shards across both nodes automatically)
scripts/dspark.sh download deepseek-ai/DeepSeek-V4-Flash-0731 --host both

# 4) switch models
scripts/dspark.sh use deepseek

# 5) serve
curl http://$DSPARK_HEAD:8002/v1/chat/completions \
  -H "Authorization: Bearer $(scripts/dspark.sh tunnel key)" \
  -H "Content-Type: application/json" \
  -d '{"model":"dspark","messages":[{"role":"user","content":"hello"}]}'
```

Full deployment walkthrough (CX7 netplan, jumbo-frame verification, HF shard splitting,
fabric rsync merge, frp public exposure): [`docs/DEPLOY-CN.md`](docs/DEPLOY-CN.md) (中文).
Per-model notes with measured benchmarks: [`docs/`](docs/).

## Daily operations

```bash
scripts/dspark.sh status        # cluster overview (containers + API + watchdog)
scripts/dspark.sh monitor       # per-node GPU/unified-memory/load + vLLM KV usage
scripts/dspark.sh bench         # TTFT / tok/s benchmark
scripts/dspark.sh net           # CX7 dual-link + MTU9000 jumbo ping test
scripts/dspark.sh cache         # dual-node HF cache sizes & consistency
scripts/dspark.sh download org/name [--host both]   # shard-split parallel download
scripts/dspark.sh model-sync org/name               # 200G fabric bidirectional rsync
scripts/dspark.sh register <short> @reg/recipe      # add a model to the switcher
scripts/dspark.sh tunnel st     # public HTTPS entry full-path probe
scripts/dspark.sh watchdog st   # watchdog counters / postmortems
scripts/dspark.sh web url       # ops web panel URL (admin LAN only)
```

## Architecture

```
                    ┌────────────────────────── spark1 (head) ──────────────────────────┐
 clients ──HTTPS──▶ │ :8002 unified gateway ──▶ :8001 protocol shim ──▶ :8000 vLLM TP=2 │
 (OpenAI SDK,       │        :8200 ops web panel (admin LAN only)                       │
  Dify, anything)   └───────────────────────────────┬───────────────────────────────────┘
                    │                    2× 100G RoCE CX7 (MTU 9000)
                    ┌───────────────────────────────┴───────────────────────────────────┐
                    │ spark2 (worker): vLLM TP=2 rank 1 + mirror of HF weights cache    │
                    └───────────────────────────────────────────────────────────────────┘
```

## Documentation

- [`docs/DEPLOY-CN.md`](docs/DEPLOY-CN.md) — full reproducible deployment guide (Chinese)
- [`docs/model-glm53.md`](docs/model-glm53.md) — GLM-5.3-Flash W4A16: specs, concurrency ladder benchmark, quirks (forced reasoning)
- [`docs/model-deepseek.md`](docs/model-deepseek.md) / [`docs/model-qwen38.md`](docs/model-qwen38.md) — other recipes
- [`docs/ops-help.md`](docs/ops-help.md) — web panel help page

## License

MIT — see [LICENSE](LICENSE).

> Hardware context: developed on a personal 2× DGX Spark cluster. Not affiliated with
> NVIDIA; DGX Spark is a trademark of NVIDIA Corporation.
