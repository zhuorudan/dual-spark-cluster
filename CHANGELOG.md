# Changelog

All notable changes to this project are documented here.
Format based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## [0.1.0] - 2026-10-04

### Added
- `dspark.sh` cluster operator: model registry with one-key switching (`use`), 10-point
  health check, shard-split dual-node parallel download (hf-mirror + proxy dual channel,
  Xet-disabled), 200G fabric bidirectional rsync merge, cache consistency audit,
  TTFT/tok/s benchmark, CX7 jumbo-frame network test, mihomo proxy control, tunnel management
- Protocol shim (`shim.py`): OpenAI legacy `functions` ↔ `tool_calls` bidirectional
  translation for Dify workflow agents on modern vLLM; fixed model name `dspark`
  rewriting; bearer-key auth (hot-read, fail-closed)
- Unified model gateway (`gateway.py`): single OpenAI-compatible entry, model name always
  `dspark`, hot-swaps to active engine
- Ops web panel (`ops_web.py`): stdlib-only HTTP panel for model lifecycle and cluster
  dashboard, admin-LAN restricted
- Watchdog: 2-minute liveness, paired dual-node rebuild (worker → head), crash postmortem
  retention (7 days), circuit breaker (4 recoveries/hour → 30 min pause)
- Prebuilt recipes: DeepSeek-V4-Flash (156G bf16), Qwen3.8-27B NVFP4, GLM-5.3-Flash W4A16
  (with concurrency ladder benchmark: 65–68 tok/s aggregate at 6-way), MiniMax-M2.7 NVFP4,
  DeepSeek-V4-Flash-Vision
- Docs: full reproducible deployment guide (CX7 dual-link netplan, jumbo frames, HF shard
  splitting, fabric merge, frp public exposure), per-model notes, config reference
