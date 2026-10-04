# Configuration reference

Every personal value in the scripts is env-overridable. Defaults are sane for a typical
two-node home cluster; override only what differs in yours.

## Environment variables

| Variable | Default | Purpose |
|---|---|---|
| `DSPARK_HEAD` | `<HEAD_IP>` | Head (spark1) node LAN IP — hosts `:8000` engine, `:8001` shim, `:8002` gateway, `:8200` panel |
| `DSPARK_WORKER` | `<WORKER_LAN_IP>` | Worker (spark2) LAN IP (management network) |
| `DSPARK_WORKER_FAB` | `192.168.0.52` | Worker CX7 fabric IP (preferred path for critical ops) |
| `DSPARK_CX7_WORKER1` | `192.168.0.52` | CX7 fabric link 1 (worker side) |
| `DSPARK_CX7_WORKER2` | `192.168.1.52` | CX7 fabric link 2 (worker side) |
| `DSPARK_PUB_DOMAIN` | *(empty)* | Optional public domain (frp + Caddy). Empty = public access disabled |
| `DSPARK_CLUSTER` | `dualspark` | sparkrun cluster name |
| `DSPARK_OPSWEB_DIR` | `$HOME/ops-web` | Where the ops panel code lives |

## Files on disk

| Path | Purpose |
|---|---|
| `~/.config/dspark/active` | Current active model short-name (written by `use`) |
| `~/.config/dspark/models.local.conf` | User-registered models (survives script upgrades) |
| `~/.config/dspark/vllm_api_key` | Bearer key shared by gateway/shim/glm53 engine (hot-read; `tunnel rotate` regenerates) |
| `~/.cache/huggingface/hub/` | Model weight cache — **must be identical on both nodes for TP=2** |
| `~/.local/state/glm53-watchdog/postmortem-*.log` | Crash postmortems (kept 7 days) |

## Key model registry format

`~/.config/dspark/models.local.conf`, one model per line:

```
short-name|@registry/recipe-or-docker-direct|HF-model-id|disable-thinking-JSON|description|image-override|orchestration
```

Or just use `scripts/dspark.sh register <short> @reg/recipe`.

## Port map (all on the head node)

| Port | Service | Auth |
|---|---|---|
| 8000 | Raw vLLM engine (debugging only) | key for glm53, open for sparkrun shapes |
| 8001 | Protocol shim (`functions`↔`tool_calls`) | bearer key, always |
| 8002 | Unified gateway (recommended client entry) | bearer key, always |
| 8200 | Ops web panel | none — **admin LAN only** (192.168.31.0/24 + fabric + localhost) |
