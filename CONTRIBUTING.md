# Contributing

Thanks for your interest! This project is maintained in spare time — keep PRs small and focused.

## Ground rules

1. **No personal data in commits** — run `tools/audit.sh` before committing. It fails on
   usernames, domains, or non-default LAN IPs outside documented config defaults.
2. **Test on real hardware** — this project only makes sense on actual DGX Spark pairs.
   If you changed switch/download/watchdog logic, say what you ran and on what cluster shape.
3. **Shell style** — match the existing `dspark.sh`: functions with `cmd_` prefix, user-facing
   output via `info`/`ok`/`warn` helpers, all paths/env overridable.
4. **Python components stay stdlib-only** — shim/gateway/ops_web must run on a bare system
   Python 3 with zero pip dependencies.

## Good first contributions

- More model recipes (see `dspark.sh register` format) with measured benchmarks
- Additional fault patterns for the troubleshooting table in `docs/DEPLOY-CN.md` §8
- English translation polish for the deploy guide
- Watchdog tuning for cluster shapes other than 2-node TP=2
