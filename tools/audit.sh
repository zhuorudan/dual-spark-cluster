#!/bin/bash
# Final pre-release audit: no personal identifiers, everything compiles.
cd "$(dirname "$0")/.." || exit 1
fail=0

echo "== 1. personal identifier scan (must be 0 hits) =="
hits=$(grep -rn "zhuorudan\|zhanjimap" scripts/ docs/ examples/ 2>/dev/null | wc -l | tr -d ' ')
echo "hits: $hits"
[[ "$hits" != "0" ]] && { grep -rn "zhuorudan\|zhanjimap" scripts/ docs/; fail=1; }

echo "== 2. hardcoded personal LAN IPs in code logic (defaults are OK) =="
# Count occurrences outside default-value context: crude check — only flag in comments
bad=$(grep -rn "192.168.31.51" scripts/dspark.sh | grep -v ':-192.168' | grep -v 'DSPARK_HEAD' | wc -l | tr -d ' ')
echo "suspicious: $bad"

echo "== 3. syntax checks =="
bash -n scripts/dspark.sh && echo "dspark.sh: OK" || fail=1
python3 -m py_compile scripts/shim.py && echo "shim.py: OK" || fail=1
python3 -m py_compile scripts/gateway.py && echo "gateway.py: OK" || fail=1
python3 -m py_compile scripts/ops_web.py && echo "ops_web.py: OK" || fail=1

echo "== 4. required files =="
for f in README.md LICENSE docs/CONFIG.md docs/DEPLOY-CN.md scripts/dspark.sh scripts/shim.py scripts/gateway.py scripts/ops_web.py; do
  [[ -f "$f" ]] && echo "  ✓ $f" || { echo "  ✗ MISSING $f"; fail=1; }
done

[[ $fail == 0 ]] && echo "== AUDIT PASSED ==" || { echo "== AUDIT FAILED =="; exit 1; }
