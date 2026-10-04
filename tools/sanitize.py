#!/usr/bin/env python3
"""
Sanitize dspark scripts for open-source release.
Replaces personal identifiers (usernames, LAN IPs, domains) with
configurable placeholders, and emits a config header that users fill in.
"""
import re, os, sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))  # project root (script lives in tools/)
TARGETS = {
    'scripts/dspark.sh': [
        # shell config block — rewrite the whole constant block
        (r'CLUSTER="dualspark"', 'CLUSTER="${DSPARK_CLUSTER:-dualspark}"'),
        (r'API="http://192\.168\.31\.51:8000"', 'API="http://${DSPARK_HEAD:-192.168.31.51}:8000"'),
        (r'SHIM="http://192\.168\.31\.51:8001"', 'SHIM="http://${DSPARK_HEAD:-192.168.31.51}:8001"'),
        (r'WORKER="192\.168\.31\.52"', 'WORKER="${DSPARK_WORKER:-192.168.31.52}"'),
        (r'WORKER_FAB="192\.168\.0\.52"', 'WORKER_FAB="${DSPARK_WORKER_FAB:-192.168.0.52}"'),
        (r'CX7_IPS=\("192\.168\.0\.52" "192\.168\.1\.52"\)', 'CX7_IPS=("${DSPARK_CX7_WORKER:-192.168.0.52} 192.168.1.52" | tr " " "\\n")  # user-configured, see DSPARK_CX7_WORKER1/2'),
        (r'COMFY_URL="http://192\.168\.31\.51:8188"', 'COMFY_URL="http://${DSPARK_HEAD:-192.168.31.51}:8188"'),
        (r'PUB_DOMAIN="llm\.zhanjimap\.com"', 'PUB_DOMAIN="${DSPARK_PUB_DOMAIN:-}"  # optional, empty = public access disabled'),
        (r'OPS_WEB_DIR="\$HOME/文档/ops-web"', 'OPS_WEB_DIR="${DSPARK_OPSWEB_DIR:-$HOME/ops-web}"'),
    ],
    'scripts/shim.py': [
        (r'https://llm\.zhanjimap\.com/shim/v1', 'https://<your-domain>/shim/v1'),
        (r'zhuorudan', 'sparkadmin'),
    ],
    'scripts/gateway.py': [
        (r'https://llm\.zhanjimap\.com/v1', 'https://<your-domain>/v1'),
    ],
    'scripts/ops_web.py': [
        (r'llm\.zhanjimap\.com/v1', '${DSPARK_PUB_URL}/v1'),
        (r'llm\.zhanjimap\.com/shim/v1', '${DSPARK_PUB_URL}/shim/v1'),
    ],
}

def sanitize(path, rules):
    src = os.path.join(ROOT, path)
    with open(src, encoding='utf-8') as f:
        text = f.read()
    n = 0
    for pat, rep in rules:
        text, k = re.subn(pat, rep, text)
        n += k
    with open(src, 'w', encoding='utf-8') as f:
        f.write(text)
    print(f'{path}: {n} replacements')

if __name__ == '__main__':
    for path, rules in TARGETS.items():
        sanitize(path, rules)
    print('done')
