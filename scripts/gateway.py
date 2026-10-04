#!/usr/bin/env python3
"""
dSpark 统一模型网关（Dify/外部工具固定模型名入口）。

对外：
  - 永远只暴露一个固定模型名（默认 dspark），客户端一次配置后，
    在运维面板切换 deepseek/qwen38/glm53/自注册模型均无需改动模型名/地址；
  - 监听 0.0.0.0:8002，内网网段直连，公网经 frpc 隧道暴露
    （https://<your-domain>/v1 → frps:8090 → frpc → 本服务）；
  - 所有 /v1/* 请求强制 Bearer 鉴权，密钥为 ~/.config/dspark/vllm_api_key
    （热读取，轮换后即时生效）；key 文件缺失时 fail-closed 全部 503；
    /health 与 /v1/health 不鉴权（仅用于探活，公网 Caddy 不放行）。

对内（所有模型统一走协议 shim）：
  - 任何 active 模型（deepseek/qwen38/glm53/自注册）→ 127.0.0.1:8001
    dify-functions-shim → 127.0.0.1:8000 当前 active 引擎；
    shim 是模型无关的 legacy functions ↔ tool_calls 协议转换器（Dify 构建模式
    Agent 必走），非 functions 流量原样透传，故全模型统一经过无副作用；
  - 自动注入 ~/.config/dspark/vllm_api_key：glm53 引擎强制校验，其他 sparkrun
    引擎忽略 Authorization 头；
  - 请求体中的 model 字段改写为当前 active 的真实 served-model-name；
  - SSE 流式响应原样透传。

仅依赖 Python 标准库。活动模型信息来自 `dspark.sh models --json`，
按 ~/.config/dspark/active 文件 mtime 失效缓存（常态零子进程开销，切换后秒级生效）。
"""
import json
import os
import hmac
import subprocess
import sys
import threading
import time
import http.client
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

HOME = os.path.expanduser("~")
DSPARK = os.path.join(HOME, "文档", "dspark.sh")
ACTIVE_FILE = os.path.join(HOME, ".config", "dspark", "active")
VLLM_KEY_FILE = os.path.join(HOME, ".config", "dspark", "vllm_api_key")

LISTEN_HOST = "0.0.0.0"
LISTEN_PORT = int(os.environ.get("DGATEWAY_PORT", "8002"))
FIXED_MODEL = os.environ.get("DGATEWAY_MODEL", "dspark")
# glm53 等 docker-direct 模型走 shim（legacy functions 协议转换）
SHIM_PORT = 8001
ENGINE_PORT = 8000
UPSTREAM_HOST = "127.0.0.1"
READ_TIMEOUT = 900
CACHE_TTL = 30  # active 文件丢失/mtime 异常时的兜底缓存秒数

ALLOW_NETS = ("127.", "192.168.31.", "192.168.0.", "192.168.1.", "::1")

HOP_BY_HOP = {
    "connection", "keep-alive", "proxy-authenticate", "proxy-authorization",
    "te", "trailers", "transfer-encoding", "upgrade", "content-length",
    "content-encoding", "host", "authorization",
}

_lock = threading.Lock()
_cache = {"mtime": None, "ts": 0.0, "active": None}


def log(msg: str) -> None:
    print(f"{time.strftime('%F %T')} {msg}", file=sys.stderr, flush=True)


def _active_mtime():
    try:
        return os.stat(ACTIVE_FILE).st_mtime
    except OSError:
        return None


def get_active():
    """返回当前 active 模型 dict（name/model/kind），带 active 文件 mtime 缓存。"""
    mtime = _active_mtime()
    now = time.time()
    with _lock:
        if _cache["active"] is not None and _cache["mtime"] == mtime \
                and (mtime is not None or now - _cache["ts"] < CACHE_TTL):
            return _cache["active"]
    try:
        out = subprocess.run([DSPARK, "models", "--json"],
                             capture_output=True, text=True, timeout=20)
        rows = json.loads(out.stdout) if out.returncode == 0 else []
        active = next((r for r in rows if r.get("current")), None)
    except Exception as e:
        log(f"解析 active 模型失败: {e!r}")
        active = None
    with _lock:
        _cache.update(mtime=mtime, ts=now, active=active)
    return active


def _read_vllm_key():
    try:
        with open(VLLM_KEY_FILE, "r", encoding="utf-8") as f:
            return f.read().strip()
    except OSError:
        return None


def upstream_for(active):
    """统一返回 shim 端口（shim 上游即当前 active 引擎，模型无关）。"""
    return SHIM_PORT, True, "shim:8001"


class Handler(BaseHTTPRequestHandler):
    server_version = "DSparkGateway/1.0"
    protocol_version = "HTTP/1.1"

    # ---- 基础 ----
    def _gate(self):
        ip = self.client_address[0]
        if ip.startswith(ALLOW_NETS) or ip == "::1":
            return True
        body = json.dumps({"error": {"message": "forbidden: 仅内网网段可用"}}).encode()
        self.send_response(403)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)
        return False

    def _json(self, obj, status=200):
        body = json.dumps(obj, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _error(self, status, message, code=None):
        err = {"message": message}
        if code:
            err["type"] = "invalid_request_error"
            err["code"] = code
        self._json({"error": err}, status)

    def _auth(self):
        """/v1/* 强制 Bearer key；key 文件缺失 fail-closed。返回 True 放行。"""
        key = _read_vllm_key()
        if not key:
            self._error(503, "网关密钥未配置（~/.config/dspark/vllm_api_key 缺失），拒绝服务")
            return False
        presented = self.headers.get("Authorization", "")
        if not presented.startswith("Bearer "):
            self._error(401, "缺少 Bearer API key（请在运维面板「接入信息」查看 key）",
                        code="invalid_api_key")
            return False
        if not hmac.compare_digest(presented[7:].strip(), key):
            log(f"401 鉴权失败 来源={self.client_address[0]} 路径={self.path}")
            self._error(401, "无效的 API key", code="invalid_api_key")
            return False
        return True

    # ---- 上游连接 ----
    def _open_upstream(self, port, method, path, body, inject_key):
        conn = http.client.HTTPConnection(UPSTREAM_HOST, port, timeout=READ_TIMEOUT)
        headers = {k: v for k, v in self.headers.items()
                   if k.lower() not in HOP_BY_HOP}
        headers["Host"] = f"{UPSTREAM_HOST}:{port}"
        headers["Accept-Encoding"] = "identity"  # 禁 gzip，SSE/JSON 直接透传
        if inject_key:
            key = _read_vllm_key()
            if key:
                headers["Authorization"] = f"Bearer {key}"
        if body is not None:
            headers["Content-Length"] = str(len(body))
        conn.request(method, path, body=body, headers=headers)
        return conn

    def _relay(self, resp):
        """原样透传上游响应（含 SSE），用 chunked 避免缓冲。"""
        self.send_response(resp.status, resp.reason)
        for k, v in resp.getheaders():
            if k.lower() in HOP_BY_HOP:
                continue
            self.send_header(k, v)
        self.send_header("Transfer-Encoding", "chunked")
        self.end_headers()
        try:
            while True:
                buf = resp.read(65536)
                if not buf:
                    break
                self.wfile.write(f"{len(buf):X}\r\n".encode() + buf + b"\r\n")
                self.wfile.flush()
            self.wfile.write(b"0\r\n\r\n")
            self.wfile.flush()
        except (BrokenPipeError, ConnectionResetError):
            pass

    def _proxy(self, method, path, raw):
        active = get_active()
        if active is None:
            self._error(503, "当前没有在线/已切换的模型：请在运维面板先 use 一个模型")
            return
        port, need_key, label = upstream_for(active)
        body = raw
        if method == "POST" and raw:
            parsed, body = self._rewrite_model(raw, active["model"])
            if parsed is not None:
                log(f"{path} dspark -> {active['name']}({active['model']}) "
                    f"via {label} stream={parsed.get('stream')}")
        try:
            conn = self._open_upstream(port, method, self.path, body, need_key)
            resp = conn.getresponse()
        except (ConnectionRefusedError, OSError) as e:
            self._error(502, f"上游 {label} 不可达（模型可能正在切换/未启动）：{e}")
            return
        self._relay(resp)
        conn.close()

    @staticmethod
    def _rewrite_model(raw, real_model):
        """把请求体 model 改写为真实名；非 JSON 请求原样转发。"""
        try:
            obj = json.loads(raw.decode("utf-8"))
            if isinstance(obj, dict):
                obj["model"] = real_model
                return obj, json.dumps(obj, ensure_ascii=False).encode("utf-8")
        except Exception:
            pass
        return None, raw

    # ---- 路由 ----
    def do_GET(self):
        if not self._gate():
            return
        path = self.path.split("?", 1)[0]
        # 探活不鉴权（公网 Caddy 不放行该路径，仅内网/frpc 回环可达）
        if path in ("/health", "/v1/health"):
            try:
                self._proxy("GET", self.path, None)
            except (BrokenPipeError, ConnectionResetError):
                pass
            return
        if not self._auth():
            return
        try:
            if path == "/v1/models":
                self._serve_models()
            else:
                self._proxy("GET", self.path, None)
        except (BrokenPipeError, ConnectionResetError):
            pass
        except Exception as e:
            log(f"GET {path} ERROR: {e!r}")
            self._error(502, f"gateway error: {e}")

    def do_POST(self):
        if not self._gate():
            return
        if not self._auth():
            return
        length = int(self.headers.get("Content-Length") or 0)
        raw = self.rfile.read(length) if length else b""
        path = self.path.split("?", 1)[0]
        try:
            if path.startswith("/v1/"):
                self._proxy("POST", path, raw)
            else:
                self._error(404, "not found")
        except (BrokenPipeError, ConnectionResetError):
            pass
        except Exception as e:
            log(f"POST {path} ERROR: {e!r}")
            self._error(502, f"gateway error: {e}")

    def _serve_models(self):
        active = get_active()
        data = [{
            "id": FIXED_MODEL,
            "object": "model",
            "created": 0,
            "owned_by": "dspark-gateway",
        }]
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        if active:
            self.send_header("X-Active-Model", active.get("model", ""))
            self.send_header("X-Active-Name", active.get("name", ""))
        body = json.dumps({"object": "list", "data": data},
                          ensure_ascii=False).encode("utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, fmt, *args):
        pass


def main():
    active = get_active()
    log(f"dspark-model-gateway listening on {LISTEN_HOST}:{LISTEN_PORT} "
        f"固定模型名={FIXED_MODEL}；当前 active="
        f"{(active or {}).get('name', '（无）')} -> {(active or {}).get('model', '-')}")
    srv = ThreadingHTTPServer((LISTEN_HOST, LISTEN_PORT), Handler)
    srv.daemon_threads = True
    srv.serve_forever()


if __name__ == "__main__":
    main()
