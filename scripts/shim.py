#!/usr/bin/env python3
"""
OpenAI legacy `functions` <-> modern `tool_calls` 协议转换 shim（:8001）。

背景：
  Dify 构建模式（dify-agent）仍使用 OpenAI 已废弃的 legacy Functions 线协议：
    请求: {"functions": [...]}            （而非 {"tools": [{"type":"function",...}]}）
    响应: delta.function_call             （而非 delta.tool_calls）
          finish_reason="function_call"   （而非 "tool_calls"）
    历史: role="function" + name          （而非 role="tool" + tool_call_id）
  新版 vLLM 已移除 functions 兼容层，会静默丢弃 functions 字段，导致模型收不到
  工具定义、Agent 卡死。本进程在不改动 vLLM 的前提下做双向协议翻译，其余流量
  （含 reasoning / reasoning_content 双字段）原样透传。

模型名统一：
  与 :8002 网关一样对外接受固定模型名 dspark（DSHIM_MODEL 可覆盖），按
  dspark.sh active 模型改写成真实 served-model-name 再转发 :8000；真实模型名
  仍可继续使用（透传）。GET /v1/models 在引擎返回列表基础上注入 dspark 别名。
  shim 模型无关，上游 :8000 永远是当前 active 引擎，任何模型均可经过。

鉴权：
  shim 经 frpc 公网暴露（https://<your-domain>/shim/v1），且非 glm53 的
  sparkrun 引擎不校验 key，故 shim 自身对所有 /v1/* 强制 Bearer 鉴权
  （密钥 ~/.config/dspark/vllm_api_key，热读取；缺失 fail-closed 503）；
  /health 豁免。:8002 网关转发时已注入同一把 key，内网直连同款 key。

仅依赖 Python 标准库。
"""
import json
import os
import sys
import time
import hmac
import subprocess
import threading
import http.client
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

# shim systemd 以 root 运行，不能用 expanduser("~")（会解析成 /root）：
# 路径从脚本位置反推（…/文档/dify-functions-shim/shim.py → …/文档、HOME=…/），
# 可用 DSHIM_HOME 覆盖。
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DOC_DIR = os.path.dirname(SCRIPT_DIR)
HOME = os.environ.get("DSHIM_HOME") or os.path.dirname(DOC_DIR)
DSPARK = os.path.join(DOC_DIR, "dspark.sh")
ACTIVE_FILE = os.path.join(HOME, ".config", "dspark", "active")
VLLM_KEY_FILE = os.path.join(HOME, ".config", "dspark", "vllm_api_key")

UPSTREAM_HOST = "127.0.0.1"
UPSTREAM_PORT = 8000
LISTEN_HOST = "0.0.0.0"
LISTEN_PORT = 8001
FIXED_MODEL = os.environ.get("DSHIM_MODEL", "dspark")
CHAT_PATH = "/v1/chat/completions"
MODELS_PATH = "/v1/models"
READ_TIMEOUT = 900  # Agent 单轮可能很长（强制思考 + shell 等待）
CACHE_TTL = 30      # active 文件丢失/mtime 异常时的兜底缓存秒数

HOP_BY_HOP = {
    "connection", "keep-alive", "proxy-authenticate", "proxy-authorization",
    "te", "trailers", "transfer-encoding", "upgrade", "content-length",
    "content-encoding", "host",
}

_lock = threading.Lock()
_cache = {"mtime": None, "ts": 0.0, "active": None}


def log(msg: str) -> None:
    print(f"{time.strftime('%F %T')} {msg}", file=sys.stderr, flush=True)


def get_active():
    """返回当前 active 模型 dict（name/model/kind），按 active 文件 mtime 缓存。"""
    try:
        mtime = os.stat(ACTIVE_FILE).st_mtime
    except OSError:
        mtime = None
    now = time.time()
    with _lock:
        if _cache["active"] is not None and _cache["mtime"] == mtime \
                and (mtime is not None or now - _cache["ts"] < CACHE_TTL):
            return _cache["active"]
    try:
        env = os.environ.copy()
        env["HOME"] = HOME  # 服务以 root 运行时保证 dspark.sh 读到 sparkadmin 的配置
        out = subprocess.run([DSPARK, "models", "--json"],
                             capture_output=True, text=True, timeout=20, env=env)
        rows = json.loads(out.stdout) if out.returncode == 0 else []
        active = next((r for r in rows if r.get("current")), None)
    except Exception as e:
        log(f"解析 active 模型失败: {e!r}")
        active = None
    with _lock:
        _cache.update(mtime=mtime, ts=now, active=active)
    return active


def rewrite_model(obj: dict):
    """固定模型名 dspark -> 当前 active 真实模型 ID。返回 (改写?, 错误信息)。"""
    if obj.get("model") != FIXED_MODEL:
        return False, None
    active = get_active()
    real = (active or {}).get("model")
    if not real:
        return False, "当前无 active 模型（dspark.sh use 切换后重试）"
    obj["model"] = real
    return True, None


def read_key():
    try:
        with open(VLLM_KEY_FILE, "r", encoding="utf-8") as f:
            return f.read().strip()
    except OSError:
        return None


# ----------------------------- 请求侧改写 -----------------------------

def transform_request(obj: dict) -> bool:
    """把 legacy functions 请求就地翻译成现代 tools 协议。返回是否为 legacy 客户端。"""
    legacy = False

    funcs = obj.pop("functions", None)
    if funcs and not obj.get("tools"):
        obj["tools"] = [{"type": "function", "function": f} for f in funcs]
        legacy = True

    # {"function_call": "auto"|"none"|{"name": "x"}} -> tool_choice
    fc = obj.pop("function_call", None)
    if fc is not None:
        if fc == "none":
            obj["tool_choice"] = "none"
        elif isinstance(fc, dict) and fc.get("name"):
            obj["tool_choice"] = {"type": "function",
                                  "function": {"name": fc["name"]}}
        else:
            obj["tool_choice"] = "auto"

    # 历史消息：assistant.function_call -> tool_calls；role=function -> role=tool
    seq = 0
    pending_ids: list[str] = []
    for m in obj.get("messages", []):
        if not isinstance(m, dict):
            continue
        role = m.get("role")
        if role == "assistant" and isinstance(m.get("function_call"), dict):
            seq += 1
            call_id = f"call_legacy_{seq}"
            f = m["function_call"]
            m["tool_calls"] = [{
                "id": call_id,
                "type": "function",
                "function": {
                    "name": f.get("name", ""),
                    "arguments": f.get("arguments", "") or "",
                },
            }]
            m.pop("function_call", None)
            if m.get("content") is None:
                m["content"] = ""
            pending_ids.append(call_id)
        elif role == "function":
            m["role"] = "tool"
            m["tool_call_id"] = pending_ids.pop(0) if pending_ids else f"call_legacy_{seq}"
            m.pop("name", None)
            legacy = True

    return legacy


# ----------------------------- 响应侧改写 -----------------------------

def finish_legacy(finish):
    return "function_call" if finish == "tool_calls" else finish


def transform_chunk(obj: dict, state: dict) -> dict:
    """改写一个 SSE chat.completion.chunk。state 跨分片保存工具索引映射。"""
    for choice in obj.get("choices", []):
        delta = choice.get("delta") or {}
        tcs = delta.pop("tool_calls", None)
        if tcs:
            out = None
            for tc in tcs:
                idx = tc.get("index", 0)
                fn = tc.get("function") or {}
                if idx not in state:
                    state[idx] = True
                if idx > 0 and out is None:
                    continue  # legacy function_call 无法表达并行调用，仅保留首个
                piece = {}
                if fn.get("name"):
                    piece["name"] = fn["name"]
                    piece["arguments"] = ""
                if "arguments" in fn and fn["arguments"]:
                    piece["arguments"] = fn["arguments"]
                if piece:
                    out = piece
            if out is not None:
                delta["function_call"] = out
        choice["finish_reason"] = finish_legacy(choice.get("finish_reason"))
    return obj


def transform_full(obj: dict) -> dict:
    """改写非流式 chat.completion。"""
    for choice in obj.get("choices", []):
        msg = choice.get("message") or {}
        tcs = msg.pop("tool_calls", None)
        if tcs:
            fn = tcs[0].get("function") or {}
            msg["function_call"] = {
                "name": fn.get("name", ""),
                "arguments": fn.get("arguments", "") or "",
            }
        choice["finish_reason"] = finish_legacy(choice.get("finish_reason"))
    return obj


# ----------------------------- HTTP 处理 -----------------------------

class Handler(BaseHTTPRequestHandler):
    server_version = "FunctionsShim/1.0"
    protocol_version = "HTTP/1.1"

    def _open_upstream(self, method: str, path: str, body: bytes | None):
        conn = http.client.HTTPConnection(UPSTREAM_HOST, UPSTREAM_PORT,
                                          timeout=READ_TIMEOUT)
        headers = {k: v for k, v in self.headers.items()
                   if k.lower() not in HOP_BY_HOP}
        headers["Host"] = f"{UPSTREAM_HOST}:{UPSTREAM_PORT}"
        headers["Accept-Encoding"] = "identity"  # 避免 gzip，便于改写 SSE/JSON
        if body is not None:
            headers["Content-Length"] = str(len(body))
        conn.request(method, path, body=body, headers=headers)
        return conn

    def _relay_headers(self, resp, chunked: bool):
        self.send_response(resp.status, resp.reason)
        for k, v in resp.getheaders():
            if k.lower() in HOP_BY_HOP:
                continue
            self.send_header(k, v)
        if chunked:
            self.send_header("Transfer-Encoding", "chunked")
        self.end_headers()

    def _chunk(self, data: bytes):
        if data:
            self.wfile.write(f"{len(data):X}\r\n".encode() + data + b"\r\n")
            self.wfile.flush()

    def _end_chunked(self):
        self.wfile.write(b"0\r\n\r\n")
        self.wfile.flush()

    def _relay_plain(self, resp):
        """原样透传（非 SSE 或无需改写的响应）。"""
        self._relay_headers(resp, chunked=True)
        while True:
            buf = resp.read(65536)
            if not buf:
                break
            self._chunk(buf)
        self._end_chunked()

    def _json_error(self, status: int, message: str):
        payload = json.dumps({"error": {"message": message,
                                        "type": "invalid_request_error"}},
                             ensure_ascii=False).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def _auth(self) -> bool:
        """/v1/* 强制 Bearer key（公网暴露闸门）；key 文件缺失 fail-closed。"""
        key = read_key()
        if not key:
            self._json_error(503, "shim 密钥未配置（~/.config/dspark/vllm_api_key 缺失），拒绝服务")
            return False
        presented = self.headers.get("Authorization", "")
        if not presented.startswith("Bearer "):
            self._json_error(401, "缺少 Bearer API key（请在运维面板「接入信息」查看 key）")
            return False
        if not hmac.compare_digest(presented[7:].strip(), key):
            log(f"401 鉴权失败 来源={self.client_address[0]} 路径={self.path}")
            self._json_error(401, "无效的 API key")
            return False
        return True

    def _handle_chat(self, raw: bytes):
        try:
            obj = json.loads(raw.decode("utf-8"))
            if not isinstance(obj, dict):
                raise ValueError("not an object")
        except Exception:
            obj = None

        legacy = False
        if obj is not None:
            _, err = rewrite_model(obj)
            if err:
                self._json_error(503, f"固定模型名 {FIXED_MODEL} 不可用：{err}")
                return
            legacy = transform_request(obj)
        body = json.dumps(obj, ensure_ascii=False).encode("utf-8") if obj is not None else raw
        if obj is not None:
            log(f"chat legacy={legacy} stream={obj.get('stream')} model={obj.get('model')} "
                f"tools={len(obj.get('tools') or [])} msgs={len(obj.get('messages') or [])}")

        conn = self._open_upstream("POST", CHAT_PATH, body)
        resp = conn.getresponse()
        ctype = (resp.getheader("Content-Type") or "").lower()

        if not legacy or resp.status != 200:
            self._relay_plain(resp)
            conn.close()
            return

        if obj.get("stream") or "text/event-stream" in ctype:
            self._relay_headers(resp, chunked=True)
            state: dict = {}
            while True:
                line = resp.readline()
                if not line:
                    break
                if line.startswith(b"data:"):
                    payload = line[5:].strip()
                    if payload == b"[DONE]":
                        self._chunk(b"data: [DONE]\n\n")
                        continue
                    try:
                        out = transform_chunk(json.loads(payload), state)
                        self._chunk(
                            b"data: " + json.dumps(out, ensure_ascii=False).encode() + b"\n\n")
                    except Exception:
                        self._chunk(b"data: " + payload + b"\n\n")
                else:
                    self._chunk(line)
            self._end_chunked()
        else:
            data = resp.read()
            try:
                out = transform_full(json.loads(data.decode("utf-8")))
                data = json.dumps(out, ensure_ascii=False).encode("utf-8")
                self.send_response(resp.status, resp.reason)
                self.send_header("Content-Type", resp.getheader("Content-Type")
                                 or "application/json")
                self.send_header("Content-Length", str(len(data)))
                self.end_headers()
                self.wfile.write(data)
                self.wfile.flush()
            except Exception:
                self._relay_headers(resp, chunked=True)
                self._chunk(data)
                self._end_chunked()
        conn.close()

    def _handle_json_post(self, path: str, raw: bytes):
        """/v1 下非 chat 的 JSON 接口：仅做固定模型名改写后透传。"""
        try:
            obj = json.loads(raw.decode("utf-8"))
            if not isinstance(obj, dict):
                raise ValueError("not an object")
        except Exception:
            obj = None
        body = raw
        if obj is not None:
            _, err = rewrite_model(obj)
            if err:
                self._json_error(503, f"固定模型名 {FIXED_MODEL} 不可用：{err}")
                return
            body = json.dumps(obj, ensure_ascii=False).encode("utf-8")
        conn = self._open_upstream("POST", path, body)
        self._relay_plain(conn.getresponse())
        conn.close()

    def do_POST(self):
        length = int(self.headers.get("Content-Length") or 0)
        raw = self.rfile.read(length) if length else b""
        path = self.path.split("?", 1)[0]
        try:
            if path.startswith("/v1/") and not self._auth():
                return
            if path == CHAT_PATH:
                self._handle_chat(raw)
            elif path.startswith("/v1/"):
                self._handle_json_post(path, raw)
            else:
                conn = self._open_upstream("POST", self.path, raw)
                self._relay_plain(conn.getresponse())
                conn.close()
        except (BrokenPipeError, ConnectionResetError):
            pass
        except Exception as e:
            log(f"POST {path} ERROR: {e!r}")
            try:
                self._json_error(502, f"shim error: {e}")
            except Exception:
                pass

    def _serve_models(self):
        """代理引擎 /v1/models 并注入固定别名 dspark（真实模型名同时保留）。"""
        conn = self._open_upstream("GET", MODELS_PATH, None)
        resp = conn.getresponse()
        data = resp.read()
        active = get_active()
        active_name = (active or {}).get("name") or ""
        active_model = (active or {}).get("model") or ""
        if resp.status == 200:
            try:
                doc = json.loads(data.decode("utf-8"))
                ids = {d.get("id") for d in doc.get("data", [])}
                if FIXED_MODEL not in ids:
                    doc.setdefault("data", []).insert(0, {
                        "id": FIXED_MODEL, "object": "model", "created": 0,
                        "owned_by": "dspark-shim",
                    })
                data = json.dumps(doc, ensure_ascii=False).encode("utf-8")
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("X-Active-Model", active_model)
                self.send_header("X-Active-Name", active_name)
                self.send_header("Content-Length", str(len(data)))
                self.end_headers()
                self.wfile.write(data)
                conn.close()
                return
            except Exception as e:
                log(f"/v1/models 注入别名失败，原样透传: {e!r}")
        self.send_response(resp.status, resp.reason)
        self.send_header("Content-Type", resp.getheader("Content-Type") or "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)
        conn.close()

    def do_GET(self):
        path = self.path.split("?", 1)[0]
        try:
            if path in ("/health", "/v1/health"):
                conn = self._open_upstream("GET", self.path, None)
                self._relay_plain(conn.getresponse())
                conn.close()
                return
            if path.startswith("/v1/") and not self._auth():
                return
            if path == MODELS_PATH:
                self._serve_models()
            else:
                conn = self._open_upstream("GET", self.path, None)
                self._relay_plain(conn.getresponse())
                conn.close()
        except (BrokenPipeError, ConnectionResetError):
            pass
        except Exception as e:
            log(f"GET {path} ERROR: {e!r}")

    def log_message(self, fmt, *args):
        pass  # 静默默认访问日志（已有结构化日志）


def main():
    srv = ThreadingHTTPServer((LISTEN_HOST, LISTEN_PORT), Handler)
    srv.daemon_threads = True
    log(f"functions-shim listening on {LISTEN_HOST}:{LISTEN_PORT} "
        f"-> http://{UPSTREAM_HOST}:{UPSTREAM_PORT}")
    srv.serve_forever()


if __name__ == "__main__":
    main()
