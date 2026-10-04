#!/usr/bin/env python3
"""双 DGX Spark 运维 Web 面板 —— dspark.sh 的 HTTP 薄外壳

设计原则：
  - 不重写任何运维逻辑，所有动作都是 dspark.sh 子命令的白名单映射；
  - 零三方依赖（仅 Python 3.12 标准库），vLLM / ComfyUI 两种形态下都能跑；
  - 免密（家庭内网使用），但仅接受管理网 192.168.31.0/24 / fabric / 本机来源，
    其他来源一律 403；
  - 由 systemd 单元 dspark-opsweb.service 托管（开机自启 + 崩溃自动重启），
    dspark.sh web up|down|st 是 systemctl 的封装；
  - 长任务（切换/启停/key 轮换，最长 25 分钟）走 job + SSE 实时输出，写操作全局串行。
"""
import json
import os
import re
import shlex
import subprocess
import threading
import time
import uuid
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse, parse_qs, unquote

# ---------- 常量 ----------
HOME = os.path.expanduser("~")
DSPARK = os.path.join(HOME, "文档", "dspark.sh")
WEB_DIR = os.path.join(HOME, "文档", "ops-web")
JOB_DIR = os.path.join(HOME, ".local", "state", "dspark-opsweb", "jobs")
LOG_FILE = os.path.join(HOME, ".local", "state", "dspark-opsweb", "opsweb.log")
PORT = int(os.environ.get("OPSWEB_PORT", "8200"))

# 允许访问的来源网段（管理网 192.168.31.x、CX7 fabric 两段、本机）
ALLOW_NETS = ("127.", "192.168.31.", "192.168.0.", "192.168.1.", "::1")

# 文档根（相对该目录的 md 都可列举/读取，禁止越界）
DOC_ROOTS = [
    ("集群运维主文档", os.path.join(HOME, "文档"), ["README.md"]),
    ("运维面板帮助", WEB_DIR, ["HELP.md"]),
    ("模型说明", os.path.join(HOME, "文档", "models"), None),
    ("ComfyUI H3 项目", os.path.join(HOME, "文档", "dgxspark_comfyui_minimax_h3"),
     ["README.md", "DEPLOYMENT.md", "NEW_SPARK_DEPLOY.md", "WORKFLOWS.md",
      "BENCHMARK.zh-CN.md", "I2V.md"]),
]

# 简单写操作白名单：action -> (arg 正则, argv 构造 kind)；复杂动作（模型生命周期）走 build_payload_argv
WRITE_RULES = {
    "switch":  (r"^(glm53|comfy|comfyui|h3|glm|vllm)$", "switch"),
    # use 的短名可能来自用户注册表，放宽为短名格式，存在性由 dspark.sh 自己校验
    "use":     (r"^[a-z0-9][a-z0-9-]{0,20}$", "use"),
    "service": (r"^(start|stop|restart)$", "service"),
    "comfy":   (r"^(down|stop)$", "comfy"),
    "watchdog":(r"^(on|off|recover)$", "watchdog"),
    "proxy":   (r"^(up|down)$", "proxy"),
    "tunnel":  (r"^(rotate)$", "tunnel"),
}
# 这些写操作互斥（动容器/形态）；register/unregister/download/model-sync 不在此列，可与切换并行
SERIAL_ACTIONS = set(WRITE_RULES)

# 参数校验（复杂动作）
RE_NAME   = re.compile(r"^[a-z0-9][a-z0-9-]{0,20}$")
RE_RECIPE = re.compile(r"^@(official|eugr)/[A-Za-z0-9._-]{1,60}$")
RE_REPO   = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,60}/[A-Za-z0-9_.-]{1,60}$")
# download 允许直接给配方名（dspark.sh 会 sparkrun show 解析成权重仓库），含 transitional 源
RE_RECIPE_DL = re.compile(r"^@(official|eugr|local|sparkrun-transitional)/[A-Za-z0-9._-]{1,60}$")
RE_KW     = re.compile(r"^[A-Za-z0-9_.@/-]{0,40}$")
RE_GLOB   = re.compile(r"^[A-Za-z0-9_.*?/\[\]-]{1,120}$")

# 短读操作白名单（同步执行）：key -> (静态 argv, timeout)；带参命令见 build_short_argv
SHORT_CMDS = {
    "status":           (["status"], 60),
    "current":          (["current"], 60),
    "models":           (["models"], 60),
    "models-json":      (["models", "--json"], 20),
    "access-json":      (["access", "--json"], 25),
    "cache":            (["cache"], 180),
    "check":            (["check"], 120),
    "net":              (["net"], 90),
    "monitor":          (["monitor"], 60),
    "proxy-status":     (["proxy", "st"], 60),
    "proxy-test":       (["proxy", "test"], 90),
    "watchdog-status":  (["watchdog", "st"], 60),
    "watchdog-pm":      (["watchdog", "pm"], 60),
    "comfy-status":     (["comfy", "st"], 60),
    "tunnel-status":    (["tunnel", "st"], 90),
    "tunnel-url":       (["tunnel", "url"], 30),
    "recipes":          (["recipes"], 90),
    "recipe-show":      (["recipe-show"], 60),
}


def build_short_argv(key, qs):
    """带参只读命令；不合法返回 None"""
    if key == "recipes":
        kw = qs.get("kw", [""])[0].strip()
        if not RE_KW.match(kw):
            return None
        return ["recipes"] + ([kw] if kw else [])
    if key == "recipe-show":
        arg = qs.get("arg", [""])[0].strip()
        return ["recipe-show", arg] if RE_RECIPE.match(arg) else None
    return None

# ---------- 任务管理 ----------
os.makedirs(JOB_DIR, exist_ok=True)
jobs = {}                     # id -> {id, action, arg, state, rc, started, argv}
jobs_lock = threading.Lock()
write_serial = threading.Lock()   # 所有写操作串行（dspark 自身还有 flock，双保险）

# 继承一个干净但完整的运行环境（docker 组、~/.local/bin、SSH/fabric 配置）
PROC_ENV = dict(os.environ)
PROC_ENV["PATH"] = HOME + "/.local/bin:" + PROC_ENV.get("PATH", "")
PROC_ENV.setdefault("LANG", "C.UTF-8")


def build_argv(action, p):
    """白名单动作 + JSON payload -> dspark.sh argv；不合法返回 None。
    p 为 dict（兼容旧客户端的 {action, arg}）。"""
    if action in WRITE_RULES:
        arg = str(p.get("arg", "") or "").strip()
        pattern, kind = WRITE_RULES[action]
        if not re.match(pattern, arg):
            return None
        if kind == "switch":   return [DSPARK, "switch", arg]
        if kind == "use":      return [DSPARK, "use", arg]
        if kind == "service":  return [DSPARK, arg]
        if kind == "comfy":    return [DSPARK, "comfy", "down"]
        if kind == "watchdog": return [DSPARK, "watchdog", arg]
        if kind == "proxy":    return [DSPARK, "proxy", arg]
        if kind == "tunnel":   return [DSPARK, "tunnel", arg]
        return None
    # ---- 模型生命周期 ----
    if action == "register":
        name = str(p.get("name", "")).strip()
        recipe = str(p.get("recipe", "")).strip()
        nothink = str(p.get("nothink", "") or "").strip()
        if not RE_NAME.match(name) or not RE_RECIPE.match(recipe):
            return None
        argv = [DSPARK, "register", name, recipe]
        if nothink:
            try:
                obj = json.loads(nothink)
            except Exception:
                return None
            if not isinstance(obj, dict):
                return None
            argv.append(json.dumps(obj, ensure_ascii=False))
        return argv
    if action == "unregister":
        name = str(p.get("name", "")).strip()
        return [DSPARK, "unregister", name] if RE_NAME.match(name) else None
    if action == "model-sync":
        repo = str(p.get("repo", "")).strip()
        return [DSPARK, "model-sync", repo] if RE_REPO.match(repo) else None
    if action == "download":
        repo = str(p.get("repo", "")).strip()
        if not (RE_REPO.match(repo) or RE_RECIPE_DL.match(repo)):
            return None
        host = str(p.get("host", "spark1")).strip()
        if host not in ("spark1", "spark2", "both"):
            return None
        try:
            workers = int(p.get("workers", 8))
        except (TypeError, ValueError):
            return None
        if not 1 <= workers <= 16:
            return None
        argv = [DSPARK, "download", repo, "--host", host, "--workers", str(workers)]
        if p.get("no_mirror"):
            argv.append("--no-mirror")
        if p.get("proxy"):
            argv.append("--proxy")
        includes = p.get("include") or []
        if not isinstance(includes, list):
            return None
        for g in includes[:8]:
            g = str(g)
            if not RE_GLOB.match(g):
                return None
            argv += ["--include", g]
        return argv
    return None


def job_label(action, p):
    """任务列表里展示的目标参数"""
    if action in ("register", "unregister"):
        return str(p.get("name", ""))
    if action in ("download", "model-sync"):
        return str(p.get("repo", ""))
    return str(p.get("arg", "") or "")


def job_worker(jid, argv, assume_yes, serial):
    job = jobs[jid]
    env = dict(PROC_ENV)
    if assume_yes:
        env["DSPARK_ASSUME_YES"] = "1"
    job["state"] = "waiting" if serial else "running"
    if serial:
        job["state"] = "waiting"
        with write_serial:
            _run_job(jid, argv, env)
    else:
        job["started"] = time.time()
        _run_job(jid, argv, env)


def _run_job(jid, argv, env):
    job = jobs[jid]
    job["state"] = "running"
    job["started"] = time.time()
    logf = open(os.path.join(JOB_DIR, jid + ".log"), "wb")
    try:
        p = subprocess.Popen(argv, stdin=subprocess.DEVNULL,
                             stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                             cwd=os.path.dirname(DSPARK), env=env, close_fds=True)
        job["pid"] = p.pid
        while True:
            chunk = p.stdout.readline()
            if not chunk:
                break
            logf.write(chunk); logf.flush()
        p.wait()
        p.stdout.close()
        job["rc"] = p.returncode
    except Exception as e:
        logf.write(("任务启动失败: %s\n" % e).encode())
        job["rc"] = -1
    finally:
        logf.close()
        job["state"] = "done"
        job["finished"] = time.time()


def job_start(action, payload):
    argv = build_argv(action, payload)
    if argv is None:
        return None
    jid = uuid.uuid4().hex[:12]
    # watchdog off/recover、tunnel rotate 的交互确认由网页二次确认承担，脚本侧免交互
    assume = action in ("watchdog", "tunnel")
    serial = action in SERIAL_ACTIONS
    jobs[jid] = {"id": jid, "action": action, "arg": job_label(action, payload),
                 "state": "queued", "rc": None, "started": None, "finished": None,
                 "serial": serial,
                 "argv": " ".join(shlex.quote(a) for a in argv)}
    threading.Thread(target=job_worker, args=(jid, argv, assume, serial),
                     daemon=True).start()
    return jobs[jid]


def run_short(key, timeout=None):
    """同步执行白名单只读命令（保留给脚本/批处理复用），返回 (rc, text)"""
    argv, dfl_timeout = SHORT_CMDS[key]
    timeout = timeout or dfl_timeout
    try:
        p = subprocess.run([DSPARK] + argv, capture_output=True, timeout=timeout,
                           cwd=os.path.dirname(DSPARK), env=PROC_ENV)
        return p.returncode, (p.stdout + p.stderr).decode("utf-8", "replace")
    except subprocess.TimeoutExpired:
        return 124, "[web] 命令 %d 秒超时\n" % timeout
    except Exception as e:
        return 1, "[web] 执行失败: %s\n" % e


# ---------- 日志 tail（dspark.sh logs/elog 是 exec -f 会挂住，这里直接 tail 原文件） ----------
def tail_remote(ssh, cmd, timeout=20):
    full = (["ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout=6", ssh, cmd]
            if ssh else ["bash", "-c", cmd])
    try:
        p = subprocess.run(full, capture_output=True, timeout=timeout, env=PROC_ENV)
        return (p.stdout + p.stderr).decode("utf-8", "replace")
    except subprocess.TimeoutExpired:
        return "[web] 日志读取超时\n"


_ANSI_RE = re.compile(r"\x1b\[[0-9;?]*[ -/]*[@-~]")


def get_log(kind):
    text = _get_log_raw(kind)
    # 剥 ANSI 颜色/光标序列，浏览器 <pre> 不渲染它们（否则显示成 [32m 乱码）
    return _ANSI_RE.sub("", text) if text else text


def _get_log_raw(kind):
    if kind in ("engine", "glm"):
        # 本机 node0：sparkrun 容器读引擎文件；glm53 裸容器读 docker logs
        inner = ('c=$(docker ps --format "{{.Names}}" | grep -E "sparkrun_.*_node_0$" | head -1); '
                 '[ -n "$c" ] && docker exec "$c" tail -n 300 /tmp/sparkrun_serve.log 2>/dev/null; '
                 'docker logs --tail 300 spark_glm53_autoround_mtp3_pmu128 2>&1 | tail -n 300')
        return tail_remote(None, inner)
    if kind == "comfy1":
        return tail_remote(None, "sudo -n tail -n 300 /root/minnimax-h3/logs/comfyui.log | tr '\\r' '\\n'")
    if kind == "comfy2":
        return tail_remote("192.168.0.52",
                           "sudo -n tail -n 300 /root/minnimax-h3/logs/comfyui.log | tr '\\r' '\\n'")
    if kind in ("dispatcher", "comfyd"):
        return tail_remote(None, "sudo -n tail -n 300 /root/minnimax-h3/logs/dispatcher.log")
    if kind == "watchdog":
        return tail_remote(None, "journalctl -u glm53-watchdog --no-pager -n 200")
    if kind == "frpc":
        return tail_remote(None, "journalctl -u frpc-glm --no-pager -n 200")
    return None


# ---------- 文档 ----------
def list_docs():
    out = []
    seen = set()
    for label, root, files in DOC_ROOTS:
        if files is None:
            files = sorted(f for f in os.listdir(root) if f.endswith(".md"))
        for f in files:
            p = os.path.join(root, f)
            if os.path.isfile(p):
                # 不同目录下可能重名（如两个 README.md），重名项用"目录/文件名"消歧
                path = f if f not in seen else os.path.basename(root) + "/" + f
                seen.add(f)
                out.append({"group": label, "path": path,
                            "title": _doc_title(p, f), "mtime": int(os.path.getmtime(p))})
    return out


def _doc_title(path, default):
    try:
        with open(path, encoding="utf-8") as f:
            for line in f:
                m = re.match(r"^#\s+(.+?)\s*$", line.strip())
                if m:
                    return m.group(1)
    except Exception:
        pass
    return default


def read_doc(name):
    """按文件名在 DOC_ROOTS 中查找 md；支持"目录名/文件名"消歧；禁止目录穿越"""
    name = unquote(name or "")
    if not re.match(r"^[\w.\-\u4e00-\u9fa5/]+$", name) or ".." in name:
        return None
    prefix = None
    if "/" in name:
        prefix, name = name.split("/", 1)
    for _label, root, files in DOC_ROOTS:
        if prefix is not None and os.path.basename(root) != prefix:
            continue
        p = os.path.join(root, name)
        if os.path.isfile(p):
            with open(p, encoding="utf-8") as f:
                return f.read()
    return None


# ---------- HTTP ----------
class Handler(BaseHTTPRequestHandler):
    server_version = "dspark-opsweb/1.0"

    def log_message(self, fmt, *args):
        # 静默默认访问日志，改记一行简日志
        try:
            with open(LOG_FILE, "a") as f:
                f.write("%s %s %s\n" % (time.strftime("%F %T"), self.client_address[0], fmt % args))
        except Exception:
            pass

    # --- 安全闸：仅信任管理网/fabric/本机，免密（家庭内网使用，不做公网暴露） ---
    def _allowed_net(self):
        ip = self.client_address[0]
        return any(ip.startswith(n) for n in ALLOW_NETS)

    def _gate(self):
        if not self._allowed_net():
            self.send_error(403, "来源网段不受信任（仅管理网 192.168.31.0/24 可访问）")
            return False
        return True

    def _json(self, obj, code=200):
        body = json.dumps(obj, ensure_ascii=False).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def _text(self, text, code=200, ctype="text/plain; charset=utf-8"):
        body = text.encode("utf-8", "replace")
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        if not self._gate():
            return
        u = urlparse(self.path)
        path, qs = u.path, parse_qs(u.query)
        try:
            if path == "/" or path == "/index.html":
                return self._serve_file(os.path.join(WEB_DIR, "index.html"), "text/html; charset=utf-8")
            if path == "/api/info":
                return self._json({"host": "spark1", "port_api": 8000, "port_shim": 8001,
                                   "comfy_url": "http://192.168.31.51:8188",
                                   "llm_url": "https://${DSPARK_PUB_URL}/v1",
                                   "llm_shim_url": "https://${DSPARK_PUB_URL}/shim/v1",
                                   "time": time.strftime("%F %T")})
            if path == "/api/docs":
                return self._json(list_docs())
            if path.startswith("/api/docs/"):
                doc = read_doc(path.split("/", 3)[3])
                return self._json({"content": doc}, 200 if doc is not None else 404)
            if path == "/api/run":
                key = qs.get("cmd", [""])[0]
                if key not in SHORT_CMDS:
                    return self._json({"error": "未知命令"}, 400)
                extra = build_short_argv(key, qs)
                if key in ("recipes", "recipe-show") and extra is None:
                    return self._json({"error": "参数不合法"}, 400)
                argv, dfl_timeout = SHORT_CMDS[key]
                if extra is not None:
                    argv = extra
                try:
                    p = subprocess.run([DSPARK] + argv, capture_output=True,
                                       timeout=dfl_timeout,
                                       cwd=os.path.dirname(DSPARK), env=PROC_ENV)
                    rc, text = p.returncode, (p.stdout + p.stderr).decode("utf-8", "replace")
                except subprocess.TimeoutExpired:
                    rc, text = 124, "[web] 命令 %d 秒超时\n" % dfl_timeout
                except Exception as e:
                    rc, text = 1, "[web] 执行失败: %s\n" % e
                return self._json({"rc": rc, "text": text})
            if path == "/api/logs":
                kind = qs.get("kind", [""])[0]
                text = get_log(kind)
                return self._json({"text": text}, 200 if text is not None else 400)
            if path == "/api/jobs":
                with jobs_lock:
                    return self._json(sorted(jobs.values(),
                                             key=lambda j: j.get("started") or 0, reverse=True)[:30])
            m = re.match(r"^/api/jobs/([0-9a-f]+)$", path)
            if m:
                job = jobs.get(m.group(1))
                return self._json(job, 200 if job else 404)
            m = re.match(r"^/api/jobs/([0-9a-f]+)/stream$", path)
            if m:
                return self._sse(m.group(1))
            self.send_error(404)
        except BrokenPipeError:
            pass
        except Exception as e:
            self._json({"error": str(e)}, 500)

    def do_POST(self):
        if not self._gate():
            return
        u = urlparse(self.path)
        try:
            if u.path == "/api/jobs":
                length = int(self.headers.get("Content-Length", 0))
                try:
                    payload = json.loads(self.rfile.read(length) or b"{}")
                except Exception:
                    return self._json({"error": "请求不是合法 JSON"}, 400)
                action = payload.get("action", "")
                job = job_start(action, payload)
                return self._json(job, 201 if job else 400)
            self.send_error(404)
        except BrokenPipeError:
            pass
        except Exception as e:
            self._json({"error": str(e)}, 500)

    # --- SSE：增量推送 job 日志，结束后关流 ---
    def _sse(self, jid):
        job = jobs.get(jid)
        if job is None:
            self.send_error(404)
            return
        path = os.path.join(JOB_DIR, jid + ".log")
        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream; charset=utf-8")
        self.send_header("Cache-Control", "no-store")
        self.send_header("Connection", "close")
        self.end_headers()
        sent = 0

        def emit(msg):
            self.wfile.write(("data: %s\n\n" % json.dumps(msg, ensure_ascii=False)).encode())
            self.wfile.flush()

        try:
            emit({"type": "meta", "job": {k: job.get(k) for k in
                                          ("id", "action", "arg", "state", "rc", "argv")}})
            idle = 0
            while True:
                if os.path.exists(path):
                    with open(path, "rb") as f:
                        f.seek(sent)
                        chunk = f.read()
                        if chunk:
                            sent = f.tell()
                            emit({"type": "out", "text": chunk.decode("utf-8", "replace")})
                            idle = 0
                if job["state"] == "done" and (not os.path.exists(path) or sent >= os.path.getsize(path)):
                    emit({"type": "done", "rc": job["rc"]})
                    return
                idle += 1
                if idle > 600:   # 20 分钟无新内容保底断开（切换最长约 25 分钟，有输出会续期）
                    emit({"type": "timeout"})
                    return
                time.sleep(1)
        except (BrokenPipeError, ConnectionResetError):
            return

    def _serve_file(self, path, ctype):
        try:
            with open(path, "rb") as f:
                body = f.read()
        except OSError:
            self.send_error(404)
            return
        self.send_response(200)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


def main():
    os.makedirs(os.path.dirname(LOG_FILE), exist_ok=True)
    srv = ThreadingHTTPServer(("0.0.0.0", PORT), Handler)
    srv.daemon_threads = True
    print("dspark ops web 监听 0.0.0.0:%d（免密；仅管理网/fabric/本机来源；systemd 托管）" % PORT, flush=True)
    try:
        srv.serve_forever()
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
