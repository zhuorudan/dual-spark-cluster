# 双 DGX Spark 集群多模型部署与运维文档

两台 NVIDIA DGX Spark（GB10，统一内存 122GB/台）通过双 CX7 直连组网，以张量并行 TP=2
运行多个大模型（同一时间运行一个，`dspark.sh use <短名>` 一键切换）：

| 短名 | 模型 | 编排 | 统一入口 |
|------|------|------|----------|
| `deepseek` | DeepSeek-V4-Flash 0731（156GB bf16，1M 上下文，~33 tok/s） | sparkrun | 统一 `:8002`（模型名 `dspark`） |
| `qwen38` | Qwen3.8-27B NVFP4 + DFlash2 投机解码（262k，低延迟高并发） | sparkrun | 统一 `:8002`（模型名 `dspark`） |
| `glm53` | GLM-5.3-Flash W4A16 + MTP3/PMU128（1M，Agent 工具调用首选，**当前常驻**） | 裸 docker 双机 + 开机自启 + 运行时看门狗 | 统一 `:8002`（模型名 `dspark`） |
| `minimax-m2-7-nvfp4-vl` | MiniMax-M2.7 NVFP4（131GB，196k 上下文，推理模型，2026-09-21 下载注册，尚未切实验证） | sparkrun（instanttensor） | 统一 `:8002`（模型名 `dspark`） |

- **所有模型的客户端入口完全一致（四个统一入口，模型名都是 `dspark`、同一把 Bearer key）**：
  公网 `https://llm.your-domain.com/v1`（网关，推荐）/ `/shim/v1`（协议 shim 直连），
  内网 `http://<HEAD_IP>:8002/v1`（网关）/ `:8001/v1`（shim）。
  网关与 shim 均强制 key 鉴权，且全量经 `:8001`（legacy `functions` ↔ 现代 `tools`
  双向转换，Dify 构建模式 Agent 全模型可用）转发到 `:8000` 当前 active 引擎，切换模型客户端零改动。
- 端口拓扑（全部在 spark1）：`:8000` 原生 vLLM 引擎（仅排障，glm53 需 key）、
  `:8001` 协议 shim（强制 key）、`:8002` 统一网关（强制 key）、`:8200` 运维 Web 面板（免密仅管理网）。
- 各模型的详细说明见 [`models/`](./models/) 目录。
- 日常运维首选 Web 面板 `http://<HEAD_IP>:8200`（免密、仅管理网）：模型查询/下载/注册/切换/停止/注销、
  双机缓存一致性、任务台实时日志与帮助页；命令行等价封装为 `dspark.sh`（见第 5 节）。

**目录**：[1. 集群拓扑](#1-集群拓扑) · [2. 软件组件](#2-软件组件) ·
[3. 部署步骤](#3-部署步骤可复现) · [4. 使用 API](#4-使用-api) ·
[5. 日常运维](#5-日常运维均在-spark1-执行) · [6. 自动恢复](#6-自动恢复开机自启--运行时看门狗) ·
[7. 带宽说明](#7-带宽说明避免误区) · [8. 故障排查清单](#8-故障排查清单) ·
[9. 关键路径速查](#9-关键路径速查)

---

## 1. 集群拓扑

| 节点 | 角色 | 管理网（家庭局域网） | CX7 链路 1 | CX7 链路 2 |
|------|------|--------------------|-----------|-----------|
| spark1 | head (node_0) | <HEAD_IP> | 192.168.0.51 | 192.168.1.51 |
| spark2 | worker (node_1) | <WORKER_LAN_IP> | 192.168.0.52 | 192.168.1.52 |

- 两机同账号（示例 `sparkadmin`），已配置**双向 SSH 免密**与**免密 sudo**
- 两条 CX7 直连链路均为 100G RoCE，**MTU 9000（巨帧）**，NCCL 双口绑定：
  - `NCCL_IB_HCA=rocep1s0f0,roceP2p1s0f0`
  - `UCX_NET_DEVICES=rocep1s0f0:1,roceP2p1s0f0:1`
- 内网大文件传输实测约 **1.8 GB/s**；推理流量只走直连链路与局域网，不占用互联网带宽

## 2. 软件组件

| 组件 | 说明 |
|------|------|
| sparkrun v0.3.8 | NVIDIA DGX Spark 官方编排工具（venv 位于 `~/.venv/sparkrun`，CLI 在 `~/.local/bin/sparkrun`） |
| vLLM 镜像 | `ghcr.io/spark-arena/dgx-vllm-eugr-nightly-b12x:latest`（35.9GB，vLLM 2026-09-10 nightly，CUDA 13，torch 2.13） |
| 配方 | `@official/deepseek-v4-flash-0731-b12x-dspark-vllm`（TP=2，FP8 KV，DSpark 投机 5 token，B12X MoE/MLA 内核） |
| 模型缓存 | 两机各自一份 HF 缓存（TP=2 要求双机一致）：`models--deepseek-ai--DeepSeek-V4-Flash-0731`（156GB，48 分片）、`models--nvidia--MiniMax-M2.7-NVFP4`（131GB，15 分片）等，均在 `~/.cache/huggingface/hub/` |
| mihomo | 用户态代理（`~/mihomo/`），仅下载加速用，**推理不依赖它** |
| 开机自启 | spark2 `glm53-worker` + spark1 `glm53-head` 双机 oneshot 编排（见第 6 节） |
| 运行时看门狗 | spark1 `glm53-watchdog.timer`（每 2 分钟）：head 容器在跑但 health 持续不通时自动**双机成对重建**（见第 6 节） |
| Dify 协议 shim | spark1 `dify-functions-shim`（:8001 → :8000 当前 active 引擎）：legacy `functions` ↔ `tool_calls` 双向协议转换，模型无关，**所有模型的 Dify 构建模式流量必经**（非 functions 请求原样透传）；接受固定模型名 `dspark` 并按 active 改写，`/v1/models` 同时列出 `dspark` 与真实模型名；自身强制 Bearer key（热读取，/health 豁免），内网与公网 `/shim/v1` 同一套鉴权 |
| Dify 统一模型网关 | spark1 `dspark-model-gateway`（:8002，systemd `dspark-model-gateway`）：内网/公网统一入口，固定模型名 `dspark`，强制 Bearer key（热读取），全量经 :8001 shim 转发 :8000 并注入上游 key，切换模型客户端零改动 |
| 公网暴露 | spark1 `frpc-glm`（frp 0.71.0，网关 :8002 → 公网机 127.0.0.1:8090、shim :8001 → 8091）+ 公网机 Caddy 容器终结 TLS：`https://llm.your-domain.com/v1` 与 `/shim/v1`；网关/shim 均强制 Bearer key 鉴权（key 缺失 fail-closed），所有 vLLM 形态常开，stop/ComfyUI 自动关（见第 3.7/4 节） |
| 运维 Web 面板 | spark1 :8200（`ops-web/`，dspark.sh 的 HTTP 薄外壳，**免密**、仅接受管理网 192.168.31.0/24 与 fabric/本机来源，其余 403）；systemd 单元 `dspark-opsweb` 托管，Restart=always 崩溃自动重启 + 开机自启；`dspark.sh web`。页面含仪表盘/服务控制/**模型管理（查询/下载/注册/切换/停止/注销 + 双机缓存一致性）**/任务台/日志/文档/帮助（[`ops-web/HELP.md`](./ops-web/HELP.md)） |
| earlyoom | **两机均已停用**（warmup 内存峰值会误杀 vllm），保留 16GB swap + 内核 OOM killer 兜底 |

---

## 3. 部署步骤（可复现）

### 3.1 安装 sparkrun 与基础环境（两机）

```bash
# 官方安装脚本（两机分别执行）
curl -fsSL https://spark-arena.github.io/sparkrun/install.sh | bash
export PATH="$HOME/.local/bin:$PATH"

# 在 spark1 上创建集群并完成全部前置配置
sparkrun cluster create dualspark --hosts 127.0.0.1,<WORKER_LAN_IP>
sparkrun cluster set-default dualspark
sparkrun setup ssh          # 双向 SSH 免密
sparkrun setup docker-group # docker 用户组（之后需重新登录生效）
sparkrun setup earlyoom     # OOM 保护（注意：部署 glm53 后两机已停用，见第 2 节）
sparkrun setup check        # 检查所有前置项
```

### 3.2 配置 CX7 双 100G 直连网络

```bash
sparkrun setup cx7 --cluster dualspark
```

该命令自动在两机生成 `/etc/netplan/40-cx7.yaml`（MTU 9000，192.168.0.x / 192.168.1.x）并应用。

**已知坑（本次实际遇到）**：如果对端网口被 NetworkManager 接管（`nmcli device status` 显示"连接中"），netplan 会与 NM 双重占用。需先删除 NM 连接再执行 setup：

```bash
# 在对应主机上查看并删除 CX7 网口的 NM 连接
nmcli device status | grep -E "enp1s0f0|enP2p1s0f0"
nmcli connection delete <连接名或UUID>
# 然后重新执行
sparkrun setup cx7 --cluster dualspark
```

验证：

```bash
ping -M do -s 8972 192.168.0.52   # 巨帧 ping 必须 0% 丢包
ping -M do -s 8972 192.168.1.52
```

> 注：`sparkrun setup cx7 --dry-run` 在检测脚本未真正执行时会误报 "No CX7 interfaces"，直接实际执行即可。

### 3.3 拉取 vLLM 镜像（两机并行）

```bash
docker pull ghcr.io/spark-arena/dgx-vllm-eugr-nightly-b12x:latest
```

若 GHCR 拉取停滞（国内常见），给 dockerd 配代理后重启：

```bash
sudo tee /etc/systemd/system/docker.service.d/http-proxy.conf <<'EOF'
[Service]
Environment="HTTP_PROXY=http://127.0.0.1:7890"
Environment="HTTPS_PROXY=http://127.0.0.1:7890"
Environment="NO_PROXY=localhost,127.0.0.1,::1,192.168.0.0/16,10.0.0.0/8"
EOF
sudo systemctl daemon-reload && sudo systemctl restart docker
```

### 3.4 下载模型（156GB）

家庭带宽为两台共享的 300M（理论上限 37.5 MB/s）。策略：**两机分片并行 + hf-mirror/代理双通道**，尽量吃满带宽；下载完再走 200G 内网合并。

> **日常加新模型不用手工切分片**：以下命令已封装进 `dspark.sh download`——
> `~/文档/dspark.sh download org/name --host both` 自动列出 safetensors 分片对半切、
> 双机并行（默认 hf-mirror + `HF_HUB_DISABLE_XET=1`）、完成后自动双向 rsync 合并；
> 也可在 Web 面板「模型管理」页表单提交（spark1/spark2/both、并发、代理、include 可选）。
> 下载参数直接给 HF 仓库 `org/name`，也可给配方名（如 `download @official/xxx`，自动解析权重仓库）；
> 去掉 @ 的 `official/xxx` 不是 HF 仓库，会被拒绝。下面保留手工步骤作为**从零复现**参考。

spark1（分片 1–24，走 hf-mirror）：

```bash
export HF_ENDPOINT=https://hf-mirror.com
hf download deepseek-ai/DeepSeek-V4-Flash-0731 \
  --include "model-0000[1-9]-of-00048.safetensors" \
  --include "model-0001[0-9]-of-00048.safetensors" \
  --include "model-0002[0-4]-of-00048.safetensors" \
  --include config.json --include generation_config.json \
  --include model.safetensors.index.json \
  --include tokenizer.json --include tokenizer_config.json \
  --include LICENSE --include README.md --include .gitattributes \
  --max-workers 8
```

spark2（分片 25–48，可再拆一部分走代理）：

```bash
export HF_ENDPOINT=https://hf-mirror.com
export HF_HUB_DISABLE_XET=1   # 关键：禁用 Xet，否则 cas-server.xethub.hf.co 401
hf download deepseek-ai/DeepSeek-V4-Flash-0731 \
  --include "model-0002[5-9]-of-00048.safetensors" \
  --include "model-0003[0-9]-of-00048.safetensors" \
  --include "model-0004[0-8]-of-00048.safetensors" \
  --max-workers 8
```

走代理的通道：

```bash
export HTTP_PROXY=http://127.0.0.1:7890
export HTTPS_PROXY=http://127.0.0.1:7890
export HF_HUB_DISABLE_XET=1
hf download deepseek-ai/DeepSeek-V4-Flash-0731 --include <分片glob> --max-workers 6
```

> 注意：不要让同机两个下载任务的 `--include` 范围重叠，否则后启动的会卡在文件锁等待（日志出现 `Still waiting to acquire lock ... elapsed: NNN seconds`）。

### 3.5 走 200G 内网双向合并（在 spark1 执行）

> 已封装：`~/文档/dspark.sh model-sync org/name`（`download --host both` 完成后会自动调用），
> 下面手工命令仅作复现参考。

首次需信任 CX7 地址的主机密钥：

```bash
ssh-keyscan 192.168.0.52 192.168.1.52 >> ~/.ssh/known_hosts
```

双向 rsync 并行（约 2 分钟完成 156GB）：

```bash
R=~/.cache/huggingface/hub/models--deepseek-ai--DeepSeek-V4-Flash-0731
rsync -aH -e "ssh -o Compression=no" $R/ sparkadmin@192.168.0.52:$R/ &
rsync -aH -e "ssh -o Compression=no" sparkadmin@192.168.0.52:$R/ $R/ &
wait
# 清理断点残留
rm -f $R/blobs/*.incomplete
```

完整性校验（两机都应输出 48 / 无缺失）：

```bash
~/.venv/sparkrun/bin/python - <<'PY'
import json, os, glob
snap = glob.glob(os.path.expanduser(
    "~/.cache/huggingface/hub/models--deepseek-ai--DeepSeek-V4-Flash-0731/snapshots/*/"))[0]
idx = json.load(open(snap+"model.safetensors.index.json"))
files = sorted(set(idx["weight_map"].values()))
print("分片数:", len(files))
print("缺失:", [f for f in files if not os.path.isfile(snap+f)] or "无 ✓")
PY
```

### 3.6 启动服务（在 spark1）

```bash
sparkrun run @official/deepseek-v4-flash-0731-b12x-dspark-vllm \
  --cluster dualspark --tp 2 --no-follow
```

首次启动约 5–6 分钟（加载权重 + 编译/捕获 CUDA Graph）。成功后两机会有同名容器：

```
sparkrun_<jobid>_node_0   (spark1, head)
sparkrun_<jobid>_node_1   (spark2, worker)
```

健康检查：

```bash
curl http://<HEAD_IP>:8000/health        # HTTP 200（health 不需要 key）
curl -H "Authorization: Bearer $(~/文档/dspark.sh tunnel key)" \
     http://<HEAD_IP>:8000/v1/models     # glm53 形态需 key
```

### 3.7 公网暴露（llm.your-domain.com，可选；已部署）

组成：spark1 frpc → 公网机（your-public-server.com/<PUBLIC_IP>）frps → 同机 Caddy 容器终结 TLS。
frp 版本须与公网机 frps 一致（v0.71.0，linux_arm64）。

```bash
# 1) key（launch-dspark.sh 已支持：VLLM_API_KEY 环境变量优先，否则读此文件，以 --api-key 注入）
mkdir -p ~/.config/dspark
echo "sk-dspark-$(openssl rand -hex 20)" > ~/.config/dspark/vllm_api_key
chmod 600 ~/.config/dspark/vllm_api_key
scp ~/.config/dspark/vllm_api_key 192.168.0.52:.config/dspark/vllm_api_key   # 同步 spark2

# 2) frpc（spark1）：二进制 /usr/local/bin/frpc，配置 /etc/frp/frpc-glm.toml（0600）
#    serverAddr=your-public-server.com:7000；token 取自公网机 /root/frp/frps.toml（勿写入文档/仓库）
#    两个 tcp proxy：统一网关 :8002 → 远程 8090（/v1/*）、协议 shim :8001 → 8091（/shim/*）
#    （frps proxyBindAddr=127.0.0.1；两个入口自身均强制 Bearer key，所有 vLLM 形态常开）
#    stop/ComfyUI 由 dspark.sh 自动关隧道
# 3) systemd：/etc/systemd/system/frpc-glm.service
#    ExecStart=/usr/local/bin/frpc -c /etc/frp/frpc-glm.toml，Restart=always；systemctl enable --now frpc-glm
```

公网机侧（root）：DNS A 记录 `llm.your-domain.com → <PUBLIC_IP>`；
Caddyfile（容器挂载宿主 `/root/caddy/Caddyfile`）加站点块：
`/v1/*` 转 8090、`/shim/*` 去前缀转 8091，其余 404；TLS 同其他站点禁用 HTTP challenge（80 被门户容器占用）。
改后 `docker exec caddy caddy validate --config /etc/caddy/Caddyfile &&
docker exec caddy caddy reload --config /etc/caddy/Caddyfile`。
验收：`dspark.sh tunnel st`（/v1 与 /shim 两路均无 key 401、带 key 200、模型列表固定名 dspark）。

### 3.8 注册新配方模型（可选）

sparkrun 配方市场里的模型可注册为自定义短名（不改 dspark.sh，注册行写
`~/.config/dspark/models.local.conf`，flock 防并发损坏）：

```bash
~/文档/dspark.sh recipes glm                      # 搜配方（@official / @eugr）
~/文档/dspark.sh recipe-show @eugr/glm-5.3-flash  # 看模型 ID / TP / 默认参数
~/文档/dspark.sh download local-inference-lab/GLM-5.3-Flash-NVFP4-Spark --host both
~/文档/dspark.sh register myglm @eugr/glm-5.3-flash '{"enable_thinking":false}'
~/文档/dspark.sh use myglm                        # 确认权重双机一致后再切换
~/文档/dspark.sh unregister myglm                 # 注销（只删注册行，不删权重）
```

- 短名规则：小写字母开头，仅小写字母/数字/中划线，≤21 字符；内置 `deepseek/qwen38/glm53` 不可注销；
  当前 active 模型需先切走才能注销。
- 也可全程在 Web 面板「模型管理」页完成；注册只是登记，**不会自动下载或切换**。
- `glm53` 是裸 docker 手工形态（非 sparkrun 配方），不要用此流程重复注册，见第 2/5 节。

---

## 4. 使用 API

> 当前常驻模型为 **glm53**（`Intel/GLM-5.3-Flash-W4A16-AutoRound`）。
> **四个统一入口（网关 :8002、shim :8001 及其公网映射）全部强制 Bearer key 鉴权**
> （缺 key 文件时 fail-closed 503，无/错 key 返回 401，热读取免重启）；glm53 引擎
> 本身也以 `--api-key` 启动，其余 sparkrun 引擎仅内网 :8000 排障直连且免 key（不暴露公网）。
> key 存双机 `~/.config/dspark/vllm_api_key`（0600），查看：`~/文档/dspark.sh tunnel key`。

### Endpoint 一览

| 用途 | 地址 | 说明 |
|------|------|------|
| **公网统一入口（推荐）** | `https://llm.your-domain.com/v1` | 模型名固定 `dspark` + Bearer key；所有 vLLM 形态常开（外网客户端/远程 Dify 都配它） |
| **公网 Dify shim** | `https://llm.your-domain.com/shim/v1` | 协议 shim 直连入口（Caddy 去 `/shim` 前缀）；模型名同样 `dspark` + 同一把 key；与公网网关等价，给固定走 shim 的客户端/Dify 配置用 |
| **内网统一入口（推荐）** | `http://<HEAD_IP>:8002/v1` | 同一个网关：固定模型名 `dspark`，全量经 shim 协议转换并注入 key，面板切换模型客户端零改动 |
| 内网原生引擎 | `http://<HEAD_IP>:8000/v1` | 排障直连（当前跑谁就是谁，模型名随切换变）；glm53 需 key，其他 sparkrun 内网免 key |
| 内网 Dify 协议 shim | `http://<HEAD_IP>:8001/v1` | legacy functions 协议转换，模型无关（上游即当前 active 引擎）；模型名同样固定 `dspark`（真实名也兼容）；强制同一把 Bearer key |

> 所有接入信息（各地址 / key / 模型名 / 隧道状态 / curl 示例）都可在
> **运维面板「模型管理 → 接入信息」卡片**一键复制，或 ssh spark1 执行 `dspark.sh access`。

公网链路：
- `https://llm.your-domain.com/v1`：`Caddy(443) → frps 127.0.0.1:8090 → frpc → 网关 :8002 → shim :8001 → :8000 当前引擎`（推荐）
- `https://llm.your-domain.com/shim/v1`：`Caddy(443) → frps 127.0.0.1:8091 → frpc → shim :8001 → :8000`（Dify 构建模式 legacy functions 直连入口；Caddy 去 `/shim` 前缀）

网关与 shim 对各自 `/v1/*` 均强制 Bearer key 校验（key 缺失 fail-closed 返回 503），
故 frpc 两条隧道在**所有 vLLM 形态都保持开启**；stop / ComfyUI 形态自动关隧道。
Caddy 仅放行 `/v1/*`（→8090 网关）与 `/shim/*`（去前缀 →8091 shim），其余路径 404
（扫描器噪声不进引擎）；公网 frps 端口只绑回环，外部唯一入口是 443。

### curl

```bash
KEY=$(~/文档/dspark.sh tunnel key)   # 或面板「接入信息」复制

# 推荐：统一入口（内网/公网均可），模型名固定 dspark，切模型不用改
curl https://llm.your-domain.com/v1/chat/completions \
  -H "Authorization: Bearer $KEY" \
  -H "Content-Type: application/json" \
  -d '{
    "model": "dspark",
    "messages": [{"role":"user","content":"你好"}]
  }'
# 内网把 base 换成 http://<HEAD_IP>:8002/v1；思考/工具参数各模型自行解释
```

### Python（OpenAI SDK）

```python
from openai import OpenAI
client = OpenAI(
    base_url="https://llm.your-domain.com/v1",          # 内网则 http://<HEAD_IP>:8002/v1
    api_key="sk-dspark-...",                           # 面板「接入信息」/ dspark.sh tunnel key
)

resp = client.chat.completions.create(
    model="dspark",                                    # 固定名，与当前实际模型无关
    messages=[{"role": "user", "content": "用三句话解释张量并行"}],
)
print(resp.choices[0].message.content)
```

### Dify 配置

**推荐：统一模型网关（一次配置，切换模型零改动；内网公网同一套模型名/key）**

spark1 运行 `dspark-model-gateway`（:8002，systemd 托管，开机自启/崩溃自愈），
对客户端永远只暴露一个模型名 `dspark`，并强制 Bearer key 鉴权；
内部对**任何 active 模型**统一经 :8001 dify-functions-shim 转发到 :8000 引擎
（Dify 构建模式的 legacy `functions` 协议全模型可用；普通请求经 shim 原样透传，
同时自动注入 vllm key）。切换模型不需要再动 Dify。

- Dify → 设置 → 模型供应商 → **OpenAI-API-compatible** → 添加模型：
  - Model Name：**`dspark`**（固定，不要填真实权重 ID）
  - Model Type：LLM / Chat；Function Calling 打开
  - API endpoint URL：
    - 内网 Dify：**`http://<HEAD_IP>:8002/v1`**
    - 公网/远程 Dify：**`https://llm.your-domain.com/v1`**（所有 vLLM 形态都通）；
      要求直连 shim 的客户端可用等价入口 **`https://llm.your-domain.com/shim/v1`**
  - API Key：**面板「模型管理 → 接入信息」里显示的 key**（即 `dspark.sh tunnel key`）；
    `tunnel rotate` 轮换后需更新此字段（网关热读取，无需重启任何服务）
  - Context size：按常用模型保守填 `131072`（各模型上限不同，min 196K / qwen38 262K / glm53 1M）
- 网关内网侧仅接受 192.168.31/192.168.0 网段；公网侧经 HTTPS + frp 隧道，key 是唯一闸门。
- 切换后最迟秒级生效（网关按 active 文件 mtime 刷新）；切换进行中调用会返回 502/503，面板健康后恢复。
- 全部地址/key/模型名随时可在面板「接入信息」卡片或 `dspark.sh access` 查询复制。

**直连各引擎（需要在 Dify 里区分多个模型名时才用）**

- 模型供应商同样选 OpenAI-API-compatible，每个模型一条配置；
- **构建模式 Agent 的 endpoint 一律填 shim** `http://<HEAD_IP>:8001/v1`
  （任何模型都适用：shim 上游就是当前 active 引擎，负责 legacy functions 协议转换），
  模型名**同样可以填固定的 `dspark`**（shim 按 active 自动改写；真实 served-model-name
  也继续兼容），密钥填 `tunnel key`；直连 :8000 会导致 Agent 工具不执行
  （新版 vLLM 已移除 functions 兼容层；glm53 另有思考流兼容细节，见
  [glm53 说明](models/README-glm53-flash-w4a16-mtp3-pmu128.md)）。
- 纯对话（无工具）内网可直连 `http://<HEAD_IP>:8000/v1`：glm53 需 key，其他 sparkrun 内网免 key；同一时刻只有一个在线。
- 引擎直连均不走公网（frpc 只转发网关 :8002 与 shim :8001 两个鉴权服务，不转发裸引擎 :8000）；公网统一使用上面的固定入口。

### API key 管理

```bash
~/文档/dspark.sh access         # 推荐：当前模型全部接入信息（地址/key/模型名/隧道态/curl）
~/文档/dspark.sh tunnel url     # 公网入口 + key + 模型名
~/文档/dspark.sh tunnel key     # 只打印 key
~/文档/dspark.sh tunnel st      # 隧道服务 + 公网 HTTPS 鉴权全链路探测（401/200）
~/文档/dspark.sh tunnel rotate  # 生成新 key → 同步双机；glm53 需重启引擎（约 8 分钟），其他形态热生效
~/文档/dspark.sh tunnel log     # 跟踪 frpc 日志
```

- 一把 key 同时用于网关（:8002）、协议 shim（:8001，内网 + 公网 /shim）和 glm53 引擎；
  面板「接入信息」卡片可直接复制最新值。
- 轮换后旧 key 立即失效（网关与 shim 均热读取 key 文件，无需重启）；
  glm53 引擎以 `--api-key` 启动，轮换后脚本自动双机重启使其生效；其他 sparkrun 引擎不校验 key，无需重启。
- frpc 与形态联动：**任何 vLLM 模型（deepseek/qwen38/glm53/自注册）运行时两条隧道均常开**
  ——公网只打到强制 key 鉴权的网关 :8002 与 shim :8001，不转发裸引擎；stop / ComfyUI 形态自动关隧道。

### 其他模型（deepseek / qwen38 / minimax）

切到对应模型后模型名与参数见 `~/文档/dspark.sh models` 与 [`models/`](./models/)：

- deepseek：`deepseek-ai/DeepSeek-V4-Flash-0731`；`chat_template_kwargs.thinking=false`
  关闭思考（思考过程在 `reasoning_content`），`reasoning_effort` 调思考强度
- qwen38：`nvidia/Qwen3.8-27B-NVFP4`；`enable_thinking=false` 关闭思考
- minimax-m2-7-nvfp4-vl：`nvidia/MiniMax-M2.7-NVFP4`（131GB NVFP4，instanttensor 加载，
  配方固定 `max_model_len=196608`、TP=2、`reasoning_parser/tool_call_parser=minimax_m2`、
  `VLLM_MARLIN_USE_ATOMIC_ADD=1`；2026-09-21 已双机下载+注册，**尚未启动实测**，
  切换验证后再补性能数据与注意事项）
- 并发默认 max_num_seqs=8、max_num_batched_tokens=8192，可用 `-o max_num_seqs=16` 等覆盖
- 这些形态下公网同样可用：客户端始终走网关/shim 固定入口（模型名 `dspark` + key），无需感知实际模型

## 5. 日常运维（均在 spark1 执行）

**推荐使用封装好的运维脚本 `~/文档/dspark.sh`：**

```bash
~/文档/dspark.sh models          # 列出可切换的模型（--json 结构化输出，Web 面板用）
~/文档/dspark.sh current         # 当前运行的模型
~/文档/dspark.sh access          # 当前模型全部接入信息（地址/key/固定模型名 dspark/隧道态，--json；面板同款）
~/文档/dspark.sh use qwen38      # 一键切换模型（停旧→等两机内存释放→起新→等健康，约2-10分钟）
~/文档/dspark.sh status          # 集群总览（容器 + API + 看门狗；active 与实际模型不符会告警）
~/文档/dspark.sh check           # 一键巡检 10 项：SSH/CX7/容器/API/磁盘/GPU/代理/shim(常检)+glm53自启看门狗/网关鉴权:8002/公网隧道
~/文档/dspark.sh start           # 启动当前模型并自动等待健康
~/文档/dspark.sh stop            # 停止
~/文档/dspark.sh restart         # 重启
~/文档/dspark.sh logs            # sparkrun 编排日志
~/文档/dspark.sh elog            # head 容器内引擎日志 /tmp/sparkrun_serve.log
~/文档/dspark.sh monitor         # 两机 GPU/统一内存/负载 + vLLM KV占用/排队（可配 watch -n5）
~/文档/dspark.sh net             # CX7 双链路 + MTU9000 巨帧测试
~/文档/dspark.sh proxy st        # mihomo: st状态 / up启动 / down停止 / test测速
~/文档/dspark.sh ask "问题"      # 快速提问（自动带各模型的轻思考参数，流式；glm53 强制思考→low）
~/文档/dspark.sh bench           # 性能测试（TTFT / tok/s）
~/文档/dspark.sh watchdog st     # glm53 看门狗：on启用 / off关闭(需yes) / st状态 / recover手动重建 / log日志 / pm崩溃现场
~/文档/dspark.sh web url         # 运维 Web 面板（:8200，免密仅管理网）：up 装 systemd 单元自启/自动重启，down/st/url
~/文档/dspark.sh tunnel st       # 公网暴露：st全链路探测 / url地址key / key / rotate轮换重启 / log
# —— 模型全套生命周期（查询/下载/部署/同步，也可在 Web 面板「模型管理」页图形化操作）——
~/文档/dspark.sh cache           # 双机 HF 权重缓存 JSON：大小/文件数/双机一致性/已注册关联
~/文档/dspark.sh recipes [kw]    # 搜 sparkrun 配方市场；recipe-show @reg/name 看模型ID/TP/默认参数
~/文档/dspark.sh download org/name [--host spark1|spark2|both] [--workers N] [--no-mirror] [--proxy]
#                                默认 hf-mirror+禁XET；both=双机 safetensors 对半并行，完成自动内网 rsync 合并
~/文档/dspark.sh model-sync org/name   # 200G fabric 双向 rsync（单机下载后补齐另一台）
~/文档/dspark.sh register 短名 @reg/recipe ['{"enable_thinking":false}']  # 注册到可切换列表（写 ~/.config/dspark/models.local.conf）
~/文档/dspark.sh unregister 短名 # 注销自注册模型（只删注册行，不删权重；内置模型受保护）
```

> Web 面板（`http://<HEAD_IP>:8200`）提供模型查询/下载/注册/切换/停止/注销的全套图形化操作与「帮助」页
> （渲染 [`ops-web/HELP.md`](./ops-web/HELP.md)，含生命周期图解、下载通道、双机同步与故障排查表）。
> 下载/同步为非串行长任务，可与模型切换并行；明细日志在 `~/.local/state/dspark-dl/`。
> 注意：从 Web 面板发起的下载任务是 opsweb 的子进程，**重启 `dspark-opsweb` 会连带中断**
> （HF 下载支持断点续传：重新发起同一任务即可，已完成分片与双机 rsync 不会重做；
> 大模型下载建议在 ssh 终端直接跑 `dspark.sh download` 规避）。

### 5.1 万兆 NAS 模型备份（双机 automount，自动续传）

绿联 NAS `192.168.31.215`（44TB，SMB3；spark1 走 10GbE 管理口实测读 373/写 385 MB/s）：

- 双机均通过 **systemd automount** 挂到 `/mnt/nas`（访问时自动挂、空闲 10 分钟自动卸、断网不卡死）；
  凭据在 `/etc/nas-cred`（root:0600，不入仓库），挂载单元 `mnt-nas.automount`，挂载参数含 `mfsymlinks`
  （CIFS 上保留 HF 缓存的 blob↔snapshot 符号链接）。
- 备份布局：`/mnt/nas/models-hf/hub/models--org--name/`（HF 原生缓存结构，可直接当 `HF_HOME` 用）、
  `/mnt/nas/models-hf/manual/`（glm53 的 `~/models` 手动权重）；只从 spark1 备一份（双机快照已校验一致）。
- **自动触发**：`download --host both` 完成双机合并后、单机 `--host spark1` 下载后、
  以及 `model-sync` 成功后，`dspark.sh` 自动增量 rsync 该仓库到 NAS（任务台可见 20s 心跳进度）；
  NAS 不可达只告警不影响下载结果，下次自动续传；日志 `~/.local/state/dspark-backup/`。
- 恢复示例：`rsync -a /mnt/nas/models-hf/hub/models--org--name/ ~/.cache/huggingface/hub/models--org--name/`，
  再对另一台执行一次 `dspark.sh model-sync org/name` 即恢复双机就绪。
- 注意 NAS 是硬盘不是显存：**不能**让 vLLM 跨 SMB 流式读权重（373MB/s 对统一内存慢约 700 倍），仅作冷备/中转。

**多模型**：权重与镜像共存于磁盘，同一时间只运行一个。客户端统一走固定入口
（模型名 `dspark` + Bearer key：内网 `http://<HEAD_IP>:8002/v1`、
公网 `https://llm.your-domain.com/v1`；`:8001` 与 `/shim/v1` 为等价 shim 直连入口），
切换模型后**不需要改任何客户端配置**；
key/地址随时在面板「接入信息」或 `dspark.sh access` 查询。
需要在 Dify 里按真实模型名区分多模型时才直连：纯对话可走 `http://<HEAD_IP>:8000/v1`
（仅内网）；**构建模式 Agent 无论什么模型都用
`http://<HEAD_IP>:8001/v1`**（shim 模型无关，上游即当前 active 引擎；
直连 :8000 会因新版 vLLM 不兼容 legacy functions 导致工具不执行，glm53 还会思考流假死，见
[glm53 说明](models/README-glm53-flash-w4a16-mtp3-pmu128.md)）。
`dspark.sh` 已内建的切换保障（2026-09-15/16 两次事故后补齐）：

- 停旧服务后等两机 **available 内存恢复 ≥25GiB** 再起新服务（看 available 而非 used，
  避免 150GB 旧权重占页缓存时误放行导致 `ibv_reg_mr ENOMEM`）；
- 切 glm53 前两机 available <40GiB 自动 `drop_caches`，预防冷盘 GPTQ→Marlin 重打包硬挂；
- health 等待期间每分钟打印引擎最后一行日志，停在 Marlin 阶段连续 5 分钟直接告警疑似硬挂
  （附 py-spy 实锤命令），不再盲等 25 分钟；
- worker 的所有 SSH 操作 **fabric（192.168.0.52）优先、管理网兜底**（r8127 管理口曾掉线）；
- use/start/stop/restart 与看门狗共用 flock，不会与自动恢复互相打架。

每个模型的详细说明见 [`models/`](./models/) 目录：

- `deepseek` — DeepSeek-V4-Flash 0731，通用旗舰，1M 上下文，~33 tok/s（[说明](models/README-deepseek-v4-flash.md)）
- `qwen38` — Qwen3.8-27B-NVFP4 + DFlash2 投机解码，低延迟高并发，262k 上下文（[说明](models/README-qwen3.8-27b-nvfp4-dflash2.md)）
- `glm53` — GLM-5.3-Flash W4A16 原生 MTP3 + PMU128，Agent 工具调用/多会话并发首选，1M 上下文、KV 池 192 万 token；2026-09-15 同口径压测单流 21.1 tok/s、**6 路聚合 64-68 tok/s（推荐并发上限 6）**；裸 docker 双节点编排（非 sparkrun），强制思考仅可 low/high/max（[说明](models/README-glm53-flash-w4a16-mtp3-pmu128.md)）
- `minimax-m2-7-nvfp4-vl` — MiniMax-M2.7 NVFP4（NVIDIA 量化，131GB，196k 上下文，sparkrun/instanttensor 形态，自带 reasoning/tool_call parser）；2026-09-21 双机下载并注册，尚未切换实测（暂无独立说明文档）

底层原语（脚本即封装的这些）：

```bash
sparkrun status --cluster dualspark     # 两机容器状态（含 job id）
sparkrun logs <job_id>                  # 跟踪日志
sparkrun stop <job_id>                  # 停止并清理两机容器
sparkrun run <recipe> --cluster dualspark --tp 2   # 重新启动
sparkrun cluster monitor                # 实时监控两机 CPU/内存/GPU
nvidia-smi                              # GB10 显存列显示 N/A 正常（统一内存看 free -h）
```

引擎详细日志在 head 容器内：`/tmp/sparkrun_serve.log`

```bash
docker exec sparkrun_*_node_0 tail -f /tmp/sparkrun_serve.log
```

## 6. 自动恢复：开机自启 + 运行时看门狗

### glm53（当前常驻模型）：重启与运行时崩溃均自动恢复

已通过双机真实重启（含同时断电式重启）验证，开机后约 10 分钟 API 恢复 200；
运行中引擎崩溃由看门狗在约 6-10 分钟内自动双机重建。
公网链路独立于引擎自启：`frpc-glm.service` 自身 enabled，开机/断线自动重连，
且 `dspark.sh` 的 use/start/stop/switch 会按模型形态自动开关它：

| systemd 单元 | 主机 | 作用 |
|---|---|---|
| `glm53-worker.service` | spark2 | 开机：fabric 就绪后起 worker 容器（失败 10s 重试） |
| `glm53-head.service` | spark1 | 开机：确认 worker 容器连续 30s 稳定 running，再延迟 20s 起 head |
| `dify-functions-shim.service` | spark1 | `:8001` 协议 shim（模型无关，全模型 Dify 构建模式必经），崩溃自动重启 |
| `frpc-glm.service` | spark1 | 公网隧道两条 tcp proxy（网关 :8002 → 8090 走 `/v1/*`、shim :8001 → 8091 走 `/shim/*`）→ llm.your-domain.com，enabled 开机自启 + frp 内置断线重连 |
| `dspark-opsweb.service` | spark1 | 运维 Web 面板 :8200（免密、仅管理网），Restart=always 崩溃自动重启 + enabled 开机自启 |
| `glm53-watchdog.timer` | spark1 | **运行时**：每 2 分钟判活，异常时自动双机成对重建（见下） |

**开机自启要点**：

- 容器策略 `unless-stopped`：worker 在 head 空窗期会于分布式 init 崩溃一次，
  Docker 自动拉起重连（这是预期行为，`RestartCount=1` 正常）。
- 注意：开机固定自启 glm53；若重启前用 `dspark.sh use` 临时切到了其他模型，
  重启后仍会回到 glm53（生产默认）。

**运行时看门狗要点**（源于 2026-09-16 事故：head 空载自主退出后单端重启陷入
NCCL 失配死循环，停摆 50 分钟）：

- 判据：head 容器 running 但 health 持续不通——曾健康过连续 3 次（≈6min）触发；
  从未健康则等容器 age>15min 后连续 5 次（≈10min，冷启动宽限）。
  容器消失/非 running（人工 stop、切其他模型）**不干预**。
- 恢复动作与人工铁律一致：双机 stop（head→worker）→ 等内存 → 必要时 drop_caches
  → worker rank1 → 20s → head rank0。
- 安全阀：两次恢复最短间隔 20 分钟；1 小时内恢复 4 次熔断 30 分钟并 journal 告警。
- 恢复前自动把两机旧容器日志存为
  `~/.local/state/glm53-watchdog/postmortem-<时间>.log`（保留 7 天），
  解决"`docker rm` 后崩溃日志永久丢失、无法定位根因"的问题。
- 排障命令：

```bash
~/文档/dspark.sh watchdog on      # 启用（当前开始判活 + 开机自启；等价 systemctl enable --now timer）
~/文档/dspark.sh watchdog off     # 关闭（停用 + 禁用开机自启，需输入 yes；不影响模型服务）
~/文档/dspark.sh watchdog st      # 状态（计数/上次恢复/熔断次数；等价 watchdog.sh status）
~/文档/dspark.sh watchdog log     # 实时看判定过程（journalctl -u glm53-watchdog -f）
~/文档/dspark.sh watchdog pm      # 列崩溃现场并预览最新一份（postmortem-*.log，保留 7 天）
~/文档/dspark.sh watchdog recover # 手动触发双机成对重建（需输入 yes 确认）
```

- 编排文件留档：`~/文档/glm53-autostart/`（单元 + 门控脚本 + watchdog.sh），
  部署副本在两机 `~/spark-recipes/tp2_glm53flash_autoround_mtp3_pmu128/`。
- 手工排障：`systemctl status glm53-head` / `journalctl -u glm53-head`，
  worker 侧优先经 fabric `ssh 192.168.0.52`（管理网抖动时仍可达）。

### deepseek / qwen38（sparkrun 模型）：手动一行

```bash
~/文档/dspark.sh start        # 按 active 记录拉起对应模型并等健康检查
# 或指定切换：~/文档/dspark.sh use deepseek|qwen38
```

### 重启后通用检查

```bash
ping -c1 192.168.0.52         # CX7 fabric（netplan 持久化，开机即应生效）
~/文档/dspark.sh check        # 10 项巡检（shim 8001 常检；glm53 时另检自启/看门狗单元；统一网关 8002 常检）
# mihomo 仅下载需要：~/文档/dspark.sh proxy up
```

## 7. 带宽说明（避免误区）

- 两台机器共享同一条 300M 家庭宽带，**总下载速度上限固定为 ~37.5 MB/s**，代理流量同样占用该带宽，双机并行不会突破上限
- 双通道/多机分片的价值在于：① 用更多并发 TCP 连接把带宽利用率从 30–60% 拉到 ~80%（实测峰值 30.4 MB/s）；② 单通道断流时有冗余，全程无需人工干预
- 真正不占外网的加速是 **200G CX7 直连**：模型只需外网拉一份，内网合并速度 ~1.8 GB/s
- 日常推理只走直连链路与局域网，零外网带宽消耗

## 8. 故障排查清单

| 现象 | 原因 / 处理 |
|------|------------|
| 终端提示 `sparkrun：未找到命令` | `~/.local/bin` 只写进了 `~/.profile`，非登录终端不读取；已在 `~/.bashrc` 追加 PATH，执行 `source ~/.bashrc` 或新开终端即可 |
| `sparkrun setup cx7` 报 NM 双重占用 | 删除 CX7 网口的 NetworkManager 连接后重跑（见 3.2） |
| hf 下载报 `cas-server.xethub.hf.co 401` | 设置 `HF_HUB_DISABLE_XET=1`，或改用 hf-mirror（`dspark.sh download` 默认已带这两项） |
| 第二个下载任务长时间等锁 | 两个任务的 `--include` 分片范围重叠，重新划分互不相交的范围；双机模式让 `download --host both` 自动切分即可 |
| 双机缓存不一致/只在一台 | `dspark.sh cache` 看大小与文件数，`dspark.sh model-sync <repo>` 走 fabric 双向 rsync 合并；Web 模型管理页可直接点「双向同步」 |
| register 报"找不到配方" | 用 `dspark.sh recipes` 确认配方名与 registry 前缀（`@official`/`@eugr`）；repo_id 与配方名不是一回事，勿混用 |
| 注册后 `use` 报缺权重 | 注册只登记不下载：先 `download` 且两机缓存一致（`cache` 显示双机一致）再切换 |
| docker pull 长时间无进度 | 给 dockerd 配 HTTP(S)_PROXY 后 `systemctl restart docker`（见 3.3） |
| sparkrun 分发报 `Host key verification failed`（192.168.0.52 / 192.168.1.52） | `ssh-keyscan` 把两机 CX7 IP 加入 `~/.ssh/known_hosts` |
| 报 `permission denied ... docker.sock` | 用户刚加入 docker 组，需重新登录（或在新 SSH 会话中执行） |
| `sparkrun status` 把本机 127.0.0.1 列为 `Idle hosts`，但服务其实在跑 | 当前终端继承了加 docker 组之前的旧进程身份（SSH 登录的对端显示正常）。临时用 `sg docker -c 'sparkrun status ...'`；根治为 Reload Window 或注销重登 |
| mihomo 启动卡在 "MMDB ... download" | 手动放置 GeoIP：`~/mihomo/geoip.metadb`（从 github.com/MetaCubeX/meta-rules-dat releases/latest 下载） |
| 服务无响应 | 先 `~/文档/dspark.sh status` 看容器/API，`./dspark.sh elog` 看引擎日志；glm53 看门狗通常会在 6-10 分钟内自动双机重建，先查 `journalctl -u glm53-watchdog` 是否已在恢复 |
| head 容器周期性重启（约每 10 分钟一轮）、worker 容器不重启 | TP 单端重启失配：只有 head 被 `unless-stopped` 拉起、与旧 worker NCCL 世界不一致，集合超时循环。**必须双机成对重建**（看门狗自动做；手动用 `dspark.sh restart` 或 launch stop+both），勿只重启单机 |
| glm53 启动卡 `Using MarlinExperts`，health 长时间 000 | 冷盘 GPTQ→Marlin 重打包在内存紧张时硬挂：看 dspark 等待输出的告警，py-spy 专家计数 60s 不动即实锤；双机 stop、确认两机内存充足后按 worker→20s→head 重启（详见 glm53 README 踩坑#4，脚本已内建 drop_caches 预防） |
| dmesg 刷 `NVRM ... NV_ERR_NO_MEMORY mem_desc.c` 但服务正常 | GB10 UVM 在 TileLang JIT/warmup 时的试探性分配噪声，几秒一簇、引擎无 ERROR 即无害，勿当崩溃处理 |
| 管理网 <WORKER_LAN_IP> 不通（r8127 watchdog） | 一切 spark2 操作走 fabric `ssh 192.168.0.52`；dspark.sh 与看门狗均已 fabric 优先、管理网兜底。管理口可用 `nmcli` down/up flap 恢复 |
| Dify 构建模式 Agent 工具不执行 | 任何模型都必须经 shim：走网关（:8002，已统一经 :8001）或直连 `:8001`，直连 `:8000` 会被新版 vLLM 丢弃 legacy `functions`；查 `systemctl status dify-functions-shim` |
| 开机后 worker 容器 `Exited(1)` 且 RestartCount=1 | 正常竞态：master 空窗期分布式 init 崩溃，`unless-stopped` 会自动拉起；持续崩溃才需查 `journalctl -u glm53-worker` 与 fabric 链路 |
| 开机后 head 单元 failed | 门控超时（worker 未稳定）；单元 `Restart=on-failure` 会自动重试，也可 `sudo systemctl reset-failed glm53-head && sudo systemctl start glm53-head` |
| 看门狗 1 小时内反复恢复并告警熔断 | 持续性硬件/网络故障，自动恢复已暂停 30 分钟；查最新 `postmortem-*.log` + `nvidia-smi`/fabric 链路，人工介入 |
| 公网调用全部 401（含 Dify） | 未带/带错 key：面板「模型管理 → 接入信息」或 `dspark.sh access`/`tunnel key` 取最新值；刚执行 `tunnel rotate` 必须同步更新所有客户端 |
| `https://llm.your-domain.com` 返回 000/502 | 顺序排查：`dspark.sh tunnel st`（服务+全链路探测）→ `systemctl status frpc-glm` → `dspark.sh tunnel log` 看 frpc 是否连上 frps；502/503 多为模型正在切换或引擎未就绪（网关本身常驻，健康后自动恢复）；公网机侧看 `docker logs caddy` |
| 公网无 key 访问竟返回 200 | 正常应被网关/shim 401 拒绝：立即 `systemctl status dspark-model-gateway dify-functions-shim`，确认 frpc 两条转发均指向鉴权服务（`sudo cat /etc/frp/frpc-glm.toml`：localPort 应为 8002/8001，不能指裸引擎 8000） |
| 切到 deepseek/qwen38/minimax 等任意模型后公网访问 | 同样走 `https://llm.your-domain.com/v1`（或等价的 `/shim/v1`，模型名固定 `dspark` + key），网关/shim 自动改写到当前引擎，无需改客户端；仅 stop/ComfyUI 形态才关隧道 |
| 打开 :8200 面板返回 403 | 面板免密但限网段（192.168.31.0/24、fabric 192.168.0/1.x、本机）；换管理网内设备访问，面板不做公网暴露 |

## 9. 关键路径速查

```
运维脚本 ................ ~/文档/dspark.sh（status/check/start/stop/restart/logs/monitor/net/proxy/ask/bench/web/tunnel/cache/recipes/recipe-show/download/model-sync/register/unregister）
sparkrun CLI ............ ~/.local/bin/sparkrun
sparkrun venv ........... ~/.venv/sparkrun（spark1，含 hf CLI 1.8）；spark2 的 hf CLI 在独立 venv ~/.venv/hf（1.31），下载命令按机自动探测
sparkrun 集群配置 ....... sparkrun cluster show dualspark
CX7 netplan ............. /etc/netplan/40-cx7.yaml（两机）
模型缓存 ................ ~/.cache/huggingface/hub/（deepseek 156GB/48 分片、MiniMax-M2.7 131GB/15 分片等；双机各自一份，TP=2 要求一致）
用户模型注册表 .......... ~/.config/dspark/models.local.conf（register/unregister 管理，flock；升级脚本不丢失）
下载明细日志 ............ ~/.local/state/dspark-dl/（双机分片各自 .log/.rc，含 15s 心跳）
mihomo .................. ~/mihomo/（config.yaml, mihomo, geoip.metadb）
docker 代理配置 ......... /etc/systemd/system/docker.service.d/http-proxy.conf
sparkrun 容器 ........... sparkrun_<jobid>_node_{0,1}
glm53 容器 .............. 两机同名 spark_glm53_autoround_mtp3_pmu128（unless-stopped）
glm53 配方/启动器 ....... ~/spark-recipes/tp2_glm53flash_autoround_mtp3_pmu128/launch-dspark.sh
开机自启编排 ............ ~/文档/glm53-autostart/（glm53-head/worker.service + boot-wait-rank.sh）
运行时看门狗 ............ glm53-watchdog.timer + watchdog.sh（同上目录；journalctl -u glm53-watchdog）
看门狗状态/崩溃现场 ..... ~/.local/state/glm53-watchdog/（fails/attempts/postmortem-*.log，与 dspark 共用 lock）
Dify 协议 shim .......... ~/文档/dify-functions-shim/（shim.py + .service，监听 :8001）
Dify 统一模型网关 ........ ~/文档/dspark-model-gateway/gateway.py + dspark-model-gateway.service（:8002，固定模型名 dspark，强制 Bearer key）
vLLM API key ............ ~/.config/dspark/vllm_api_key（双机 0600；dspark.sh access / tunnel key/rotate）
frpc 公网隧道（spark1）.. /etc/frp/frpc-glm.toml + frpc-glm.service（/usr/local/bin/frpc，:8002→8090 网关、:8001→8091 shim；vLLM 运行时常开，stop/ComfyUI 自动关）
公网机（your-public-server.com）. Caddy 容器：/root/caddy/Caddyfile（llm 块，/v1/* 转 8090、/shim/* 去前缀转 8091）；frps：/root/frp/frps.toml（远程端口 8090/8091 仅绑回环）
运维 Web 面板 ........... ~/文档/ops-web/（:8200，免密仅管理网；systemd 单元 dspark-opsweb 自启/自动重启；dspark.sh web；帮助文档 ops-web/HELP.md）
Web 任务/面板日志 ....... ~/.local/state/dspark-opsweb/（jobs/*.log 任务台输出；opsweb.log 访问日志）
引擎日志 ................ sparkrun: 容器内 /tmp/sparkrun_serve.log；glm53: docker logs <容器名>
API（内网直连） ......... http://<HEAD_IP>:8000/v1（glm53 需 Bearer key；其他 sparkrun 内网免 key）
API（Dify 固定入口） .... http://<HEAD_IP>:8002/v1（模型名 dspark + Bearer key；推荐）
API（内网 Dify shim） .. http://<HEAD_IP>:8001/v1（全模型 legacy functions 转换；模型名 dspark/真实名均可；同一把 key）
API（公网固定入口） ..... https://llm.your-domain.com/v1（网关；模型名 dspark + Bearer key，所有 vLLM 形态；ACME 自动证书）
API（公网 Dify shim） ... https://llm.your-domain.com/shim/v1（shim 直连；模型名 dspark + 同一把 key）
```
