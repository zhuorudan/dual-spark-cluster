import json, re

issues = json.load(open('/tmp/eugr_issues.json'))
# 关键词→你的解法映射
SOLUTIONS = [
    (r'nccl|mismatch|wedge|hang|stuck|wedg', '看门狗双机成对重建（watchdog）', '单端重启导致 NCCL 失配死循环——dspark.sh watchdog 每2分钟判活，异常时自动 worker→head 成对重建，崩溃现场留档7天'),
    (r'nv_err|nvrm|no_memory|uvm', 'NVRM 噪声判别', 'README 故障排查表：GB10 UVM warmup 时的试探性分配是噪声，几秒一簇、引擎无 ERROR 即无害'),
    (r'prefix.?cache|cached_tokens|pmu', 'PMU128 前缀缓存实测', 'glm53 用 PMU128 前缀匹配粒度，866 prompt 命中 768，命中数在 usage.prompt_tokens_details.cached_tokens'),
    (r'consisten|sync|mirror|both node|two node|cache.*node', '双机缓存一致性', 'dspark.sh cache 查双机大小/文件数/一致性，model-sync 走 200G fabric 双向 rsync（1.8GB/s）'),
    (r'max.?context|context.?len|1m|long context', '1M 上下文配置', 'glm53 实测 1,048,576 上下文 + KV 池 192万 token（fp8），配法在 model-glm53.md'),
    (r'concurrent|throughput|tok/s|benchmark|ttft', '并发阶梯压测方法', '标准压测脚本：并发1/2/3/4/6/8 阶梯，测出 max-num-seqs=6 是吞吐饱和点（65-68 tok/s）'),
    (r'tool.?call|function|dify', 'Dify functions shim', 'shim.py 双向翻译 legacy functions↔tool_calls，Dify 构建模式 Agent 全模型可用工具'),
    (r'download|hf|xet|401|mirror', '下载编排', 'dspark.sh download 双机分片并行+hf-mirror/代理双通道+HF_HUB_DISABLE_XET=1，自动 fabric rsync 合并'),
    (r'earlyoom|oom|kill', 'OOM 策略', 'warmup 峰值会触发 earlyoom 误杀，两机停用 earlyoom，留 16GB swap + 内核 OOM killer 兜底'),
    (r'gateway|unified|multi.?model|switch', '统一网关+一键切换', 'dspark.sh use 一键切换（停旧→等内存→起新→健康门控），:8002 网关固定模型名 dspark，客户端零改动'),
]

matched = []
for i in issues:
    blob = (i['title'] + ' ' + (i['body'] or '')[:800]).lower()
    for pat, sol, detail in SOLUTIONS:
        if re.search(pat, blob):
            matched.append({
                'num': i['number'],
                'title': i['title'][:90],
                'url': i['html_url'],
                'created': i['created_at'][:10],
                'comments': i['comments'],
                'solution': sol,
                'detail': detail,
                'labels': [l['name'] for l in i.get('labels', [])],
            })
            break

print(f'112 个 issue 中，{len(matched)} 个与你的已验证解法直接相关：\n')
for m in sorted(matched, key=lambda x: -x['comments']):
    print(f"#{m['num']} [{m['created']}] ({m['comments']}条回复) {m['title']}")
    print(f"   → 解法: {m['solution']}")
    print(f"   {m['url']}\n")

json.dump(matched, open('/Users/zhuorudan/Projects/dual-spark-cluster/promo/eugr-issues-match.json', 'w'), ensure_ascii=False, indent=1)
print(f'已存: promo/eugr-issues-match.json')
