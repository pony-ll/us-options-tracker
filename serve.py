#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
美股大额未平仓期权追踪 - 本地看板服务

作用:
    启动即检查数据新鲜度 → 若数据日落后于最近一个美股收盘交易日，自动抓取更新
    → 提供本地 HTTP 服务 → 自动打开浏览器。

为何需要它:
    Nasdaq 接口没有 CORS 允许头，浏览器无法直接跨域取数，
    所以"自动更新"必须由本地进程完成。本服务就是这个进程。

用法:
    python serve.py                    # 启动并自动检查/更新，然后打开浏览器
    python serve.py --port 9000        # 指定端口
    python serve.py --force            # 忽略新鲜度，强制重新抓取一次
    python serve.py --no-fetch         # 只起服务，不抓数据（纯看本地已有数据）
    python serve.py --no-open          # 不自动打开浏览器

接口:
    GET  /api/status    当前数据时间、是否正在更新、最近一次错误
    POST /api/refresh   触发后台更新，立即返回；前端轮询 /api/status 等待完成

新鲜度判定:
    以美东时间为准，取"最近一个已完成收盘(16:00 ET)的交易日"，跳过周末。
    meta["asOf"] 是 OI 的结算交易日（由 fetch_options.py 用内容指纹判定，不是标的报价日），
    只要它 >= 该交易日即视为最新；落后时交由后台巡检重试，并用冷却窗口避免反复抓取。
"""

import argparse
import json
import subprocess
import sys
import threading
import time
import webbrowser
from datetime import datetime, timedelta, timezone
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

ROOT = Path(__file__).resolve().parent
DATA_JS = ROOT / "options_data.js"
FETCH_SCRIPT = ROOT / "fetch_options.py"
ET = timezone(timedelta(hours=-5))   # 美东（简化：忽略夏令时，与本项目取数脚本一致）
CST = timezone(timedelta(hours=8))

RETRY_AFTER_MIN = 60      # 冷却窗口(分钟): 刚抓过且源端未推进时，避免反复重抓
STATE = {"refreshing": False, "lastError": None, "lastRun": None}
_LOCK = threading.Lock()


def now_et():
    return datetime.now(timezone.utc).astimezone(ET)


def now_cst_str():
    return datetime.now(timezone.utc).astimezone(CST).strftime("%Y-%m-%d %H:%M:%S CST")


def last_closed_session(now=None):
    """最近一个已完成收盘(16:00 ET)的美股交易日，跳过周末。"""
    n = now or now_et()
    d = n.date()
    if n.hour < 16:
        d -= timedelta(days=1)
    while d.weekday() >= 5:      # 5=周六 6=周日
        d -= timedelta(days=1)
    return d


def read_meta():
    """读取 options_data.js 里的 meta，解析失败返回 None。"""
    if not DATA_JS.exists():
        return None
    try:
        s = DATA_JS.read_text(encoding="utf-8")
        i, j = s.index("{"), s.rstrip().rfind("}")
        return json.loads(s[i:j + 1]).get("meta") or {}
    except Exception:
        return None


def fetch_age_minutes(meta):
    """距上次抓取过了多少分钟；无法解析返回 None。"""
    s = (meta or {}).get("fetchedAt")
    if not s:
        return None
    try:
        dt = datetime.strptime(s, "%Y-%m-%d %H:%M:%S CST").replace(tzinfo=CST)
    except ValueError:
        return None
    return (datetime.now(timezone.utc) - dt).total_seconds() / 60.0


def is_stale(meta, retry_after=None):
    """数据是否需要重新抓取。无数据文件也视为需要更新。

    meta["asOf"] 语义是「OI 的结算交易日」，由抓取脚本用内容指纹判定（而非标的报价日），
    因此可以直接与「最近已收盘交易日」比较。
    免费源在收盘后需数小时才发布当日清算数据，这期间 asOf 会停在上一交易日；若刚抓取过
    就不再重抓，避免无意义的反复请求。
    """
    if retry_after is None:
        retry_after = RETRY_AFTER_MIN
    if not meta or not meta.get("asOf"):
        return True, "缺少可用数据文件"

    asof = meta["asOf"]
    session = last_closed_session().isoformat()
    state = meta.get("oiState")

    if asof >= session:
        return False, f"OI 数据日 {asof} 已覆盖最近收盘交易日 {session}（{state or '—'}）"

    reason = f"OI 数据日为 {asof}，落后于最近收盘交易日 {session}"
    if state:
        reason += f"（{state}"
        if meta.get("oiComparedContracts"):
            reason += f"，上轮 OI 变化 {meta.get('oiChangedContracts')}/{meta.get('oiComparedContracts')} 条"
        reason += "）"

    age = fetch_age_minutes(meta)
    if retry_after > 0 and age is not None and age < retry_after:
        return False, (f"{reason}；{age:.0f} 分钟前刚抓取过，源端尚未推送新数据"
                       f"（{retry_after} 分钟内不重复抓取）")
    return True, reason


def run_fetch(reason=""):
    """执行抓取脚本。返回 True/False；同一时刻只允许一个任务在跑。"""
    with _LOCK:
        if STATE["refreshing"]:
            return False
        STATE["refreshing"] = True
        STATE["lastError"] = None
    try:
        print(f"[{now_cst_str()}] 开始更新数据 · {reason}", flush=True)
        t0 = time.time()
        p = subprocess.run(
            [sys.executable, str(FETCH_SCRIPT)],
            cwd=str(ROOT), capture_output=True, text=True,
            encoding="utf-8", errors="replace", timeout=900,
        )
        if p.returncode != 0:
            msg = (p.stderr or p.stdout or "未知错误").strip()[-600:]
            STATE["lastError"] = msg
            print(f"[{now_cst_str()}] 更新失败: {msg}", flush=True)
            return False
        meta = read_meta() or {}
        STATE["lastRun"] = {
            "asOf": meta.get("asOf"),
            "fetchedAt": meta.get("fetchedAt"),
            "contracts": meta.get("contractCount"),
        }
        print(f"[{now_cst_str()}] 更新完成 · 数据日 {meta.get('asOf')} · "
              f"{meta.get('contractCount')} 条合约 · 耗时 {time.time() - t0:.0f}s", flush=True)
        return True
    except subprocess.TimeoutExpired:
        STATE["lastError"] = "抓取超时（>15 分钟）"
        print(f"[{now_cst_str()}] 抓取超时", flush=True)
        return False
    except Exception as e:
        STATE["lastError"] = str(e)
        print(f"[{now_cst_str()}] 更新异常: {e}", flush=True)
        return False
    finally:
        STATE["refreshing"] = False


class Handler(SimpleHTTPRequestHandler):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, directory=str(ROOT), **kwargs)

    # 数据文件每次都要拿新的，禁止缓存，否则页面刷新看不到更新后的数据
    def end_headers(self):
        if self.path.startswith("/api/") or self.path.endswith((".js", ".html", ".json")):
            self.send_header("Cache-Control", "no-store, no-cache, must-revalidate, max-age=0")
            self.send_header("Pragma", "no-cache")
        super().end_headers()

    def _send_json(self, obj, code=200):
        body = json.dumps(obj, ensure_ascii=False).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        path = self.path.split("?")[0]
        if path == "/api/status":
            meta = read_meta() or {}
            stale, stale_reason = is_stale(meta)
            self._send_json({
                "ok": True,
                "refreshing": STATE["refreshing"],
                "lastError": STATE["lastError"],
                "lastRun": STATE["lastRun"],
                "stale": stale,
                "staleReason": stale_reason,
                "lastClosedSession": last_closed_session().isoformat(),
                "fetchAgeMinutes": None if fetch_age_minutes(meta) is None else round(fetch_age_minutes(meta)),
                "retryAfterMinutes": RETRY_AFTER_MIN,
                "data": {k: meta.get(k) for k in
                         ("asOf", "fetchedAt", "contractCount", "symbolsCount")},
            })
            return
        if path in ("/", ""):
            self.path = "/index.html"
        super().do_GET()

    def do_POST(self):
        if self.path.split("?")[0] == "/api/refresh":
            if STATE["refreshing"]:
                self._send_json({"ok": False, "error": "已有更新任务在执行"}, 409)
                return
            threading.Thread(target=run_fetch, args=("页面手动触发",), daemon=True).start()
            self._send_json({"ok": True, "started": True})
            return
        self._send_json({"ok": False, "error": "not found"}, 404)

    # 静默常规访问日志，只在出错时输出（避免刷屏）
    def log_message(self, fmt, *args):
        return


def watch_stale(interval_min):
    """后台巡检：服务长时间运行时，一旦数据落后于最近收盘交易日就自动补抓。"""
    while True:
        time.sleep(interval_min * 60)
        try:
            if STATE["refreshing"]:
                continue
            stale, reason = is_stale(read_meta())
            if stale:
                run_fetch("定时巡检 · " + reason)
        except Exception as e:
            print(f"[{now_cst_str()}] 巡检异常: {e}", flush=True)


def serve(port, open_browser, check_every):
    httpd = None
    for p in range(port, port + 20):
        try:
            httpd = ThreadingHTTPServer(("127.0.0.1", p), Handler)
            port = p
            break
        except OSError:
            continue
    if httpd is None:
        sys.exit(f"端口 {port}~{port + 19} 都被占用，请用 --port 指定其它端口")

    if check_every > 0:
        threading.Thread(target=watch_stale, args=(check_every,), daemon=True).start()

    url = f"http://127.0.0.1:{port}/index.html"
    print(f"\n看板服务已启动: {url}")
    if check_every > 0:
        print(f"后台巡检: 每 {check_every} 分钟检查一次，数据过期自动更新")
    print("关闭窗口或按 Ctrl+C 结束服务\n")
    if open_browser:
        threading.Thread(target=lambda: (time.sleep(0.6), webbrowser.open(url)), daemon=True).start()
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        print("\n服务已停止")
    finally:
        httpd.server_close()


def main():
    ap = argparse.ArgumentParser(description="期权追踪看板本地服务（启动自动更新数据）")
    ap.add_argument("--port", type=int, default=8787, help="监听端口，默认 8787")
    ap.add_argument("--force", action="store_true", help="忽略新鲜度检查，强制重新抓取")
    ap.add_argument("--no-fetch", action="store_true", help="只起服务，不抓取数据")
    ap.add_argument("--no-open", action="store_true", help="不自动打开浏览器")
    ap.add_argument("--check-every", type=int, default=30,
                    help="后台巡检间隔(分钟)，数据过期自动更新；0=关闭，默认 30")
    ap.add_argument("--retry-after", type=int, default=60,
                    help="冷却窗口(分钟)：抓取后源端未推进时不重复抓取，默认 60")
    args = ap.parse_args()

    global RETRY_AFTER_MIN
    RETRY_AFTER_MIN = args.retry_after

    meta = read_meta()
    need, reason = is_stale(meta)
    print(f"[{now_cst_str()}] {reason}")

    if args.no_fetch:
        need = False
        print("已指定 --no-fetch，跳过数据更新")
    elif args.force:
        need, reason = True, "指定了 --force，强制更新"
        print(f"[{now_cst_str()}] {reason}")

    if need and not meta:
        # 没有可用数据文件时必须先等抓完，否则页面打开是空的
        print(f"[{now_cst_str()}] 无本地数据，先同步抓取（约 1~3 分钟）...")
        if not run_fetch("首次启动"):
            sys.exit("数据抓取失败，请检查网络后重试（或先手动运行 python fetch_options.py）")
        serve(args.port, not args.no_open, args.check_every)
        return

    if need:
        # 有旧数据可看：先把服务起起来，抓取放后台，页面会显示"更新中"并自动重载
        threading.Thread(target=run_fetch, args=(reason,), daemon=True).start()

    serve(args.port, not args.no_open, args.check_every)


if __name__ == "__main__":
    main()
