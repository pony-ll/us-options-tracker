#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
美股大额未平仓期权追踪 - 数据抓取脚本

数据源: Nasdaq 公开期权链接口 (api.nasdaq.com)，免费、无需 key。
抓取范围: 标的池内每个 ticker 的看涨/看跌期权链，覆盖近 ~2 个月到期日。
输出: options_data.js —— 供 index.html 离线加载的数据文件 (window.OPTIONS_DATA)。

用法:
    python fetch_options.py
    python fetch_options.py --tickers AAPL,TSLA,NVDA      # 自定义标的
    python fetch_options.py --min-oi 1000                 # 只保留 OI>=1000 的合约(减小体积)

口径说明:
    - Open Interest (OI) = 未平仓合约张数，来自 Nasdaq 最近交易日收盘快照。
    - Volume = 当日成交量(张)。Volume/OI 高表示当日新开仓活跃。
    - 估算名义金额 = OI × 100(合约乘数) × 行权价 —— 粗略量级，非精确敞口(未用 Delta/标的价格)，仅供排序参考。
    - 数据为报价快照(meta.marketPhase 标注 盘前/盘中/盘后/休市)，非实时逐笔大单流；免费源无逐笔 block trade 数据。
"""

import argparse
import json
import os
import re
import sys
import time
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

try:
    import requests
except ImportError:
    sys.exit("缺少依赖 requests，请先执行: pip install requests")

BASE = "https://api.nasdaq.com/api/quote/{sym}/option-chain"
HEADERS = {
    "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                   "AppleWebKit/537.36 (KHTML, like Gecko) "
                   "Chrome/120.0 Safari/537.36"),
    "Accept": "application/json",
    "Accept-Language": "en-US,en;q=0.9",
}

# 默认标的池: 大盘ETF + 科技龙头 + 高波动热门股
DEFAULT_UNIVERSE = [
    ("SPY", "etf"), ("QQQ", "etf"), ("IWM", "etf"), ("DIA", "etf"),
    ("NVDA", "stocks"), ("TSLA", "stocks"), ("AAPL", "stocks"),
    ("MSFT", "stocks"), ("AMD", "stocks"), ("META", "stocks"),
    ("AMZN", "stocks"), ("GOOGL", "stocks"), ("NFLX", "stocks"),
    ("AVGO", "stocks"), ("PLTR", "stocks"), ("COIN", "stocks"),
    ("HOOD", "stocks"), ("SOFI", "stocks"), ("MU", "stocks"),
    ("INTC", "stocks"), ("BA", "stocks"), ("GME", "stocks"),
    ("AMC", "stocks"), ("RIVN", "stocks"), ("LCID", "stocks"),
    ("UBER", "stocks"), ("ABNB", "stocks"), ("SMCI", "stocks"),
    ("ARM", "stocks"), ("LLY", "stocks"),
]

ETF_SET = {"SPY", "QQQ", "IWM", "DIA", "VTI", "TLT", "GLD", "SLV", "HYG", "LQD",
           "XLF", "XLK", "XLE", "XLV", "SMH", "SOXX", "ARKK", "VXX", "UVXY",
           "EWJ", "FXI", "MCHI", "KWEB", "EEM", "VGK", "QQQM", "SPX", "NDX"}


def to_int(v):
    if v is None:
        return 0
    s = str(v).replace(",", "").strip()
    if s in ("", "--", "N/A", "-"):
        return 0
    try:
        return int(float(s))
    except ValueError:
        return 0


def to_float(v):
    if v is None:
        return 0.0
    s = str(v).replace(",", "").replace("$", "").strip()
    if s in ("", "--", "N/A", "-"):
        return 0.0
    try:
        return float(s)
    except ValueError:
        return 0.0


def parse_last_trade(text):
    """从 'LAST TRADE: $342.56 (AS OF NOV 20, 2026)' 提取标的价格与数据日期。"""
    price, asof = 0.0, None
    if not text:
        return price, asof
    m = re.search(r"\$\s*([\d,.]+)", text)
    if m:
        price = to_float(m.group(1))
    m = re.search(r"AS OF\s+([A-Z][a-z]{2}\s+\d{1,2},\s*\d{4})", text, re.I)
    if m:
        try:
            asof = datetime.strptime(m.group(1), "%b %d, %Y").date().isoformat()
        except ValueError:
            pass
    return price, asof


def market_phase(now_et):
    """美东时间的市场阶段。用于标注数据快照的真实时点（盘前/盘中/盘后/休市）。"""
    if now_et.weekday() >= 5:
        return "休市"
    hm = now_et.hour * 60 + now_et.minute
    if hm < 9 * 60 + 30:
        return "盘前"
    if hm < 16 * 60:
        return "盘中"
    if hm < 20 * 60:
        return "盘后"
    return "休市"


def last_closed_session(now_et):
    """最近一个已完成收盘(16:00 ET)的交易日，跳过周末。"""
    d = now_et.date()
    if now_et.hour < 16:
        d -= timedelta(days=1)
    while d.weekday() >= 5:
        d -= timedelta(days=1)
    return d


def prev_session(d):
    """上一个交易日（跳过周末）。"""
    d = d - timedelta(days=1)
    while d.weekday() >= 5:
        d -= timedelta(days=1)
    return d


def load_prev_snapshot(path):
    """读上次数据文件，返回 (OI 指纹字典, meta)。

    Nasdaq 免费接口不提供持仓量的结算日期字段：响应里只有 lastTrade（标的股票
    最近一笔成交的报价时点），没有 OI 的数据日。因此无法直接读出"这批 OI 是哪天
    结算的"，只能用内容指纹反推：把本次 OI 与上次快照逐条比对，变化足够多才说明
    源端确实把新一天的清算数据推上来了。
    """
    try:
        s = path.read_text(encoding="utf-8")
        m = re.search(r"window\.OPTIONS_DATA\s*=\s*(\{.*\})\s*;", s, re.S)
        if not m:
            return {}, {}
        d = json.loads(m.group(1))
        syms, exps, cons = d.get("symbols") or [], d.get("expiries") or [], d.get("contracts") or []
        prev = {}
        for c in cons:
            prev[(syms[c[0]]["s"], c[1], exps[c[2]], round(c[3], 2))] = c[4]
        return prev, (d.get("meta") or {})
    except Exception:
        return {}, {}


def oi_change_stats(contracts, symbols, expiries, prev):
    """对比本次与上次的 OI，返回 (变化率, 变化条数, 可比条数, 变化样本)。"""
    if not prev:
        return None, 0, 0, []
    same = diff = 0
    samples = []
    for c in contracts:
        k = (symbols[c[0]]["s"], c[1], expiries[c[2]], round(c[3], 2))
        if k in prev:
            if prev[k] == c[4]:
                same += 1
            else:
                diff += 1
                if len(samples) < 12:
                    samples.append((k, prev[k], c[4]))
    total = same + diff
    if total == 0:
        return None, 0, 0, []
    return diff / total, diff, total, samples


def parse_expiry(label, base_date):
    """把 'Nov 23' / 'Jan 15' 解析为完整日期。无年份时按基准日推断(早于基准月=次年)。"""
    if not label:
        return None
    label = label.strip()
    for fmt in ("%b %d", "%B %d"):
        try:
            dt = datetime.strptime("2000 " + label, "%Y " + fmt)
            break
        except ValueError:
            continue
    else:
        return None
    if isinstance(base_date, datetime):
        base = base_date
    elif isinstance(base_date, date):
        base = datetime(base_date.year, base_date.month, base_date.day)
    else:
        base = datetime.strptime(str(base_date), "%Y-%m-%d")
    year = base.year + (1 if dt.month < base.month else 0)
    try:
        return f"{year:04d}-{dt.month:02d}-{dt.day:02d}"
    except ValueError:
        return None


def fetch_symbol(session, sym, assetclass, fromdate, todate, retries=3):
    url = BASE.format(sym=sym)
    params = {"assetclass": assetclass, "limit": 5000, "type": "all",
              "fromdate": fromdate, "todate": todate}
    last_err = None
    for i in range(retries):
        try:
            r = session.get(url, params=params, timeout=35)
            if r.status_code == 404:
                alt = "stocks" if assetclass == "etf" else "etf"
                params["assetclass"] = alt
                r = session.get(url, params=params, timeout=35)
            r.raise_for_status()
            data = (r.json() or {}).get("data") or {}
            rows = (data.get("table") or {}).get("rows") or []
            if not rows:
                return None, f"{sym}: 接口无数据行"
            return data, None
        except Exception as e:
            last_err = e
            time.sleep(1.2 * (i + 1))
    return None, f"{sym}: 抓取失败 {last_err}"


def main():
    ap = argparse.ArgumentParser(description="抓取 Nasdaq 美股期权链未平仓数据")
    ap.add_argument("--tickers", help="自定义标的，逗号分隔，如 AAPL,TSLA,SPY")
    ap.add_argument("--min-oi", type=int, default=100,
                    help="保留 OI>=阈值的合约，默认 100")
    ap.add_argument("--days", type=int, default=60, help="到期日窗口向后天数，默认 60")
    ap.add_argument("--out", default="options_data.js", help="输出文件名")
    args = ap.parse_args()

    if args.tickers:
        universe = [(t.strip().upper(),
                     "etf" if t.strip().upper() in ETF_SET else "stocks")
                    for t in args.tickers.split(",") if t.strip()]
    else:
        universe = DEFAULT_UNIVERSE

    now_et = datetime.now(timezone(timedelta(hours=-5)))
    today = now_et.date()                                        # 美东日期
    session_date = last_closed_session(now_et)                   # 最近已收盘交易日
    prev_day = prev_session(session_date)                        # 它的上一个交易日
    fromdate = (today - timedelta(days=2)).isoformat()
    todate = (today + timedelta(days=args.days)).isoformat()
    out_path = Path(__file__).with_name(args.out)

    session = requests.Session()
    session.headers.update(HEADERS)

    # 第一遍: 抓取全部标的原始数据,统计 asOf 众数作为统一数据基准日
    # (盘前/周末接口个别标的会误报当日,多数标的报最近交易日收盘,以众数为准)
    fetched = []
    errors = []
    print(f"抓取窗口: {fromdate} -> {todate} | 标的数: {len(universe)} | 美东今日: {today}")
    for idx, (sym, ac) in enumerate(universe, 1):
        data, err = fetch_symbol(session, sym, ac, fromdate, todate)
        if err:
            errors.append(err)
            print(f"[{idx:02d}/{len(universe)}] {sym:<6} ERROR {err}")
            continue
        underlying, asof_raw = parse_last_trade(data.get("lastTrade"))
        fetched.append((sym, ac, data, underlying, asof_raw))
        print(f"[{idx:02d}/{len(universe)}] {sym:<6} 标的价 {underlying:>9.2f}  接口数据日 {asof_raw}")
        time.sleep(0.25)

    if not fetched:
        sys.exit("未抓到任何合约数据，请检查网络或接口可用性。")

    # 标的报价时点仅作参考：lastTrade 是股票最近一笔成交的报价时间戳，会随盘前/盘后
    # 报价跳动，不能当作期权持仓量的结算日期。
    from collections import Counter
    quote_counter = Counter(a for _, _, _, _, a in fetched if a)
    print(f"\n标的报价时点分布(仅供参考): {dict(quote_counter)}")

    # 第二遍: 用统一基准日解析到期日,构建紧凑列式数据
    symbols, expiries = [], []
    sym_idx, exp_idx = {}, {}
    contracts = []           # [symIdx, side(0=CALL/1=PUT), expIdx, strike, oi, vol, last, bid, ask]

    def sym_id(sym, ac, price, asof):
        if sym not in sym_idx:
            sym_idx[sym] = len(symbols)
            symbols.append({"s": sym, "a": ac, "p": round(price, 4), "as": asof})
        return sym_idx[sym]

    def exp_id(exp):
        if exp not in exp_idx:
            exp_idx[exp] = len(expiries)
            expiries.append(exp)
        return exp_idx[exp]

    for sym, ac, data, underlying, quote_asof in fetched:
        sid = sym_id(sym, ac, underlying, quote_asof or "")
        rows = [x for x in data.get("table", {}).get("rows", []) if x.get("expiryDate")]
        cnt = 0
        for r in rows:
            expiry = parse_expiry(r.get("expiryDate"), today)
            if not expiry:
                continue
            eid = exp_id(expiry)
            strike = to_float(r.get("strike"))
            if strike <= 0:
                continue
            for side_i, oi_k, vol_k, last_k, bid_k, ask_k in (
                (0, "c_Openinterest", "c_Volume", "c_Last", "c_Bid", "c_Ask"),
                (1, "p_Openinterest", "p_Volume", "p_Last", "p_Bid", "p_Ask"),
            ):
                oi = to_int(r.get(oi_k))
                if oi <= 0 or oi < args.min_oi:
                    continue
                contracts.append([
                    sid, side_i, eid, round(strike, 4), oi, to_int(r.get(vol_k)),
                    round(to_float(r.get(last_k)), 4), round(to_float(r.get(bid_k)), 4),
                    round(to_float(r.get(ask_k)), 4),
                ])
                cnt += 1
        print(f"  {sym:<6} 合约 {cnt:>5}")

    # ---- 判定 OI 数据日：看内容指纹，不看标的报价时点 ----
    # 源端推送新一天的清算数据时，会有可观比例的合约 OI 发生变化（实测 10%~30%）；
    # 同一时点重复抓取时几乎为 0。据此判断"这批 OI 到底是哪天结算的"。
    prev_oi, prev_meta = load_prev_snapshot(out_path)
    ratio, diff_cnt, matched, samples = oi_change_stats(contracts, symbols, expiries, prev_oi)
    prev_asof = (prev_meta or {}).get("asOf")
    OI_UPDATE_MIN_RATIO = 0.02

    close_dt = datetime.combine(session_date, datetime.min.time()).replace(hour=16)
    hours_since_close = max((now_et.replace(tzinfo=None) - close_dt).total_seconds() / 3600.0, 0.0)

    if ratio is not None and ratio >= OI_UPDATE_MIN_RATIO:
        data_date, oi_state = session_date.isoformat(), "已更新"
    elif prev_asof and session_date.isoformat() <= prev_asof:
        data_date, oi_state = prev_asof, "已最新"
    elif prev_asof:
        data_date, oi_state = prev_asof, "源端未更新"
    elif hours_since_close >= 16:
        # 首次运行无对比基准：按收盘后经过时间推断（源端通常在收盘后 12~16 小时内发布完）
        data_date, oi_state = session_date.isoformat(), "推定已更新"
    else:
        data_date, oi_state = prev_day.isoformat(), "推定未更新"

    pending = session_date.isoformat() > data_date
    ratio_txt = "无基准" if ratio is None else f"{ratio*100:.1f}%"
    print(f"\nOI 数据日判定: {data_date} | 状态={oi_state} | "
          f"OI 变化率={ratio_txt} ({diff_cnt}/{matched} 条) | 收盘后 {hours_since_close:.1f}h")
    if pending:
        print(f"  注意: {session_date} 的结算数据源端尚未完整发布，当前为 {data_date} 的持仓")
    for k, old, new in samples[:6]:
        sym, side, exp, ks = k
        print(f"    {sym} {'CALL' if side == 0 else 'PUT '} {exp} K={ks:g}: OI {old} -> {new}")
    if samples:
        print(f"    ...（共 {diff_cnt} 条 OI 发生变化）")

    payload = {
        "meta": {
            "source": "Nasdaq 公开期权链 API (api.nasdaq.com)",
            "sourceUrl": "https://www.nasdaq.com/market-activity/options/chain",
            "fetchedAt": datetime.now(timezone(timedelta(hours=8))).strftime("%Y-%m-%d %H:%M:%S CST"),
            "asOf": data_date,  # OI 数据的结算交易日（由内容指纹判定，非标的报价日）
            "oiState": oi_state,
            "oiChangeRatio": None if ratio is None else round(ratio, 4),
            "oiChangedContracts": diff_cnt,
            "oiComparedContracts": matched,
            "sessionPending": pending,
            "quoteAsOfDist": dict(quote_counter),
            "marketPhase": market_phase(now_et),
            "lastSession": session_date.isoformat(),
            "window": {"from": fromdate, "to": todate},
            "minOi": args.min_oi,
            "universeSize": len(universe),
            "contractCount": len(contracts),
            "symbolsCount": len(symbols),
            "errors": errors,
            "note": ("免费源不提供持仓量的结算日期字段，asOf 由 OI 内容指纹判定："
                     "与上次快照比对，变化率达标即认为源端已推送新交易日清算数据。"
                     "免费源无逐笔大单流；估算名义金额=OI×100×行权价，仅供量级参考。"),
        },
        "symbols": symbols,       # [{s, a, p, as}]  as=标的报价时点
        "expiries": expiries,     # ["YYYY-MM-DD", ...]
        "contracts": contracts,   # [symIdx, side, expIdx, strike, oi, vol, last, bid, ask]
    }

    js = "window.OPTIONS_DATA = " + json.dumps(payload, ensure_ascii=False, separators=(",", ":")) + ";\n"
    # 原子写入: 先写临时文件再替换,避免服务运行期间浏览器读到写了一半的数据
    tmp_path = out_path.with_name(out_path.name + ".tmp")
    tmp_path.write_text(js, encoding="utf-8")
    os.replace(tmp_path, out_path)
    print(f"\n完成: {out_path} ({out_path.stat().st_size/1024:.0f} KB), 合约 {len(contracts)} 条")
    if errors:
        print("失败标的:", ", ".join(errors))


if __name__ == "__main__":
    main()
