#!/usr/bin/env python3
"""
Бэктест скринера на реальной ленте сделок текущей сессии FORTS (MOEX ISS).

Скачивает все сделки сессии, прогоняет их через тот же детектор, что работает
в бою, и строит HTML-отчёт: алерты, которые пришли бы, и отброшенные кандидаты
(с причиной), у каждого — график сделок вокруг момента.

В истории ISS нет стакана (бид/аск), поэтому прогон идёт в режиме «по сделкам».

  python backtest.py                          # скачать ленту и построить отчёт
  python backtest.py --load tape.json.gz      # прогнать сохранённую ленту
  python backtest.py --grid THRESHOLD=1,1.2,1.5 MIN_LOTS=2,5   # сравнить настройки
"""
from __future__ import annotations

import argparse
import gzip
import html
import itertools
import json
import os
import sys
import time
from bisect import bisect_left, bisect_right
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime

import moex_spike_screener as m


# ───────── лента ─────────

def download(client: m.MoexClient, workers: int = 4) -> list[dict]:
    n = client.count_trades()
    starts = list(range(0, n, m.PAGE_LIMIT))
    m.log(f"[bt] сделок в сессии: {n}, страниц: {len(starts)}")
    done = [0]

    def page(st):
        rows = client.trades(st)
        done[0] += 1
        if done[0] % 10 == 0:
            m.log(f"[bt] скачано страниц: {done[0]}/{len(starts)}")
        return rows

    with ThreadPoolExecutor(workers) as ex:
        pages = list(ex.map(page, starts))
    tape = [r for p in pages for r in p]
    # хвост мог прирасти, пока качали: дочитываем
    while True:
        more = client.trades(len(tape))
        if not more:
            break
        tape += more
    return tape


# ───────── прогон ─────────

class Capture(m.Notifier):
    def __init__(self):
        super().__init__(None, None, None)
        self.got: list[m.Alert] = []

    def send(self, a):
        self.got.append(a)

    def send_text(self, text):
        return False


def replay(tape: list[dict], instr: dict, env: dict) -> tuple[list, list, dict]:
    saved = {k: os.environ.get(k) for k in env}
    os.environ.update({k: str(v) for k, v in env.items()})
    try:
        sys.argv = ["bt"]
        args = m.parse_args()
        cap = Capture()
        scr = m.Screener(args, None, cap)
        scr.instr = instr
        scr.instr_at = time.time() + 10 ** 9           # не обновлять справочник
        scr.refresh_instruments = lambda force=False: None
        # US-группа по базовому активу (как в бою при обновлении справочника)
        for secid, info in instr.items():
            if info.get("kind") == "fut" and (info.get("asset") or "").upper() in scr.us_assets:
                scr.us.add(secid.upper())
                m.US_PERPS.add(secid.upper())
                scr.det.ref_age_override[secid] = scr.us_age
        drops = []
        orig = m.SpikeConfirmer._log_drop

        def rec(p, why):
            if p.can_alert:
                drops.append((p, why))
        m.SpikeConfirmer._log_drop = staticmethod(rec)
        m.log = lambda msg: None
        try:
            for r in tape:
                if r.get("OFFMARKETDEAL"):
                    continue
                secid, price, st = r.get("SECID"), r.get("PRICE"), r.get("SYSTIME")
                if not secid or not price or not st:
                    continue
                last_ts = m.parse_ts(st)
                scr.handle_trade(secid, last_ts, float(price),
                                 int(r.get("QUANTITY") or 0), r.get("BUYSELL") or "")
            if tape:
                for done in scr.conf.flush(last_ts + 3600):   # иголки в самом конце ленты
                    scr._emit(*done)
        finally:
            m.SpikeConfirmer._log_drop = orig
        return cap.got, drops, dict(scr.conf.dropped)
    finally:
        for k, v in saved.items():
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v


# ───────── отчёт ─────────

def index_tape(tape):
    by = {}
    for r in tape:
        if r.get("OFFMARKETDEAL") or not r.get("PRICE"):
            continue
        by.setdefault(r["SECID"], []).append((m.parse_ts(r["SYSTIME"]), float(r["PRICE"]),
                                              int(r.get("QUANTITY") or 0), r.get("BUYSELL") or ""))
    return by


def svg_chart(rows, t0, ref, peak, back=None, before=20, after=25, w=560, h=150) -> str:
    ts = [r[0] for r in rows]
    lo_i, hi_i = bisect_left(ts, t0 - before), bisect_right(ts, t0 + after)
    pts = rows[lo_i:hi_i]
    if not pts:
        return ""
    ps = [p for _, p, _, _ in pts] + [ref, peak] + ([back] if back else [])
    pmin, pmax = min(ps), max(ps)
    pad = (pmax - pmin) * 0.08 or pmax * 0.001
    pmin, pmax = pmin - pad, pmax + pad
    X = lambda t: 34 + (t - (t0 - before)) / (before + after) * (w - 44)
    Y = lambda p: 8 + (pmax - p) / (pmax - pmin) * (h - 26)
    out = [f'<svg viewBox="0 0 {w} {h}" class="ch" role="img">']
    for val, cls in ((ref, "ref"), (peak, "pk")) + (((back, "bk"),) if back else ()):
        out.append(f'<line x1="34" x2="{w-10}" y1="{Y(val):.1f}" y2="{Y(val):.1f}" class="{cls}"/>')
    out.append(f'<line x1="{X(t0):.1f}" x2="{X(t0):.1f}" y1="6" y2="{h-18}" class="t0"/>')
    path = " ".join(f"{X(t):.1f},{Y(p):.1f}" for t, p, _, _ in pts)
    out.append(f'<polyline points="{path}" class="ln"/>')
    for t, p, q, sd in pts:
        r_ = min(1.5 + q ** 0.5 * 0.4, 6)
        out.append(f'<circle cx="{X(t):.1f}" cy="{Y(p):.1f}" r="{r_:.1f}" class="{ "b" if sd == "B" else "s" if sd == "S" else "u"}"/>')
    for k in (-before, 0, after):
        out.append(f'<text x="{X(t0 + k):.1f}" y="{h-4}" class="ax">{k:+d}с</text>')
    out.append(f'<text x="2" y="{Y(pmax - pad)+4:.1f}" class="ax">{m.fmt_price(pmax - pad)}</text>')
    out.append(f'<text x="2" y="{Y(pmin + pad):.1f}" class="ax">{m.fmt_price(pmin + pad)}</text>')
    out.append("</svg>")
    return "".join(out)


CSS = """
:root{--bg:#f6f7f9;--fg:#15171a;--mut:#666c75;--card:#fff;--line:#e3e5e8;--up:#15803d;--dn:#b91c1c;--acc:#2563eb}
@media (prefers-color-scheme: dark){:root:not([data-theme="light"]){--bg:#0f1115;--fg:#e6e6e6;--mut:#8a8f98;--card:#171a21;--line:#262a33;--up:#22c55e;--dn:#ef4444;--acc:#60a5fa}}
body{margin:0;background:var(--bg);color:var(--fg);font:14px/1.45 system-ui,sans-serif}
main{max-width:980px;margin:0 auto;padding:16px}
h1{font-size:20px;margin:4px 0 2px}h2{font-size:15px;margin:26px 0 8px}
.mut{color:var(--mut)}.up{color:var(--up)}.dn{color:var(--dn)}
.grid{display:grid;gap:12px;grid-template-columns:repeat(auto-fill,minmax(300px,1fr))}
.card{background:var(--card);border:1px solid var(--line);border-radius:10px;padding:10px 12px}
.card b{font-variant-numeric:tabular-nums}
.ch{width:100%;height:auto;display:block;margin-top:6px}
.ch .ln{fill:none;stroke:var(--mut);stroke-width:1;opacity:.6}
.ch .b{fill:var(--up)}.ch .s{fill:var(--dn)}.ch .u{fill:var(--mut)}
.ch .ref{stroke:var(--mut);stroke-dasharray:3 3}.ch .pk{stroke:var(--acc);stroke-dasharray:2 4}
.ch .bk{stroke:var(--up);stroke-dasharray:1 3}.ch .t0{stroke:var(--line)}
.ch .ax{font-size:10px;fill:var(--mut)}
table{border-collapse:collapse;width:100%;font-variant-numeric:tabular-nums;background:var(--card);border:1px solid var(--line);border-radius:10px;overflow:hidden}
td,th{padding:6px 10px;text-align:right;border-top:1px solid var(--line)}th{color:var(--mut);font-weight:500}
td:first-child,th:first-child{text-align:left}
.tag{display:inline-block;padding:0 6px;border-radius:6px;background:var(--line);font-size:12px}
"""


def card(sec_rows, secid, name, t0, ref, peak, vol, back=None, note=""):
    pct = (peak / ref - 1) * 100
    cls = "up" if pct > 0 else "dn"
    tm = datetime.fromtimestamp(t0, m.MSK).strftime("%H:%M:%S")
    path = f"{m.fmt_price(ref)} → {m.fmt_price(peak)}" + (f" → {m.fmt_price(back)}" if back else "")
    return (f'<div class="card"><div><b>{html.escape(secid)}</b> <span class="mut">{html.escape(name)}</span> '
            f'<b class="{cls}">{pct:+.2f}%</b> <span class="mut">{tm}</span></div>'
            f'<div class="mut">{path} · {vol} лот{(" · " + html.escape(note)) if note else ""}</div>'
            f'{svg_chart(sec_rows.get(secid, []), t0, ref, peak, back)}</div>')


def report(tape, instr, alerts, drops, counts, env, grid_rows, path):
    by = index_tape(tape)
    t_first = tape[0]["SYSTIME"] if tape else "—"
    t_last = tape[-1]["SYSTIME"] if tape else "—"
    parts = [f"<!doctype html><html lang='ru'><head><meta charset='utf-8'>"
             f"<meta name='viewport' content='width=device-width,initial-scale=1'>"
             f"<title>Бэктест скринера</title><style>{CSS}</style></head><body><main>",
             f"<h1>Бэктест скринера прострелов</h1>",
             f"<div class='mut'>Лента FORTS {html.escape(t_first)} — {html.escape(t_last[11:])}, "
             f"{len(tape):,} сделок. ".replace(",", " ") +
             f"Режим без стакана (в истории ISS его нет). "
             f"Настройки: {html.escape(', '.join(f'{k}={v}' for k, v in env.items()) or 'по умолчанию')}</div>"]
    if grid_rows:
        reasons = list(grid_rows[0][2].keys())
        parts.append("<h2>Сравнение настроек</h2><table><tr><th>Настройки</th><th>Алертов</th>"
                     + "".join(f"<th>Отсеяно: {html.escape(r)}</th>" for r in reasons) + "</tr>")
        for label, na, c in grid_rows:
            parts.append(f"<tr><td>{html.escape(label)}</td><td>{na}</td>"
                         + "".join(f"<td>{c.get(r, 0)}</td>" for r in reasons) + "</tr>")
        parts.append("</table>")
    parts.append(f"<h2>Алерты, которые пришли бы: {len(alerts)}</h2>")
    parts.append("<div class='mut'>Отсеяно кандидатов: " + html.escape(
        ", ".join(f"{k} — {v}" for k, v in counts.items())) + ".</div>")
    parts.append("<div class='mut'>Серая пунктирная линия — рынок до прострела, синяя — пик, зелёная — откат. "
                 "Точки — сделки (зелёные покупки, красные продажи, размер — объём).</div><div class='grid'>")
    for a in alerts:
        t0 = a.seen.timestamp() - a.lag_s          # время начала прострела
        parts.append(card(by, a.secid, a.name, t0, a.ref, a.price, a.qty, a.back,
                          (f"откат за {a.back_s:.0f} с" if a.back_s and a.back_s >= 1 else "откат за <1 с")
                          + (" · на открытии" if a.opening else "")))
    parts.append("</div>")
    near = sorted(drops, key=lambda d: -abs(d[0].ext / d[0].ref - 1))[:40]
    parts.append(f"<h2>Отброшенные кандидаты: {len(drops)} (крупнейшие {len(near)})</h2>"
                 f"<div class='mut'>Здесь удобно искать пропуски: если среди них есть настоящая иголка — "
                 f"видно, какое условие её отрезало.</div><div class='grid'>")
    for p, why in near:
        parts.append(card(by, p.secid, instr.get(p.secid, {}).get("name", p.secid), p.start,
                          p.ref, p.ext, p.vol, None, why))
    parts.append("</div></main></body></html>")
    with open(path, "w", encoding="utf-8") as f:
        f.write("".join(parts))


# ───────── main ─────────

def main():
    ap = argparse.ArgumentParser(description="Бэктест скринера на ленте текущей сессии")
    ap.add_argument("--load", help="прогнать сохранённую ленту (.json.gz)")
    ap.add_argument("--save", default="tape.json.gz", help="куда сохранить скачанную ленту")
    ap.add_argument("--out", default="backtest_report.html", help="HTML-отчёт")
    ap.add_argument("--grid", nargs="*", default=[], help="сравнить настройки: THRESHOLD=1,1.2 MIN_LOTS=2,5")
    ap.add_argument("--set", nargs="*", default=[], help="настройки для отчёта: THRESHOLD=1.2 ...")
    a = ap.parse_args()

    client = m.MoexClient()
    orig_get = m.MoexClient.get
    m.MoexClient.get = lambda self, p, pa, retries=12: orig_get(self, p, pa, retries)
    if a.load:
        with gzip.open(a.load, "rt", encoding="utf-8") as f:
            data = json.load(f)
        tape, instr = data["tape"], data["instr"]
    else:
        instr = client.instruments()
        tape = download(client)
        with gzip.open(a.save, "wt", encoding="utf-8") as f:
            json.dump({"tape": tape, "instr": instr}, f)
        m.log(f"[bt] лента сохранена: {a.save}")
    log = m.log

    env = dict(kv.split("=", 1) for kv in a.set)
    grid_rows = []
    if a.grid:
        keys = [g.split("=", 1)[0] for g in a.grid]
        vals = [g.split("=", 1)[1].split(",") for g in a.grid]
        for combo in itertools.product(*vals):
            e = {**env, **dict(zip(keys, combo))}
            al, dr, cnt = replay(tape, instr, e)
            label = ", ".join(f"{k}={v}" for k, v in zip(keys, combo))
            grid_rows.append((label, len(al), cnt))
            m.log = log
            log(f"[bt] {label}: алертов {len(al)}, отсеяно {cnt}")
    alerts, drops, counts = replay(tape, instr, env)
    m.log = log
    report(tape, instr, alerts, drops, counts, env, grid_rows, a.out)
    log(f"[bt] алертов: {len(alerts)}, отсеяно: {counts}. Отчёт: {a.out}")
    for al in alerts:
        log("   " + m.fmt_alert(al).replace("\n", " | "))


if __name__ == "__main__":
    main()
