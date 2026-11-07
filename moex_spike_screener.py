#!/usr/bin/env python3
"""
MOEX FORTS spike screener — ловит прострелы по фьючам Мосбиржи.

Два типа сигналов:
  1. IMPULSE — резкое движение внутри скользящего окна (по умолчанию 5 мин):
     цена ушла от минимума/максимума окна на >= THRESHOLD %.
  2. DAY     — изменение к предыдущему клирингу (PREVSETTLEPRICE) пробило
     уровень 2%, 3%, 4%... (каждый уровень алертит один раз за день).

Алерты: консоль (+звук), Telegram, CSV-лог.
Данные: публичный ISS API (iss.moex.com). Без подписки котировки FORTS
могут идти с задержкой ~15 мин — см. README про MOEX_LOGIN/MOEX_PASSWORD.
"""
from __future__ import annotations

import argparse
import csv
import html as htmlmod
import json
import threading
import os
import sys
import time
from collections import defaultdict, deque
from dataclasses import dataclass, field
from datetime import datetime, date
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import requests

try:
    from rich.console import Console
    from rich.live import Live
    from rich.table import Table
    from rich.layout import Layout
    from rich.panel import Panel
    from rich.text import Text
    HAS_RICH = True
except ImportError:  # работает и без rich, просто попроще
    HAS_RICH = False

ISS_URL = "https://iss.moex.com/iss/engines/futures/markets/forts/securities.json"
PASSPORT_URL = "https://passport.moex.com/authenticate"


# ─────────────────────────── данные ───────────────────────────

@dataclass
class Quote:
    secid: str
    name: str
    asset: str
    last: float
    prev_settle: float | None
    num_trades: int
    val_today: float
    last_trade_date: str
    update_time: str


def iss_table(js: dict, key: str) -> list[dict]:
    block = js.get(key) or {}
    cols = block.get("columns", [])
    return [dict(zip(cols, row)) for row in block.get("data", [])]


class MoexClient:
    def __init__(self, login: str | None = None, password: str | None = None):
        self.s = requests.Session()
        self.s.headers["User-Agent"] = "moex-spike-screener/1.0"
        if login and password:
            r = self.s.get(PASSPORT_URL, auth=(login, password), timeout=15)
            if r.ok and "MicexPassportCert" in self.s.cookies:
                print("[auth] MOEX Passport: ok (данные без задержки, если есть подписка)")
            else:
                print(f"[auth] MOEX Passport: не удалось ({r.status_code}), работаю анонимно")

    PARAMS = {
        "iss.meta": "off",
        "iss.only": "securities,marketdata",
        "securities.columns": "SECID,SHORTNAME,ASSETCODE,PREVSETTLEPRICE,LASTTRADEDATE",
        "marketdata.columns": "SECID,LAST,NUMTRADES,VALTODAY,UPDATETIME,TIME",
    }

    def fetch(self, retries: int = 3) -> list[Quote]:
        for attempt in range(retries):
            try:
                r = self.s.get(ISS_URL, params=self.PARAMS, timeout=15)
                r.raise_for_status()
                js = r.json()
                break
            except (requests.RequestException, ValueError):
                if attempt == retries - 1:
                    raise
                time.sleep(1 + attempt)
        secs = {row["SECID"]: row for row in iss_table(js, "securities")}
        out: list[Quote] = []
        for md in iss_table(js, "marketdata"):
            secid = md.get("SECID")
            last = md.get("LAST")
            if not secid or not last:
                continue
            sec = secs.get(secid, {})
            out.append(Quote(
                secid=secid,
                name=sec.get("SHORTNAME") or secid,
                asset=sec.get("ASSETCODE") or "",
                last=float(last),
                prev_settle=float(sec["PREVSETTLEPRICE"]) if sec.get("PREVSETTLEPRICE") else None,
                num_trades=int(md.get("NUMTRADES") or 0),
                val_today=float(md.get("VALTODAY") or 0),
                last_trade_date=sec.get("LASTTRADEDATE") or "9999-12-31",
                update_time=md.get("UPDATETIME") or md.get("TIME") or "",
            ))
        return out


def front_month_only(quotes: list[Quote]) -> list[Quote]:
    """Оставляет по каждому базовому активу ближайший контракт, где есть сделки."""
    best: dict[str, Quote] = {}
    for q in quotes:
        if q.num_trades == 0:
            continue
        key = q.asset or q.secid
        cur = best.get(key)
        if cur is None or q.last_trade_date < cur.last_trade_date:
            best[key] = q
    return list(best.values())


# ─────────────────────────── детектор ───────────────────────────

@dataclass
class Alert:
    ts: datetime
    kind: str          # IMPULSE / DAY
    secid: str
    name: str
    direction: int     # +1 / -1
    pct: float         # величина движения в %
    price: float
    ref_price: float
    window_s: int
    day_pct: float | None
    val_today: float


@dataclass
class SecState:
    hist: deque = field(default_factory=deque)          # (ts, price)
    last_alert: dict = field(default_factory=dict)      # dir -> (ts, pct)
    day_levels: set = field(default_factory=set)        # уже пробитые уровни (+2, -3 ...)
    day: date | None = None
    settle: float | None = None


class SpikeDetector:
    def __init__(self, threshold: float, window_s: int, cooldown_s: int,
                 escalate: float, day_levels: bool, day_step: float):
        self.thr = threshold
        self.window_s = window_s
        self.cooldown_s = cooldown_s
        self.escalate = escalate
        self.day_levels_on = day_levels
        self.day_step = day_step
        self.state: dict[str, SecState] = defaultdict(SecState)
        self.window_move: dict[str, float] = {}          # для дашборда
        self.day_move: dict[str, float] = {}

    def update(self, q: Quote, now: float) -> list[Alert]:
        st = self.state[q.secid]
        alerts: list[Alert] = []
        today = datetime.fromtimestamp(now).date()
        if st.day != today or st.settle != q.prev_settle:
            st.day = today
            st.settle = q.prev_settle
            st.day_levels.clear()

        # история цен в окне
        h = st.hist
        if not h or h[-1][1] != q.last or now - h[-1][0] > 30:
            h.append((now, q.last))
        while h and now - h[0][0] > self.window_s:
            h.popleft()

        day_pct = None
        if q.prev_settle:
            day_pct = (q.last / q.prev_settle - 1) * 100
            self.day_move[q.secid] = day_pct

        if len(h) >= 2:
            lo_ts, lo = min(h, key=lambda x: x[1])
            hi_ts, hi = max(h, key=lambda x: x[1])
            up = (q.last / lo - 1) * 100 if lo > 0 else 0
            dn = (q.last / hi - 1) * 100 if hi > 0 else 0
            self.window_move[q.secid] = up if abs(up) >= abs(dn) else dn
            for direction, pct, ref, ref_ts in ((+1, up, lo, lo_ts), (-1, dn, hi, hi_ts)):
                if abs(pct) < self.thr:
                    continue
                prev = st.last_alert.get(direction)
                if prev:
                    p_ts, p_pct = prev
                    in_cd = now - p_ts < self.cooldown_s
                    # в кулдауне алертим только если движение расширилось
                    if in_cd and abs(pct) < abs(p_pct) + self.escalate:
                        continue
                st.last_alert[direction] = (now, pct)
                alerts.append(Alert(datetime.fromtimestamp(now), "IMPULSE", q.secid, q.name,
                                    direction, pct, q.last, ref, int(now - ref_ts),
                                    day_pct, q.val_today))

        if self.day_levels_on and day_pct is not None and abs(day_pct) >= self.thr:
            sign = 1 if day_pct > 0 else -1
            # максимальный пробитый уровень: thr, thr+step, thr+2*step ...
            n = int((abs(day_pct) - self.thr) // self.day_step)
            level = round(sign * (self.thr + n * self.day_step), 4)
            if level not in st.day_levels:
                # помечаем все уровни до текущего, чтобы не спамить пачкой
                for i in range(n + 1):
                    st.day_levels.add(round(sign * (self.thr + i * self.day_step), 4))
                alerts.append(Alert(datetime.fromtimestamp(now), "DAY", q.secid, q.name,
                                    sign, day_pct, q.last, q.prev_settle, 0,
                                    day_pct, q.val_today))
        return alerts


# ─────────────────────────── алерты ───────────────────────────

def fmt_val(v: float) -> str:
    if v >= 1e9:
        return f"{v/1e9:.1f} млрд"
    if v >= 1e6:
        return f"{v/1e6:.1f} млн"
    return f"{v:,.0f}".replace(",", " ")


def fmt_alert(a: Alert, html: bool = False) -> str:
    arrow = "🚀" if a.direction > 0 else "🩸"
    b = (lambda s: f"<b>{htmlmod.escape(s)}</b>") if html else (lambda s: s)
    name = htmlmod.escape(a.name) if html else a.name
    if a.kind == "IMPULSE":
        mins = max(a.window_s, 1) / 60
        head = f"{arrow} {b(a.secid)} ({name}) прострел {a.pct:+.2f}% за {mins:.1f} мин"
        body = f"{a.ref_price:g} → {a.price:g}"
    else:
        head = f"{arrow} {b(a.secid)} ({name}) день {a.pct:+.2f}% к клирингу"
        body = f"клиринг {a.ref_price:g} → {a.price:g}"
    tail = []
    if a.kind == "IMPULSE" and a.day_pct is not None:
        tail.append(f"день {a.day_pct:+.2f}%")
    tail.append(f"оборот {fmt_val(a.val_today)}")
    return f"{head}\n{body} | " + " | ".join(tail)


class Notifier:
    def __init__(self, tg_token: str | None, tg_chat: str | None, csv_path: str | None,
                 beep: bool):
        self.tg_token, self.tg_chat = tg_token, tg_chat
        self.csv_path = csv_path
        self.beep = beep
        self.s = requests.Session()
        if csv_path and not os.path.exists(csv_path):
            with open(csv_path, "w", newline="", encoding="utf-8") as f:
                csv.writer(f).writerow(["time", "kind", "secid", "name", "dir", "pct",
                                        "price", "ref_price", "window_s", "day_pct", "val_today"])

    def send_text(self, text: str) -> bool:
        if not (self.tg_token and self.tg_chat):
            return False
        try:
            r = self.s.post(f"https://api.telegram.org/bot{self.tg_token.strip()}/sendMessage",
                            data={"chat_id": self.tg_chat.strip(), "text": text,
                                  "parse_mode": "HTML", "disable_web_page_preview": True},
                            timeout=10)
            if r.ok:
                return True
            try:
                desc = r.json().get("description", r.text)
            except ValueError:
                desc = r.text[:200]
            hint = {401: "неверный TG_TOKEN",
                    400: "неверный TG_CHAT_ID или у бота не нажат Start",
                    403: "бот заблокирован или не добавлен в чат"}.get(r.status_code, "")
            print(f"[tg] Telegram ответил {r.status_code}: {desc} {('— ' + hint) if hint else ''}",
                  file=sys.stderr, flush=True)
        except requests.RequestException as e:
            print(f"[tg] ошибка сети: {e}", file=sys.stderr, flush=True)
        return False

    def send(self, a: Alert):
        if self.beep:
            sys.stdout.write("\a")
            sys.stdout.flush()
        if self.csv_path:
            with open(self.csv_path, "a", newline="", encoding="utf-8") as f:
                csv.writer(f).writerow([a.ts.isoformat(timespec="seconds"), a.kind, a.secid,
                                        a.name, a.direction, f"{a.pct:.3f}", a.price,
                                        a.ref_price, a.window_s,
                                        "" if a.day_pct is None else f"{a.day_pct:.3f}",
                                        a.val_today])
        self.send_text(fmt_alert(a, html=True))


# ─────────────────────────── UI ───────────────────────────

def pct_text(v: float | None, thr: float) -> "Text":
    if v is None:
        return Text("—", style="dim")
    style = "bold green" if v >= thr else "bold red" if v <= -thr else \
        "green" if v > 0 else "red" if v < 0 else ""
    return Text(f"{v:+.2f}%", style=style)


def build_dashboard(det: SpikeDetector, quotes: dict[str, Quote], feed: deque,
                    status: str, args) -> "Layout":
    def movers(src: dict[str, float], title: str) -> Table:
        t = Table(title=title, expand=True, header_style="bold cyan")
        for c in ("Тикер", "Название", "Цена", "Окно", "День", "Оборот"):
            t.add_column(c, justify="right" if c not in ("Тикер", "Название") else "left")
        items = sorted(((k, v) for k, v in src.items() if k in quotes),
                       key=lambda kv: -abs(kv[1]))[:args.top]
        for secid, _ in items:
            q = quotes[secid]
            t.add_row(secid, q.name, f"{q.last:g}",
                      pct_text(det.window_move.get(secid), args.threshold),
                      pct_text(det.day_move.get(secid), args.threshold),
                      fmt_val(q.val_today))
        return t

    feed_t = Table(title="Алерты", expand=True, header_style="bold magenta", show_lines=False)
    feed_t.add_column("Время", width=8)
    feed_t.add_column("Тип", width=7)
    feed_t.add_column("Тикер")
    feed_t.add_column("Движ.", justify="right")
    feed_t.add_column("Цена", justify="right")
    for a in list(feed)[:args.top]:
        feed_t.add_row(a.ts.strftime("%H:%M:%S"), a.kind, a.secid,
                       pct_text(a.pct, 0), f"{a.ref_price:g}→{a.price:g}")

    lay = Layout()
    lay.split_column(Layout(Panel(Text(status), title="MOEX FORTS spike screener"), size=3),
                     Layout(name="body"))
    lay["body"].split_row(
        Layout(movers(det.window_move, f"Движение за {args.window} мин")),
        Layout(movers(det.day_move, "К клирингу")),
        Layout(feed_t),
    )
    return lay


# ─────────────────────────── веб-дашборд (для хостинга) ───────────────────────────

PAGE = """<!doctype html><html lang="ru"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>MOEX прострелы</title><style>
:root{--bg:#0f1115;--fg:#e6e6e6;--mut:#8a8f98;--card:#171a21;--line:#262a33;--up:#22c55e;--dn:#ef4444}
@media (prefers-color-scheme: light){:root{--bg:#f6f7f9;--fg:#111;--mut:#666;--card:#fff;--line:#e3e5e8;--up:#15803d;--dn:#b91c1c}}
body{margin:0;background:var(--bg);color:var(--fg);font:14px/1.4 system-ui,sans-serif}
header{padding:12px 16px;border-bottom:1px solid var(--line)}h1{font-size:17px;margin:0 0 4px}
#st{color:var(--mut);font-size:12px}main{display:grid;gap:16px;padding:16px;grid-template-columns:repeat(auto-fit,minmax(320px,1fr))}
section{background:var(--card);border:1px solid var(--line);border-radius:10px;overflow:auto}
h2{font-size:13px;margin:0;padding:10px 12px;border-bottom:1px solid var(--line);color:var(--mut);text-transform:uppercase;letter-spacing:.04em}
table{width:100%;border-collapse:collapse;font-variant-numeric:tabular-nums}td,th{padding:6px 12px;text-align:right;white-space:nowrap}
th{color:var(--mut);font-weight:500;font-size:12px}td:first-child,th:first-child{text-align:left}tr+tr td{border-top:1px solid var(--line)}
.up{color:var(--up)}.dn{color:var(--dn)}.b{font-weight:700}
</style></head><body><header><h1>MOEX FORTS — прострелы</h1><div id="st">загрузка…</div></header>
<main><section><h2>Алерты</h2><table id="al"></table></section>
<section><h2 id="wt">Движение за окно</h2><table id="wm"></table></section>
<section><h2>К клирингу</h2><table id="dm"></table></section></main>
<script>
const e=s=>String(s).replace(/[&<>"]/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;'}[c]));
let T=2;const p=v=>v==null?'—':`<span class="${v>0?'up':v<0?'dn':''} ${Math.abs(v)>=T?'b':''}">${v>0?'+':''}${v.toFixed(2)}%</span>`;
function mv(rows){return '<tr><th>Тикер</th><th>Цена</th><th>Окно</th><th>День</th><th>Оборот</th></tr>'+rows.map(r=>`<tr><td>${e(r.secid)} <span style="color:var(--mut)">${e(r.name)}</span></td><td>${r.last}</td><td>${p(r.win)}</td><td>${p(r.day)}</td><td>${e(r.val)}</td></tr>`).join('')}
async function load(){try{const d=await (await fetch('api/state')).json();T=d.threshold;
document.getElementById('st').textContent=d.status;document.getElementById('wt').textContent='Движение за '+d.window+' мин';
document.getElementById('al').innerHTML='<tr><th>Время</th><th>Тип</th><th>Тикер</th><th>Движ.</th><th>Цена</th></tr>'+(d.alerts.length?d.alerts.map(a=>`<tr><td>${e(a.time)}</td><td>${e(a.kind)}</td><td>${e(a.secid)}</td><td>${p(a.pct)}</td><td>${a.ref}→${a.price}</td></tr>`).join(''):'<tr><td colspan=5 style="color:var(--mut)">пока тихо</td></tr>');
document.getElementById('wm').innerHTML=mv(d.window_top);document.getElementById('dm').innerHTML=mv(d.day_top);
}catch(x){document.getElementById('st').textContent='нет связи с сервером'}}
load();setInterval(load,5000);
</script></body></html>"""


def start_web(port: int, get_state):
    class H(BaseHTTPRequestHandler):
        def do_GET(self):
            path = self.path.split("?")[0]
            if path in ("/health", "/healthz"):
                body, ctype = b"ok", "text/plain"
            elif path == "/api/state":
                body, ctype = json.dumps(get_state(), ensure_ascii=False).encode(), "application/json"
            elif path == "/":
                body, ctype = PAGE.encode(), "text/html; charset=utf-8"
            else:
                self.send_error(404)
                return
            self.send_response(200)
            self.send_header("Content-Type", ctype)
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Cache-Control", "no-store")
            self.end_headers()
            if self.command != "HEAD":
                self.wfile.write(body)

        do_HEAD = do_GET

        def log_message(self, *a):
            pass

    srv = ThreadingHTTPServer(("0.0.0.0", port), H)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    print(f"[web] дашборд на порту {port}", flush=True)


# ─────────────────────────── main ───────────────────────────

def parse_args():
    p = argparse.ArgumentParser(description="Скринер прострелов по фьючам MOEX")
    p.add_argument("-t", "--threshold", type=float, default=float(os.getenv("THRESHOLD", 2.0)),
                   help="порог прострела в %% (по умолч. 2)")
    p.add_argument("-w", "--window", type=float, default=float(os.getenv("WINDOW_MIN", 5)),
                   help="окно импульса в минутах (по умолч. 5)")
    p.add_argument("-i", "--interval", type=float, default=float(os.getenv("INTERVAL", 5)),
                   help="период опроса в секундах (по умолч. 5)")
    p.add_argument("--cooldown", type=float, default=15,
                   help="кулдаун повторного алерта по тикеру/направлению, мин (по умолч. 15)")
    p.add_argument("--escalate", type=float, default=1.0,
                   help="в кулдауне алертить снова, если движение выросло ещё на N %% (по умолч. 1)")
    p.add_argument("--no-day", action="store_true", help="выключить дневные алерты к клирингу")
    p.add_argument("--day-step", type=float, default=1.0,
                   help="шаг дневных уровней после порога, %% (2,3,4...; по умолч. 1)")
    p.add_argument("--min-trades", type=int, default=int(os.getenv("MIN_TRADES", 30)),
                   help="минимум сделок за день, чтобы отсечь неликвид (по умолч. 30)")
    p.add_argument("--min-value", type=float, default=0,
                   help="минимальный оборот за день в рублях (по умолч. 0)")
    p.add_argument("--front", action="store_true",
                   help="только ближайший контракт по каждому активу")
    p.add_argument("--only", default="", help="только эти тикеры/активы через запятую (Si,BR,MX...)")
    p.add_argument("--exclude", default="", help="исключить тикеры/активы через запятую")
    p.add_argument("--top", type=int, default=20, help="строк в таблицах дашборда")
    p.add_argument("--csv", default="alerts.csv", help="файл лога алертов ('' — выкл)")
    p.add_argument("--no-beep", action="store_true", help="без звука")
    p.add_argument("--plain", action="store_true", help="без дашборда, только лог алертов")
    p.add_argument("--test-alert", action="store_true",
                   help="отправить тестовый алерт (проверить Telegram) и выйти")
    return p.parse_args()


def main():
    args = parse_args()
    notifier = Notifier(os.getenv("TG_TOKEN"), os.getenv("TG_CHAT_ID"), args.csv or None,
                        beep=not args.no_beep)

    if args.test_alert:
        a = Alert(datetime.now(), "IMPULSE", "SiZ6", "Si-12.26", 1, 2.34, 102340, 100000, 180,
                  2.1, 5.2e9)
        print(fmt_alert(a))
        notifier.send(a)
        print("Тестовый алерт отправлен" + ("" if os.getenv("TG_TOKEN") else
                                            " (TG_TOKEN не задан — только консоль/CSV)"))
        return

    client = MoexClient(os.getenv("MOEX_LOGIN"), os.getenv("MOEX_PASSWORD"))
    det = SpikeDetector(args.threshold, int(args.window * 60), int(args.cooldown * 60),
                        args.escalate, not args.no_day, args.day_step)
    only = {x.strip().upper() for x in args.only.split(",") if x.strip()}
    excl = {x.strip().upper() for x in args.exclude.split(",") if x.strip()}

    use_rich = HAS_RICH and not args.plain and sys.stdout.isatty()
    console = Console() if HAS_RICH else None
    feed: deque[Alert] = deque(maxlen=200)
    quotes: dict[str, Quote] = {}
    errors = 0
    first_tick = True
    startup: list[Alert] = []
    status_line = "старт…"
    lock = threading.Lock()

    def get_state() -> dict:
        qs, wm, dm = dict(quotes), dict(det.window_move), dict(det.day_move)

        def top(src):
            items = sorted(((k, v) for k, v in src.items() if k in qs), key=lambda kv: -abs(kv[1]))
            return [{"secid": k, "name": qs[k].name, "last": qs[k].last,
                     "win": wm.get(k), "day": dm.get(k),
                     "val": fmt_val(qs[k].val_today)} for k, _ in items[:args.top]]
        with lock:
            return {"status": status_line, "threshold": args.threshold, "window": args.window,
                    "alerts": [{"time": a.ts.strftime("%d.%m %H:%M:%S"), "kind": a.kind,
                                "secid": a.secid, "pct": a.pct, "price": a.price,
                                "ref": a.ref_price} for a in list(feed)[:100]],
                    "window_top": top(wm), "day_top": top(dm)}

    port = os.getenv("PORT")
    if port:
        start_web(int(port), get_state)

    def log(msg: str):
        if use_rich:
            return  # в rich-режиме всё видно в дашборде
        print(f"{datetime.now():%H:%M:%S} {msg}", flush=True)

    def tick() -> str:
        nonlocal status_line
        new = _tick()
        with lock:
            status_line = new
        return new

    def _tick() -> str:
        nonlocal errors, first_tick
        try:
            data = client.fetch()
            errors = 0
        except Exception as e:  # сеть/биржа моргнула — не падаем
            errors += 1
            return f"ошибка ISS ({errors}): {type(e).__name__}: {str(e)[:200]}"
        if args.front:
            data = front_month_only(data)
        now = time.time()
        n = 0
        for q in data:
            if q.num_trades < args.min_trades or q.val_today < args.min_value:
                continue
            if only and q.secid.upper() not in only and q.asset.upper() not in only:
                continue
            if q.secid.upper() in excl or q.asset.upper() in excl:
                continue
            n += 1
            quotes[q.secid] = q
            for a in det.update(q, now):
                feed.appendleft(a)
                if first_tick:
                    startup.append(a)   # уже ушедшие за порог до старта — одной сводкой
                    continue
                notifier.send(a)
                log(fmt_alert(a).replace("\n", " | "))
        if first_tick:
            first_tick = False
            startup.sort(key=lambda a: -abs(a.pct))
            lines = [f"{'🚀' if a.direction > 0 else '🩸'} <b>{htmlmod.escape(a.secid)}</b> "
                     f"{a.pct:+.2f}% к клирингу" for a in startup[:40]]
            more = f"\n…и ещё {len(startup) - 40}" if len(startup) > 40 else ""
            msg = (f"✅ Скринер запущен: {n} фьючей, порог ±{args.threshold}%, "
                   f"окно {args.window:g} мин.")
            if startup:
                msg += f"\nУже за порогом ({len(startup)}):\n" + "\n".join(lines) + more
            notifier.send_text(msg)
            log(f"старт: уже за порогом {len(startup)} шт. (сводкой)")
        last_upd = max((q.update_time for q in data), default="")
        return (f"{datetime.now():%H:%M:%S}  инструментов: {n}  |  порог ±{args.threshold}%  "
                f"окно {args.window:g} мин  опрос {args.interval:g}с  |  "
                f"последняя сделка на бирже: {last_upd}  |  алертов: {len(feed)}")

    print(f"Старт: порог ±{args.threshold}%, окно {args.window:g} мин, опрос {args.interval:g}с. "
          f"Telegram: {'вкл' if os.getenv('TG_TOKEN') and os.getenv('TG_CHAT_ID') else 'выкл'}. "
          "Ctrl+C — выход.", flush=True)
    if notifier.tg_token and notifier.tg_chat:
        ok = notifier.send_text("🔌 Скринер MOEX стартует, подключаюсь к бирже…")
        print(f"[tg] тестовое сообщение: {'отправлено' if ok else 'НЕ отправлено, см. ошибку выше'}",
              flush=True)
    else:
        print("[tg] TG_TOKEN или TG_CHAT_ID не заданы — алерты в Telegram не пойдут", flush=True)
    try:
        if use_rich:
            with Live(console=console, screen=True, auto_refresh=False) as live:
                while True:
                    status = tick()
                    live.update(build_dashboard(det, quotes, feed, status, args), refresh=True)
                    time.sleep(args.interval)
        else:
            last_hb = 0.0
            while True:
                status = tick()
                if status.startswith("ошибка") or time.time() - last_hb > 600:
                    log(status)
                    last_hb = time.time()
                time.sleep(args.interval)
    except KeyboardInterrupt:
        print("\nСтоп.")


if __name__ == "__main__":
    main()
