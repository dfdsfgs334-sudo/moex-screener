#!/usr/bin/env python3
"""
MOEX FORTS print-spike screener — ловит секундные прострелы по фьючам Мосбиржи.

Прострел = сделка (принт), цена которой отличается от рынка на >= THRESHOLD %
(по умолчанию 2%). Рынок — одно из двух (берётся большее отклонение):
  * медиана последних REF_TRADES сделок по тикеру за REF_MAX_AGE секунд
    до текущей секунды (медиана — чтобы возврат после шпильки не считался
    вторым прострелом);
  * первая сделка текущей секунды (прострел внутри одной секунды).
Если перед принтом сделок за REF_MAX_AGE секунд не было, сравнивать не с чем.

Источник — полная лента сделок FORTS из ISS (iss.moex.com), читается
инкрементально: каждый опрос забирает только новые сделки, ничего не пропуская.
Без подписки ISS отдаёт данные с задержкой ~15 мин (см. README).

Алерты: Telegram, веб-дашборд (/), CSV-лог. /health — для пингера.
"""
from __future__ import annotations

import argparse
import csv
import html as htmlmod
import json
import os
import statistics
import sys
import threading
import time
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import requests

ISS_ROOT = "https://iss.moex.com/iss/engines"
ISS = f"{ISS_ROOT}/futures/markets/forts"
PASSPORT_URL = "https://passport.moex.com/authenticate"
PAGE_LIMIT = 5000  # максимум строк, который ISS отдаёт за один запрос


# Вечные фьючерсы FORTS на американские акции/индексы. Список можно переопределить
# переменной US_PERPS (через запятую) или дополнить US_PERPS_ADD.
US_PERPS_DEFAULT = (
    "QQQF,SP500F,NBISF,BSXF,AMDF,SNDKF,CMGF,LULUF,COHRF,AMZNF,TSLAF,SMCIF,CVNAF,"
    "NFLXF,COINF,LITEF,PANWF,UBERF,DASHF,HOODF,APPF,CRWDF"
)


def us_perps_from_env() -> set[str]:
    base = os.getenv("US_PERPS", US_PERPS_DEFAULT)
    extra = os.getenv("US_PERPS_ADD", "")
    return {x.strip().upper() for x in f"{base},{extra}".split(",") if x.strip()}


US_PERPS: set[str] = us_perps_from_env()   # + квартальные контракты на US_ASSETS (из справочника)

# Квартальные фьючи FORTS на американские акции, индексы и ETF — по базовому активу
# (ASSETCODE), чтобы новые контракты при смене квартала подхватывались сами.
# Китайские ADR с листингом в США (ALIBABA, BAIDU, JDCOM, PDD) — через US_ASSETS_ADD.
US_ASSETS_DEFAULT = "AMD,NASD,DJ30,R2000,SPYF,UPRO,SOXQ,IBIT,ETHA,ETHAA,TLT,ARGT,IPO"


def us_assets_from_env() -> set[str]:
    base = os.getenv("US_ASSETS", US_ASSETS_DEFAULT)
    extra = os.getenv("US_ASSETS_ADD", "")
    return {x.strip().upper() for x in f"{base},{extra}".split(",") if x.strip()}
ETF_SECIDS: set[str] = set()   # заполняется из справочника ISS (тип бумаги J)


def log(msg: str):
    print(f"{datetime.now():%H:%M:%S} {msg}", flush=True)


# ─────────────────────────── ISS ───────────────────────────

def iss_rows(js: dict, key: str) -> list[dict]:
    block = js.get(key) or {}
    cols = block.get("columns", [])
    return [dict(zip(cols, row)) for row in block.get("data", [])]


class MoexClient:
    """Клиент ISS для одного рынка. По умолчанию — срочный рынок FORTS."""
    TRADE_COLS = "TRADENO,SECID,PRICE,QUANTITY,SYSTIME,BUYSELL,RECNO,OFFMARKETDEAL"
    SEQ = "RECNO"           # монотонный номер записи в ленте
    KIND = "fut"
    LABEL = "FORTS"

    def __init__(self, login: str | None = None, password: str | None = None,
                 base: str = ISS, session: requests.Session | None = None):
        self.base = base
        if session is not None:   # общая сессия (cookie авторизации MOEX Passport)
            self.s = session
            return
        self.s = requests.Session()
        self.s.headers["User-Agent"] = "moex-spike-screener/2.0"
        if login and password:
            r = self.s.get(PASSPORT_URL, auth=(login, password), timeout=15)
            ok = r.ok and "MicexPassportCert" in self.s.cookies
            log(f"[auth] MOEX Passport: {'ok' if ok else f'не удалось ({r.status_code}), работаю анонимно'}")

    def get(self, path: str, params: dict, retries: int = 4) -> dict:
        params = {"iss.meta": "off", **params}
        for attempt in range(retries):
            try:
                r = self.s.get(f"{self.base}/{path}", params=params, timeout=30)
                r.raise_for_status()
                return r.json()
            except (requests.RequestException, ValueError):
                if attempt == retries - 1:
                    raise
                time.sleep(1 + attempt)
        raise RuntimeError("unreachable")

    def instruments(self) -> dict[str, dict]:
        """secid -> {name, asset, num_trades}"""
        js = self.get("securities.json", {
            "iss.only": "securities,marketdata",
            "securities.columns": "SECID,SHORTNAME,ASSETCODE,PREVOPENPOSITION",
            "marketdata.columns": "SECID,NUMTRADES",
        })
        out = {r["SECID"]: {"name": r.get("SHORTNAME") or r["SECID"],
                            "asset": r.get("ASSETCODE") or "", "num_trades": 0,
                            "prev_oi": int(r.get("PREVOPENPOSITION") or 0)}
               for r in iss_rows(js, "securities")}
        for r in iss_rows(js, "marketdata"):
            if r["SECID"] in out:
                out[r["SECID"]]["num_trades"] = int(r.get("NUMTRADES") or 0)
        for v in out.values():
            v["kind"] = self.KIND
        return out

    def trades(self, start: int, limit: int = PAGE_LIMIT) -> list[dict]:
        """Сделки текущей торговой сессии в хронологическом порядке, начиная с позиции start."""
        js = self.get("trades.json", {"start": start, "limit": limit,
                                      "trades.columns": self.TRADE_COLS})
        return iss_rows(js, "trades")

    def latest_trade(self) -> dict | None:
        js = self.get("trades.json", {"reversed": 1, "limit": 1, "trades.columns": self.TRADE_COLS})
        rows = iss_rows(js, "trades")
        return rows[0] if rows else None

    def count_trades(self) -> int:
        """Число сделок в сессии (бинарный поиск по start, ~20 лёгких запросов)."""
        def exists(i: int) -> bool:
            return bool(self.trades(i, 1))
        if not exists(0):
            return 0
        lo, hi = 0, 1024
        while exists(hi):
            lo, hi = hi, hi * 2
        while hi - lo > 1:
            mid = (lo + hi) // 2
            if exists(mid):
                lo = mid
            else:
                hi = mid
        return hi


class EtfClient(MoexClient):
    """Биржевые фонды (БПИФ/ETF) фондового рынка: тип бумаги J в режимах TQBR, TQTY…"""
    TRADE_COLS = "TRADENO,SECID,PRICE,QUANTITY,SYSTIME,BUYSELL"
    SEQ = "TRADENO"
    KIND = "etf"

    NEXT_TRADE = True       # номер сделки сквозной: можно читать «всё после №N»

    def __init__(self, board: str, session: requests.Session):
        super().__init__(base=f"{ISS_ROOT}/stock/markets/shares/boards/{board}", session=session)
        self.board = board
        self.LABEL = f"фонды {board}"

    def trades_after(self, tradeno: int, limit: int = PAGE_LIMIT) -> list[dict]:
        js = self.get("trades.json", {"tradeno": tradeno, "next_trade": 1, "limit": limit,
                                      "trades.columns": self.TRADE_COLS})
        return iss_rows(js, "trades")

    def instruments(self) -> dict[str, dict]:
        js = self.get("securities.json", {
            "iss.only": "securities,marketdata",
            "securities.columns": "SECID,SHORTNAME,SECTYPE",
            "marketdata.columns": "SECID,NUMTRADES",
        })
        out = {r["SECID"]: {"name": r.get("SHORTNAME") or r["SECID"], "asset": r["SECID"],
                            "num_trades": 0, "prev_oi": 0, "kind": "etf"}
               for r in iss_rows(js, "securities") if r.get("SECTYPE") == "J"}
        for r in iss_rows(js, "marketdata"):
            if r["SECID"] in out:
                out[r["SECID"]]["num_trades"] = int(r.get("NUMTRADES") or 0)
        return out


# ─────────────────────────── детектор ───────────────────────────

@dataclass
class Alert:
    exch_time: str      # время сделки на бирже
    seen: datetime      # когда заметили
    secid: str
    name: str
    direction: int
    pct: float
    price: float
    ref: float
    qty: int
    side: str           # B / S
    lag_s: float
    back: float | None = None     # цена, куда вернулось (откат подтверждён)
    back_s: float | None = None   # за сколько секунд вернулось (от пика)
    rise_s: float | None = None   # за сколько секунд выстрелило (от начала до пика)


@dataclass
class SecState:
    recent: deque = field(default_factory=lambda: deque(maxlen=50))   # (ts, price, side)
    cur_sec: float = -1
    ref: dict = field(default_factory=dict)        # side -> медиана сделок до этой секунды
    sec_open: dict = field(default_factory=dict)   # side -> первая сделка этой секунды
    book: deque = field(default_factory=lambda: deque(maxlen=64))     # (ts, bid, ask)
    win: dict = field(default_factory=dict)        # side -> deque[(sec, price)] за SPIKE_WINDOW
    last_alert: dict = field(default_factory=dict)                    # dir -> (ts, pct)


class PrintDetector:
    """Прострел = принт, ушедший на >= thr % за пределы рынка.

    Со стаканом (котировки Финама): рынок = лучший бид/аск ДО секунды сделки.
      Принт внутри спреда или по биду/аску — не прострел (это и был спам на
      широких спредах); выше аска — % от аска, ниже бида — % от бида.
    Без стакана (ISS): покупки сравниваются с недавними покупками, продажи —
      с продажами (медиана за REF_MAX_AGE + первая сделка той же стороны в этой
      секунде), чтобы «пинг-понг» бид↔аск не выглядел прострелом.
    """

    def __init__(self, threshold: float, ref_trades: int, ref_max_age: float,
                 cooldown: float, escalate: float):
        self.thr = threshold
        self.ref_n = ref_trades
        self.ref_max_age = ref_max_age
        self.cooldown = cooldown
        self.escalate = escalate
        self.ref_age_override: dict[str, float] = {}   # свой REF_MAX_AGE для тикера
        self.state: dict[str, SecState] = {}
        # прострел может разгоняться ступеньками за десятки секунд (шпилька на минутке)
        self.window = float(os.getenv("SPIKE_WINDOW", 2))

    def _window_base(self, st: SecState, sec: float, side: str, up: bool,
                     all_sides: bool = False):
        """Уровень ДО разгона: для движения вверх — минимум покупок и аска за окно,
        вниз — максимум продаж и бида. Сравнение со своей стороной, чтобы
        пинг-понг бид↔аск не давал ложных сигналов."""
        lo = sec - self.window
        vals = []
        for sd in ([side] if side and not all_sides else ["B", "S", ""]):
            vals += [p for t, p in st.win.get(sd, ()) if lo <= t < sec]
        vals += [(a if up else b) for t, b, a in st.book if lo <= t < sec and (a if up else b) > 0]
        if not vals:
            return None
        return min(vals) if up else max(vals)

    # --- стакан ---
    def on_quote(self, key: str, ts: float, bid: float, ask: float):
        st = self.state.setdefault(key, SecState())
        if st.book:  # частичные обновления: пустое поле = без изменений
            _, pb, pa = st.book[-1]
            bid = bid or pb
            ask = ask or pa
        if bid or ask:
            st.book.append((ts, bid, ask))

    @staticmethod
    def _book_before(st: SecState, sec: float):
        """Последний стакан строго до секунды сделки (во время прострела стакан
        двигается, и котировка «после» может прийти раньше самой сделки)."""
        for t, bid, ask in reversed(st.book):
            if t < sec:
                return bid, ask
        return None

    # --- сделки ---
    def on_trade(self, secid: str, ts: float, price: float, side: str = "",
                 ) -> tuple[int, float, float] | None:
        """Возвращает (direction, pct, ref), если сделка — прострел."""
        side = side if side in ("B", "S") else ""
        st = self.state.setdefault(secid, SecState())
        sec = float(int(ts))
        hit = None
        if sec != st.cur_sec:
            st.cur_sec = sec
            st.sec_open = {}
            max_age = self.ref_age_override.get(secid.split(":")[0], self.ref_max_age)
            fresh = [(p, sd) for t, p, sd in st.recent if sec - t <= max_age]
            st.ref = {}
            for sd in ("B", "S", ""):
                ps = [p for p, x in fresh if (x == sd or sd == "")][-self.ref_n:]
                if ps:
                    st.ref[sd] = statistics.median(ps)
        st.sec_open.setdefault(side, price)

        best = None
        book = self._book_before(st, sec)
        # стакан должен сходиться с последними сделками; если он дальше порога от них,
        # это уже стакан «после» прострела, пришедший раньше сделок — не используем
        prev_tr = next((p for t, p, _ in reversed(st.recent) if t < sec and sec - t <= 60), None)
        if book and prev_tr and book[0] > 0 and book[1] > 0:
            lo_ok = book[0] * (1 - self.thr / 100)
            hi_ok = book[1] * (1 + self.thr / 100)
            if not (lo_ok <= prev_tr <= hi_ok):
                book = None
        # узкий спред: сравнивать можно и со сделками другой стороны — тогда прострел
        # ловится, даже если стакан «после» пришёл с меткой времени раньше сделки
        narrow = bool(book and book[0] > 0 and book[1] >= book[0]
                      and (book[1] / book[0] - 1) * 100 < self.thr / 2)
        if book and book[0] > 0 and book[1] > 0 and book[1] >= book[0]:
            bid, ask = book
            if price > ask:
                best = ((price / ask - 1) * 100, ask)
            elif price < bid:
                best = ((price / bid - 1) * 100, bid)
        else:
            prev = next((p for t, p, _ in reversed(st.recent) if t < sec), None)
            for ref, from_median in ((st.ref.get(side), True), (st.sec_open.get(side), False)):
                if not ref:
                    continue
                pct = (price / ref - 1) * 100
                # от медианы — только если это скачок от предыдущей сделки, а не
                # сползание по чуть-чуть за минуту
                if from_median and prev and abs(price / prev - 1) * 100 < self.thr / 2:
                    continue
                if best is None or abs(pct) > abs(best[0]):
                    best = (pct, ref)
        # разгон за окно: от уровня до начала движения
        if side in ("B", ""):
            base = self._window_base(st, sec, side, up=True, all_sides=narrow)
            if base and price > base:
                pct = (price / base - 1) * 100
                if best is None or pct > abs(best[0]):
                    best = (pct, base)
        if side in ("S", ""):
            base = self._window_base(st, sec, side, up=False, all_sides=narrow)
            if base and price < base:
                pct = (price / base - 1) * 100
                if best is None or -pct > abs(best[0]):
                    best = (pct, base)
        dq = st.win.setdefault(side, deque())
        dq.append((sec, price))
        while dq and sec - dq[0][0] > self.window:
            dq.popleft()
        if best:
            pct, ref = best
            if abs(pct) >= self.thr:
                d = 1 if pct > 0 else -1
                prev = st.last_alert.get(d)
                if not prev or ts - prev[0] >= self.cooldown or abs(pct) >= abs(prev[1]) + self.escalate:
                    st.last_alert[d] = (ts, pct)
                    hit = (d, pct, ref)
        st.recent.append((sec, price, side))
        return hit


@dataclass
class Pending:
    """Прострел-кандидат: ждём объём и откат цены."""
    secid: str
    board: str
    side: str
    direction: int
    ref: float          # рынок до прострела
    ext: float          # самая дальняя цена прострела
    vol: int            # лотов в прострел
    start: float        # время первой сделки прострела (биржа)
    can_alert: bool
    ext_ts: float = 0.0 # когда достигнут пик

class SpikeConfirmer:
    """Алерт только на «прострел с откатом»: ушло на ≥ порог, прошло ≥ MIN_LOTS лотов
    и цена (сделкой или стаканом) вернулась хотя бы на REVERT_FRAC прострела
    за REVERT_WINDOW секунд. Плавные движения и одиночные лоты отсекаются."""

    def __init__(self, thr: float):
        env = os.getenv
        self.thr = thr
        self.on = env("CONFIRM_REVERT", "1") != "0"
        self.min_lots = int(env("MIN_LOTS", 2))
        self.frac = float(env("REVERT_FRAC", 0.5))
        self.window = float(env("REVERT_WINDOW", 20))
        self.max_dur = float(env("SPIKE_MAX_DURATION", 300))  # прострел целиком, сек
        self.pending: dict[str, Pending] = {}
        self.dropped = {"мало лотов": 0, "без отката": 0}

    def _back_level(self, p: Pending) -> float:
        return p.ref + (1 - self.frac) * (p.ext - p.ref)

    def _confirm(self, key: str, p: Pending, back: float, ts: float):
        self.pending.pop(key, None)
        if p.vol < self.min_lots:
            self.dropped["мало лотов"] += 1
            self._log_drop(p, f"мало лотов ({p.vol} < {self.min_lots})")
            return None
        return p, back, max(ts - (p.ext_ts or p.start), 0.0)

    def _expired(self, key: str, p: Pending, ts: float) -> bool:
        # REVERT_WINDOW секунд на откат — от пика; плюс общий потолок длительности
        if ts - (p.ext_ts or p.start) > self.window or ts - p.start > self.max_dur:
            self.pending.pop(key, None)
            self.dropped["без отката"] += 1
            self._log_drop(p, f"не вернулось за {self.window:.0f} с")
            return True
        return False

    @staticmethod
    def _log_drop(p: Pending, why: str):
        if p.can_alert:
            pct = (p.ext / p.ref - 1) * 100
            t = datetime.fromtimestamp(p.start, timezone(timedelta(hours=3))).strftime("%H:%M:%S")
            log(f"[отсеян] {p.secid} {pct:+.2f}% {fmt_price(p.ref)}→{fmt_price(p.ext)}, "
                f"{p.vol} лот, биржа {t}: {why}")

    def start(self, key: str, p: Pending):
        p.ext_ts = p.ext_ts or p.start
        self.pending[key] = p

    def on_trade(self, key: str, ts: float, price: float, qty: int):
        p = self.pending.get(key)
        if not p or self._expired(key, p, ts):
            return None
        d = p.direction
        beyond = (price - p.ref) * d >= p.ref * self.thr / 200   # дальше половины порога
        if beyond:
            p.vol += qty             # сделки в зоне прострела
        if (price - p.ext) * d > 0:
            p.ext, p.ext_ts = price, ts   # прострел добирает — пик выше
            return None
        if (price - self._back_level(p)) * d <= 0:
            return self._confirm(key, p, price, ts)
        return None

    def on_quote(self, key: str, ts: float, bid: float, ask: float):
        p = self.pending.get(key)
        if not p or self._expired(key, p, ts):
            return None
        if ts < p.start + 1:          # стакан той же секунды может быть ещё «до» прострела
            return None
        level = self._back_level(p)
        if p.direction > 0 and ask and ask <= level:
            return self._confirm(key, p, ask, ts)
        if p.direction < 0 and bid and bid >= level:
            return self._confirm(key, p, bid, ts)
        return None


# ─────────────────────────── уведомления ───────────────────────────

def fmt_price(p: float) -> str:
    s = f"{p:,.6f}".rstrip("0").rstrip(".")
    return s.replace(",", " ")


def fmt_alert(a: Alert, html: bool = False) -> str:
    esc = htmlmod.escape if html else (lambda x: x)
    b = (lambda x: f"<b>{esc(x)}</b>") if html else (lambda x: x)
    arrow = "🚀" if a.direction > 0 else "🩸"
    side = {"B": "покупка", "S": "продажа"}.get(a.side, a.side or "?")
    lag = f"задержка {a.lag_s / 60:.0f} мин" if a.lag_s >= 90 else f"задержка {a.lag_s:.0f} с"
    us = "🇺🇸 " if a.secid.upper() in US_PERPS else ""
    etf = "📊 " if a.secid in ETF_SECIDS else ""
    name = "вечник US" if us and a.name.upper() == a.secid.upper() else a.name
    if us and a.name.upper() != a.secid.upper():
        name = f"{a.name} · US"
    if etf:
        name = a.name.replace(" ETF", "") + " · фонд"
    us = us or etf
    if a.back is not None:
        sec = lambda v: f"{v:.0f} с" if v and v >= 1 else "<1 с"
        return (f"⚡{arrow} {us}{b(a.secid)} ({esc(name)}) прострел {a.pct:+.2f}% "
                f"за {sec(a.rise_s)}, откат за {sec(a.back_s)}\n"
                f"{fmt_price(a.ref)} → {fmt_price(a.price)} → {fmt_price(a.back)} | "
                f"{a.qty} лот, {side}\n"
                f"биржа {a.exch_time} ({lag})")
    return (f"{arrow} {us}{b(a.secid)} ({esc(name)}) принт {a.pct:+.2f}% за секунду\n"
            f"{fmt_price(a.ref)} → {fmt_price(a.price)} | {a.qty} лот, {side}\n"
            f"биржа {a.exch_time} ({lag})")


class Notifier:
    def __init__(self, tg_token: str | None, tg_chat: str | None, csv_path: str | None):
        self.tg_token = (tg_token or "").strip()
        # несколько получателей через запятую: личка, канал (@name или -100…), группа
        self.tg_chats = [c.strip() for c in (tg_chat or "").split(",") if c.strip()]
        self.tg_chat = ",".join(self.tg_chats)
        self.csv_path = csv_path
        self.hub = None  # публичный бот (tg_bot.BotHub), если включён
        self.s = requests.Session()
        if csv_path and not os.path.exists(csv_path):
            with open(csv_path, "w", newline="", encoding="utf-8") as f:
                csv.writer(f).writerow(["exch_time", "seen", "secid", "name", "dir", "pct",
                                        "price", "ref", "qty", "side"])

    @property
    def enabled(self) -> bool:
        return bool(self.tg_token and self.tg_chat)

    def send_text(self, text: str) -> bool:
        """Шлёт всем получателям. True — дошло хотя бы до одного."""
        if not self.enabled:
            return False
        return any([self._send_one(chat, text) for chat in self.tg_chats])

    def _send_one(self, chat: str, text: str, attempts: int = 3) -> bool:
        for _ in range(attempts):
            try:
                r = self.s.post(f"https://api.telegram.org/bot{self.tg_token}/sendMessage",
                                data={"chat_id": chat, "text": text, "parse_mode": "HTML",
                                      "disable_web_page_preview": True}, timeout=10)
                if r.ok:
                    return True
                try:
                    js = r.json()
                except ValueError:
                    js = {}
                if r.status_code == 429:  # лимит Telegram (в канал ~20 сообщений/мин)
                    wait = (js.get("parameters") or {}).get("retry_after", 5)
                    log(f"[tg] {chat}: Telegram просит подождать {wait}с")
                    time.sleep(min(float(wait), 60))
                    continue
                desc = js.get("description", r.text[:200])
                hint = {401: "неверный TG_TOKEN",
                        400: "неверный chat id, у бота не нажат Start или бот не добавлен в канал",
                        403: "бот заблокирован, не добавлен в чат или не админ канала"
                        }.get(r.status_code, "")
                log(f"[tg] {chat}: Telegram ответил {r.status_code}: {desc}"
                    f"{' — ' + hint if hint else ''}")
                return False
            except requests.RequestException as e:
                log(f"[tg] {chat}: ошибка сети: {e}")
                time.sleep(2)
        return False

    def send(self, a: Alert):
        """Неблокирующая отправка: алерт уходит в очередь фонового потока."""
        if not hasattr(self, "_q"):
            import queue
            self._q = queue.Queue(maxsize=1000)
            threading.Thread(target=self._worker, daemon=True, name="notifier").start()
        try:
            self._q.put_nowait(a)
        except Exception:
            log("[tg] очередь алертов переполнена, алерт пропущен")

    def _worker(self):
        while True:
            a = self._q.get()
            try:
                self._deliver(a)
            except Exception as e:
                log(f"[tg] ошибка доставки: {e}")

    def _deliver(self, a: Alert):
        if self.csv_path:
            with open(self.csv_path, "a", newline="", encoding="utf-8") as f:
                csv.writer(f).writerow([a.exch_time, a.seen.isoformat(timespec="seconds"), a.secid,
                                        a.name, a.direction, f"{a.pct:.3f}", a.price, a.ref,
                                        a.qty, a.side])
        text = fmt_alert(a, html=True)
        self.send_text(text)
        if self.hub:
            self.hub.broadcast(text, a.pct, skip=set(self.tg_chats))


# ─────────────────────────── веб-дашборд ───────────────────────────

PAGE = """<!doctype html><html lang="ru"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>MOEX прострелы</title><style>
:root{--bg:#0f1115;--fg:#e6e6e6;--mut:#8a8f98;--card:#171a21;--line:#262a33;--up:#22c55e;--dn:#ef4444}
@media (prefers-color-scheme: light){:root{--bg:#f6f7f9;--fg:#111;--mut:#666;--card:#fff;--line:#e3e5e8;--up:#15803d;--dn:#b91c1c}}
body{margin:0;background:var(--bg);color:var(--fg);font:14px/1.4 system-ui,sans-serif}
header{padding:12px 16px;border-bottom:1px solid var(--line)}h1{font-size:17px;margin:0 0 6px}
.st{display:flex;flex-wrap:wrap;gap:6px 18px;color:var(--mut);font-size:12px}.st b{color:var(--fg);font-weight:600}
main{padding:16px;max-width:1100px}section{background:var(--card);border:1px solid var(--line);border-radius:10px;overflow:auto}
h2{font-size:13px;margin:0;padding:10px 12px;border-bottom:1px solid var(--line);color:var(--mut);text-transform:uppercase;letter-spacing:.04em}
table{width:100%;border-collapse:collapse;font-variant-numeric:tabular-nums}td,th{padding:7px 12px;text-align:right;white-space:nowrap}
th{color:var(--mut);font-weight:500;font-size:12px}td:nth-child(-n+2),th:nth-child(-n+2){text-align:left}tr+tr td{border-top:1px solid var(--line)}
.up{color:var(--up);font-weight:700}.dn{color:var(--dn);font-weight:700}.mut{color:var(--mut)}
</style></head><body><header><h1>MOEX FORTS — секундные прострелы</h1><div class="st" id="st">загрузка…</div></header>
<main><section><h2>Принты ±<span id="thr">2</span>% и больше</h2><table id="al"></table></section></main>
<script>
const e=s=>String(s).replace(/[&<>"]/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;'}[c]));
const side={B:'покупка',S:'продажа'};
async function load(){try{const d=await (await fetch('api/state')).json();
document.getElementById('thr').textContent=d.threshold;
document.getElementById('st').innerHTML=d.status.map(([k,v])=>`<span>${e(k)}: <b>${e(v)}</b></span>`).join('');
document.getElementById('al').innerHTML='<tr><th>Время биржи</th><th>Тикер</th><th>Принт</th><th>Рынок → принт</th><th>Лоты</th><th>Сторона</th></tr>'+
(d.alerts.length?d.alerts.map(a=>`<tr><td>${e(a.time)}</td><td>${e(a.secid)} <span class="mut">${e(a.name)}</span></td><td class="${a.pct>0?'up':'dn'}">${a.pct>0?'+':''}${a.pct.toFixed(2)}%</td><td>${e(a.ref)} → ${e(a.price)}</td><td>${a.qty}</td><td>${e(side[a.side]||a.side||'')}</td></tr>`).join(''):'<tr><td colspan=6 class="mut">пока тихо — прострелов не было</td></tr>');
}catch(x){document.getElementById('st').textContent='нет связи с сервером'}}
load();setInterval(load,3000);
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
    log(f"[web] дашборд на порту {port}")


# ─────────────────────────── основной цикл ───────────────────────────

MSK = timezone(timedelta(hours=3))  # время биржи — Москва (без перехода на летнее)


def parse_ts(systime: str) -> float:
    return datetime.strptime(systime, "%Y-%m-%d %H:%M:%S").replace(tzinfo=MSK).timestamp()


class IssFeed:
    """Инкрементальное чтение ленты сделок одного рынка ISS."""

    def __init__(self, client: MoexClient):
        self.c = client
        self.offset = 0
        self.last_seq = 0
        self.empty_since = 0.0

    def seek_to_end(self):
        if getattr(self.c, "NEXT_TRADE", False):
            last = self.c.latest_trade()
            self.last_seq = int(last[self.c.SEQ]) if last else 0
            self.empty_since = 0.0
            log(f"лента {self.c.LABEL}: начинаю после сделки №{self.last_seq}")
            return
        n = self.c.count_trades()
        self.offset = n
        last = self.c.latest_trade()
        self.last_seq = int(last[self.c.SEQ]) if last else 0
        self.empty_since = 0.0
        log(f"лента {self.c.LABEL}: в сессии {n} сделок, начинаю с новых")

    def poll(self, scr: "Screener") -> bool:
        if getattr(self.c, "NEXT_TRADE", False):
            rows = self.c.trades_after(self.last_seq) if self.last_seq else self.c.trades(0)
        else:
            rows = self.c.trades(self.offset)
        if rows:
            self.empty_since = 0.0
            now = time.time()
            for r in rows:
                self.offset += 1
                seq = int(r.get(self.c.SEQ) or 0)
                if seq and seq <= self.last_seq:
                    continue  # уже видели
                self.last_seq = max(self.last_seq, seq)
                scr.processed += 1
                if r.get("OFFMARKETDEAL"):
                    continue  # адресные/внебиржевые сделки — цена произвольная
                secid, price, systime = r.get("SECID"), r.get("PRICE"), r.get("SYSTIME")
                if not secid or not price or not systime:
                    continue
                if self.c.KIND == "etf" and not scr.is_etf(secid):
                    continue  # в режиме TQBR лента общая с акциями — берём только фонды
                # один фонд может торговаться в рублях (TQBR) и юанях (TQTY) —
                # история цен у каждого режима своя, иначе разница валют = «прострел»
                key = f"{secid}:{self.c.board}" if self.c.KIND == "etf" else secid
                scr.handle_trade(secid, parse_ts(systime), float(price),
                                 int(r.get("QUANTITY") or 0), r.get("BUYSELL") or "",
                                 key=key, board=getattr(self.c, "board", ""))
            scr.rate.append((now, len(rows)))
            return len(rows) >= PAGE_LIMIT
        # пусто: либо тишина, либо биржа начала новую сессию и нумерация сбросилась
        now = time.time()
        if not self.empty_since:
            self.empty_since = now
        elif now - self.empty_since > 30:
            self.empty_since = now
            last = self.c.latest_trade()
            if last and int(last[self.c.SEQ]) > self.last_seq:
                log(f"лента {self.c.LABEL}: новая торговая сессия — пересчитываю позицию")
                self.seek_to_end()
        return False


class Screener:
    def __init__(self, args, client: MoexClient, notifier: Notifier,
                 extra_clients: list[MoexClient] | None = None):
        self.a = args
        self.client = client
        self.clients = [c for c in [client, *(extra_clients or [])] if c is not None]
        self.feeds = [IssFeed(c) for c in self.clients]
        self.etf_on = os.getenv("ETF_ON", "1") != "0"
        self.etf_age = float(os.getenv("ETF_REF_MAX_AGE", 60))
        self.notifier = notifier
        self.det = PrintDetector(args.threshold, args.ref_trades, args.ref_max_age,
                                 args.cooldown, args.escalate)
        self.conf = SpikeConfirmer(args.threshold)
        self.us = set() if os.getenv("US_PERPS_ON", "1") == "0" else set(US_PERPS)
        self.us_assets = set() if not self.us else us_assets_from_env()
        self.us_age = float(os.getenv("US_REF_MAX_AGE", 60))
        self.det.ref_age_override = {s: self.us_age for s in self.us}
        self.only = {x.strip().upper() for x in args.only.split(",") if x.strip()}
        self.excl = {x.strip().upper() for x in args.exclude.split(",") if x.strip()}
        self.instr: dict[str, dict] = {}
        self.instr_at = 0.0
        self.alerts: deque[Alert] = deque(maxlen=300)
        self.processed = 0
        self.quotes = 0
        self.rate = deque(maxlen=600)      # (time, trades) для сделок/мин
        self.last_exch = "—"
        self.last_err = ""
        self.source = "MOEX ISS, задержка ~15 мин"
        self.lock = threading.Lock()

    # --- фильтры ---
    def allowed(self, secid: str) -> bool:
        info = self.instr.get(secid, {})
        asset = (info.get("asset") or "").upper()
        if self.only and secid.upper() not in self.only and asset not in self.only:
            return False
        if secid.upper() in self.excl or asset in self.excl:
            return False
        if secid.upper() in self.us:
            return True  # US-вечники неликвидны — фильтр по числу сделок не применяем
        if info.get("kind") == "etf":
            return self.etf_on  # фонды — все, без фильтра ликвидности
        return info.get("num_trades", 0) >= self.a.min_trades

    def is_etf(self, secid: str) -> bool:
        return self.instr.get(secid, {}).get("kind") == "etf"

    def refresh_instruments(self, force=False):
        if force or time.time() - self.instr_at > 60:
            merged: dict[str, dict] = {}
            for c in self.clients:
                try:
                    merged.update(c.instruments())
                except Exception as e:
                    if force and c is self.client:
                        raise
                    self.last_err = f"инструменты {c.LABEL}: {type(e).__name__}"
            if merged:
                self.instr = merged
                self.instr_at = time.time()
                for secid, info in merged.items():
                    if (info.get("kind") == "fut" and self.us_assets
                            and (info.get("asset") or "").upper() in self.us_assets
                            and secid.upper() not in self.us):
                        # квартальный фьюч на американскую акцию / индекс / ETF
                        self.us.add(secid.upper())
                        US_PERPS.add(secid.upper())
                        self.det.ref_age_override[secid] = self.us_age
                    if info.get("kind") == "etf":
                        self.det.ref_age_override[secid] = self.etf_age
                        ETF_SECIDS.add(secid)

    def handle_trade(self, secid: str, ts: float, price: float, qty: int, side: str,
                     can_alert: bool = True, key: str | None = None, board: str = ""):
        """Общая обработка сделки для любого источника (ISS или Finam)."""
        key = key or secid
        self.last_exch = datetime.fromtimestamp(ts, MSK).strftime("%H:%M:%S")
        done = self.conf.on_trade(key, ts, price, qty) if self.conf.on else None
        if done:
            self._emit(*done)
        hit = self.det.on_trade(key, ts, price, side)
        if not hit:
            return
        d, pct, ref = hit
        if not self.conf.on:
            if can_alert and self.allowed(secid) and qty >= self.conf.min_lots:
                self._emit(Pending(secid, board, side, d, ref, price, qty, ts, True), None, None)
            return
        if self.conf.pending.get(key):
            return  # уже следим за прострелом; движение назад — это его откат, не новый
        self.conf.start(key, Pending(secid, board, side, d, ref, price, qty, ts,
                                     can_alert and self.allowed(secid)))

    def _emit(self, p: Pending, back: float | None, back_s: float | None):
        if not p.can_alert:
            return
        name = self.instr.get(p.secid, {}).get("name", p.secid)
        cur = {"TQTY": "¥", "TQTD": "$", "TQTE": "€", "TQTH": "HK$"}.get(p.board)
        if cur:
            name = f"{name} {cur}"
        pct = (p.ext / p.ref - 1) * 100
        a = Alert(datetime.fromtimestamp(p.start, MSK).strftime("%H:%M:%S"), datetime.now(),
                  p.secid, name, p.direction, pct, p.ext, p.ref, p.vol, p.side,
                  time.time() - p.start, back, back_s, max((p.ext_ts or p.start) - p.start, 0.0))
        with self.lock:
            self.alerts.appendleft(a)
        log(fmt_alert(a).replace("\n", " | "))
        self.notifier.send(a)

    def on_quote(self, secid: str, ts: float, bid: float, ask: float):
        self.det.on_quote(secid, ts, bid, ask)
        self.quotes += 1
        if self.conf.on:
            done = self.conf.on_quote(secid, ts, bid, ask)
            if done:
                self._emit(*done)

    def tick(self) -> bool:
        """Один опрос всех лент. True — где-то есть ещё данные, опросить сразу."""
        self.refresh_instruments()
        more = False
        for f in self.feeds:
            try:
                more |= f.poll(self)
            except Exception as e:
                self.last_err = f"ISS {f.c.LABEL}: {type(e).__name__}"
                log(f"ошибка ISS ({f.c.LABEL}): {type(e).__name__}: {str(e)[:200]}")
        return more

    def coverage(self) -> str:
        fut = sum(1 for s, v in self.instr.items() if v.get("kind") == "fut" and self.allowed(s))
        us = sum(1 for s in self.instr if s.upper() in self.us and self.allowed(s))
        etf = sum(1 for s, v in self.instr.items() if v.get("kind") == "etf" and self.allowed(s))
        return (f"принты ±{self.a.threshold}% за секунду — фьючей {fut} (из них американских {us})"
                + (f", фондов {etf}" if etf else ""))

    def state(self) -> dict:
        with self.lock:
            alerts = list(self.alerts)[:150]
        now = time.time()
        per_min = sum(n for t, n in list(self.rate) if now - t < 60)
        status = [("источник", self.source), ("биржа", self.last_exch),
                  ("сделок обработано", f"{self.processed:,}".replace(",", " ")),
                  ("сделок/мин", str(per_min)), ("порог", f"±{self.a.threshold}%"),
                  ("Telegram", "вкл" if self.notifier.enabled else "выкл"),
                  ("прострелов", str(len(alerts))),
                  ("отсеяно", ", ".join(f"{k}: {v}" for k, v in self.conf.dropped.items()))]
        if self.last_err:
            status.append(("ошибка", self.last_err))
        return {"threshold": self.a.threshold, "status": status,
                "alerts": [{"time": a.exch_time, "secid": a.secid, "name": a.name, "pct": a.pct,
                            "price": fmt_price(a.price), "ref": fmt_price(a.ref), "qty": a.qty,
                            "side": a.side} for a in alerts]}

    def run(self):
        while True:
            try:
                self.refresh_instruments(force=True)
                for f in self.feeds:
                    f.seek_to_end()
                break
            except Exception as e:
                self.last_err = f"старт: {type(e).__name__}"
                log(f"ошибка ISS при старте: {e}; повтор через 10с")
                time.sleep(10)
        self.notifier.send_text(f"✅ Скринер запущен: {self.coverage()}")
        last_hb = time.time()
        while True:
            try:
                more = self.tick()
                self.last_err = ""
            except Exception as e:
                more = False
                self.last_err = f"ISS: {type(e).__name__}"
                log(f"ошибка ISS: {type(e).__name__}: {str(e)[:200]}")
                time.sleep(3)
            if time.time() - last_hb > 600:
                last_hb = time.time()
                log(" | ".join(f"{k}: {v}" for k, v in self.state()["status"]))
            if not more:
                time.sleep(self.a.interval)


def parse_args():
    env = os.getenv
    p = argparse.ArgumentParser(description="Скринер секундных прострелов по фьючам MOEX")
    p.add_argument("-t", "--threshold", type=float, default=float(env("THRESHOLD", 1.5)),
                   help="порог прострела в %% (по умолч. 1.5)")
    p.add_argument("--ref-trades", type=int, default=int(env("REF_TRADES", 5)),
                   help="сколько последних сделок брать для цены рынка (медиана, по умолч. 5)")
    p.add_argument("--ref-max-age", type=float, default=float(env("REF_MAX_AGE", 10)),
                   help="рынок = сделки не старше N секунд до принта (по умолч. 10)")
    p.add_argument("--cooldown", type=float, default=float(env("COOLDOWN", 30)),
                   help="пауза между алертами по тикеру в одну сторону, сек (по умолч. 30)")
    p.add_argument("--escalate", type=float, default=1.0,
                   help="в паузе алертить снова, если принт дальше ещё на N %% (по умолч. 1)")
    p.add_argument("--min-trades", type=int, default=int(env("MIN_TRADES", 30)),
                   help="минимум сделок за день по тикеру, отсекает неликвид (по умолч. 30)")
    p.add_argument("--only", default=env("ONLY", ""), help="только эти тикеры/активы: Si,BR,MX...")
    p.add_argument("--exclude", default=env("EXCLUDE", ""), help="исключить тикеры/активы")
    p.add_argument("-i", "--interval", type=float, default=float(env("INTERVAL", 1)),
                   help="период опроса ленты, сек (по умолч. 1)")
    p.add_argument("--csv", default=env("CSV", ""), help="файл лога алертов (по умолч. выкл)")
    p.add_argument("--test-alert", action="store_true", help="отправить тестовый алерт и выйти")
    return p.parse_args()


def main():
    args = parse_args()
    notifier = Notifier(os.getenv("TG_TOKEN"), os.getenv("TG_CHAT_ID"), args.csv or None)

    if args.test_alert:
        a = Alert("16:27:01", datetime.now(), "SiZ6", "Si-12.26", 1, 2.41, 84910, 82907, 12, "B", 3)
        print(fmt_alert(a))
        ok = notifier.send_text(fmt_alert(a, html=True))
        print("Telegram:", "отправлено" if ok else "не отправлено (см. выше / TG_TOKEN не задан)")
        return

    log(f"Старт: принты ±{args.threshold}% за секунду, рынок = медиана {args.ref_trades} сделок. "
        f"Telegram: {'вкл' if notifier.enabled else 'выкл'}")
    if notifier.enabled:
        ok = notifier.send_text("🔌 Скринер MOEX стартует, подключаюсь к бирже…")
        log(f"[tg] тестовое сообщение: {'отправлено' if ok else 'НЕ отправлено, см. ошибку выше'}")
    else:
        log("[tg] TG_TOKEN или TG_CHAT_ID не заданы — алерты в Telegram не пойдут")

    client = MoexClient(os.getenv("MOEX_LOGIN"), os.getenv("MOEX_PASSWORD"))
    etf_clients = []
    if os.getenv("ETF_ON", "1") != "0":
        boards = [b.strip().upper() for b in os.getenv("ETF_BOARDS", "TQBR,TQTY").split(",")
                  if b.strip()]
        etf_clients = [EtfClient(b, client.s) for b in boards]
    scr = Screener(args, client, notifier, etf_clients)
    if notifier.tg_token and notifier.tg_chats and os.getenv("PUBLIC_BOT", "1") != "0":
        from tg_bot import BotHub

        def bot_status() -> str:
            st = dict(scr.state()["status"])
            return (f"источник: {st.get('источник')}\nбиржа: {st.get('биржа')}, "
                    f"сделок/мин: {st.get('сделок/мин')}, прострелов: {st.get('прострелов')}")
        notifier.hub = BotHub(notifier.tg_token, notifier.tg_chats[0], args.threshold, log,
                              bot_status)
        notifier.hub.start()
        log("[bot] публичный бот включён: любой может написать боту /start")
    if os.getenv("PORT"):
        start_web(int(os.getenv("PORT")), scr.state)
    secret = (os.getenv("FINAM_SECRET") or "").strip()
    try:
        if secret:
            from finam_source import FinamFeed
            scr.source = "Finam, подключаюсь…"
            while True:
                try:
                    scr.refresh_instruments(force=True)
                    break
                except Exception as e:
                    log(f"ошибка ISS (список фьючей): {type(e).__name__}; повтор через 10с")
                    time.sleep(10)
            notifier.send_text(f"✅ Скринер запущен (Finam, реальное время): {scr.coverage()}")
            FinamFeed(scr, secret).run()
        else:
            scr.run()
    except KeyboardInterrupt:
        print("\nСтоп.")


if __name__ == "__main__":
    main()
