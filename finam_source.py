"""
Источник сделок в реальном времени: Finam Trade API (gRPC, пакет finam-sdk).

На каждый фьючерс открывается свой стрим SubscribeLatestTrades — так устроен
API (один symbol на подписку). Список фьючей и их ликвидность берутся из ISS,
символы Finam (ticker@mic) — из справочника инструментов Finam.

Включается переменной окружения FINAM_SECRET (секрет/токен из кабинета Финама).
"""
from __future__ import annotations

import asyncio
import os
import time
from collections import deque

from moex_spike_screener import Screener, log

SIDES = {1: "B", 2: "S"}  # Side.SIDE_BUY / SIDE_SELL


def _dec(d) -> float:
    """google.type.Decimal -> float."""
    v = getattr(d, "value", "") if d is not None else ""
    try:
        return float(v) if v else 0.0
    except ValueError:
        return 0.0


class FinamFeed:
    def __init__(self, scr: Screener, secret: str):
        self.scr = scr
        self.secret = secret
        self.max_streams = int(os.getenv("FINAM_MAX_STREAMS", 200))
        self.fallback_mic = os.getenv("FINAM_FUT_MIC", "RTSX")
        self.symbols: dict[str, str] = {}       # secid -> ticker@mic
        self.tasks: dict[str, asyncio.Task] = {}
        self.seen: dict[str, deque] = {}        # secid -> последние trade_id (дедуп)
        self.dead: set[str] = set()             # символы, которых нет у Финама
        self.errors: dict[str, str] = {}

    # --- запуск ---
    def run(self):
        asyncio.run(self._main())

    async def _main(self):
        from finam_trade_api import AsyncFinamClient

        backoff = 5
        while True:
            try:
                async with AsyncFinamClient(secret=self.secret) as client:
                    log("[finam] авторизация ok")
                    backoff = 5
                    await self._log_limits(client)
                    await self._resolve_symbols(client)
                    await self._supervise(client)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                self.scr.last_err = f"Finam: {type(e).__name__}"
                log(f"[finam] ошибка подключения: {type(e).__name__}: {str(e)[:300]}; "
                    f"повтор через {backoff}с")
                for t in self.tasks.values():
                    t.cancel()
                self.tasks.clear()
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 120)

    async def _log_limits(self, client):
        try:
            from finam_trade_api.metrics import GetUsageMetricsRequest
            resp = await client.metrics.GetUsageMetrics(GetUsageMetricsRequest())
            for q in getattr(resp, "quotas", []):
                log(f"[finam] квота {q.name}: {q.remaining}/{q.limit}")
        except Exception as e:
            log(f"[finam] квоты получить не удалось: {type(e).__name__}")

    async def _resolve_symbols(self, client):
        """ticker -> ticker@mic для фьючей из справочника Finam."""
        try:
            from finam_trade_api.assets import AssetsRequest
            resp = await client.assets.Assets(AssetsRequest())
            fut = {}
            for a in resp.assets:
                if "FUT" in (a.type or "").upper() and not a.is_archived:
                    fut[a.ticker] = a.symbol
            if fut:
                self.symbols = fut
                sample = ", ".join(list(fut.values())[:3])
                log(f"[finam] фьючей в справочнике: {len(fut)} (пример: {sample})")
                return
            log("[finam] в справочнике не нашлось фьючей — использую ticker@" + self.fallback_mic)
        except Exception as e:
            log(f"[finam] справочник недоступен ({type(e).__name__}) — использую ticker@"
                f"{self.fallback_mic}")
        self.symbols = {}

    def _symbol(self, secid: str) -> str:
        return self.symbols.get(secid) or f"{secid}@{self.fallback_mic}"

    # --- какие фьючи слушать ---
    def _wanted(self) -> list[str]:
        scr = self.scr
        cands = []
        for secid, info in scr.instr.items():
            asset = (info.get("asset") or "").upper()
            if scr.only and secid.upper() not in scr.only and asset not in scr.only:
                continue
            if secid.upper() in scr.excl or asset in scr.excl or secid in self.dead:
                continue
            if self.symbols and secid not in self.symbols:
                continue
            cands.append((info.get("num_trades", 0), info.get("prev_oi", 0), secid))
        cands.sort(reverse=True)
        return [s for *_, s in cands[: self.max_streams]]

    async def _supervise(self, client):
        last_refresh = 0.0
        while True:
            if time.time() - last_refresh > 600 or not self.tasks:
                last_refresh = time.time()
                await asyncio.to_thread(self.scr.refresh_instruments)
                new = [s for s in self._wanted() if s not in self.tasks]
                for secid in new:
                    self.tasks[secid] = asyncio.create_task(self._stream(client, secid))
                if new:
                    log(f"[finam] подписок: {len(self.tasks)} (+{len(new)})")
                    self.scr.source = f"Finam, реальное время, {len(self.tasks)} фьючей"
            else:
                await asyncio.to_thread(self.scr.refresh_instruments)  # num_trades для фильтра
            # если все стримы упали с ошибкой авторизации — переподключаемся целиком
            done = [s for s, t in self.tasks.items() if t.done()]
            for s in done:
                self.tasks.pop(s, None)
            await asyncio.sleep(60)

    # --- один стрим ---
    async def _stream(self, client, secid: str):
        from finam_trade_api.market_data import SubscribeLatestTradesRequest
        from finam_trade_api import NotFoundError, AuthError, PermissionDeniedError

        symbol = self._symbol(secid)
        seen = self.seen.setdefault(secid, deque(maxlen=2000))
        seen_set: set[str] = set()
        backoff = 1
        while True:
            try:
                req = SubscribeLatestTradesRequest(symbol=symbol)
                async for resp in client.market_data.SubscribeLatestTrades(req):
                    trades = list(resp.trades)
                    for t in trades:
                        tid = t.trade_id
                        if tid:
                            if tid in seen_set:
                                continue
                            if len(seen) == seen.maxlen:
                                seen_set.discard(seen[0])
                            seen.append(tid)
                            seen_set.add(tid)
                        price = _dec(t.price)
                        if price <= 0:
                            continue
                        ts = float(t.timestamp.seconds)
                        self.scr.processed += 1
                        # снэпшот = история при подписке: учитываем для «рынка», но не алертим
                        self.scr.handle_trade(secid, ts, price, int(_dec(t.size)),
                                              SIDES.get(int(t.side), ""),
                                              can_alert=not t.is_data_snapshot)
                    if trades:
                        self.scr.rate.append((time.time(), len(trades)))
                    backoff = 1
                    self.errors.pop(secid, None)
            except asyncio.CancelledError:
                raise
            except NotFoundError:
                log(f"[finam] {symbol}: инструмент не найден у Финама — пропускаю")
                self.dead.add(secid)
                return
            except (AuthError, PermissionDeniedError) as e:
                self.scr.last_err = f"Finam: нет доступа ({type(e).__name__})"
                log(f"[finam] {symbol}: нет доступа: {str(e)[:200]}")
                await asyncio.sleep(60)
            except Exception as e:
                msg = f"{type(e).__name__}: {str(e)[:200]}"
                if self.errors.get(secid) != msg:
                    log(f"[finam] {symbol}: стрим прервался ({msg}), переподключаюсь")
                    self.errors[secid] = msg
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 60)
