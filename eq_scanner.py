#!/usr/bin/env python3
"""
EQ Scanner - Binance USDT-M Perpetual Futures.

Считает дневной EQ (середина между последним pivot high и pivot low)
по ЗАКРЫТЫМ дневным барам - так же, как индикатор EQ Multi-TF в TradingView
(lookahead_on + [1], то есть текущий незакрытый день в расчёт не входит).

Цены получает одним WebSocket-потоком на все символы сразу, поэтому REST
используется только раз в сутки для пересчёта уровней. Алерт уходит
в Telegram в момент пересечения ценой уровня EQ.
"""

import asyncio
import json
import logging
import os
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

import aiohttp

# ============================================================
# НАСТРОЙКИ
# ============================================================

TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN", "СЮДА_ТОКЕН_БОТА")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "СЮДА_CHAT_ID")

# Параметры EQ - должны совпадать с настройками индикатора в TradingView
PIVOT_LEFT = 17          # Pivot левое плечо
PIVOT_RIGHT = 1          # Pivot правое плечо
KLINE_INTERVAL = "1d"    # Таймфрейм зоны
KLINE_LIMIT = 120        # Сколько баров тянуть (нужно минимум PIVOT_LEFT+PIVOT_RIGHT+2)

# Мёртвая зона вокруг EQ в процентах.
# Пока цена внутри неё, пересечение не засчитывается - защита от дребезга,
# когда цена стоит вплотную к уровню и дёргается туда-сюда.
DEADBAND_PCT = 0.05

# Троттлинг: не чаще одного алерта по одному символу за столько минут
SYMBOL_COOLDOWN_MIN = 30

# Пейсинг REST-запросов (щадящий режим, чтобы гарантированно не поймать бан)
REST_CONCURRENCY = 3
REST_DELAY = 0.2

# Telegram: собираем алерты в пачку за это окно, чтобы не спамить
# отдельным сообщением на каждую монету при общем движении рынка
BATCH_WINDOW_SEC = 5
MAX_BATCH = 25

REST_BASE = "https://fapi.binance.com"
WS_URL = "wss://fstream.binance.com/ws/!miniTicker@arr"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("eq")


# ============================================================
# СОСТОЯНИЕ
# ============================================================

@dataclass
class SymbolState:
    eq: float
    side: str | None = None      # 'above' | 'below' | None (ещё не определено)
    last_alert_ts: float = 0.0


levels: dict[str, SymbolState] = {}
alert_queue: asyncio.Queue = asyncio.Queue()


# ============================================================
# РАСЧЁТ EQ
# ============================================================

def find_last_pivot(values: list[float], left: int, right: int, is_high: bool):
    """
    Ищет последний pivot. Центральный бар должен быть строго выше (ниже)
    всех соседей в окне - так же, как ta.pivothigh / ta.pivotlow в Pine.
    Возвращает значение пивота или None.
    """
    n = len(values)
    for i in range(n - right - 1, left - 1, -1):
        v = values[i]
        ok = True
        for j in range(i - left, i + right + 1):
            if j == i:
                continue
            if is_high:
                if values[j] >= v:
                    ok = False
                    break
            else:
                if values[j] <= v:
                    ok = False
                    break
        if ok:
            return v
    return None


def compute_eq(highs: list[float], lows: list[float]) -> float | None:
    """EQ = середина между последним pivot high и последним pivot low."""
    ph = find_last_pivot(highs, PIVOT_LEFT, PIVOT_RIGHT, True)
    pl = find_last_pivot(lows, PIVOT_LEFT, PIVOT_RIGHT, False)
    if ph is None or pl is None:
        return None
    return (ph + pl) / 2.0


# ============================================================
# REST
# ============================================================

async def fetch_symbols(session: aiohttp.ClientSession) -> list[str]:
    """Все торгуемые USDT-перпетуалы."""
    url = f"{REST_BASE}/fapi/v1/exchangeInfo"
    async with session.get(url, timeout=aiohttp.ClientTimeout(total=30)) as r:
        r.raise_for_status()
        data = await r.json()

    out = []
    for s in data.get("symbols", []):
        if (
            s.get("contractType") == "PERPETUAL"
            and s.get("quoteAsset") == "USDT"
            and s.get("status") == "TRADING"
        ):
            out.append(s["symbol"])
    return sorted(out)


async def fetch_klines(
    session: aiohttp.ClientSession,
    sem: asyncio.Semaphore,
    symbol: str,
    retries: int = 3,
):
    """Тянет свечи. Возвращает список [high, low] по ЗАКРЫТЫМ барам."""
    url = f"{REST_BASE}/fapi/v1/klines"
    params = {"symbol": symbol, "interval": KLINE_INTERVAL, "limit": KLINE_LIMIT}

    async with sem:
        for attempt in range(retries):
            try:
                async with session.get(
                    url, params=params, timeout=aiohttp.ClientTimeout(total=20)
                ) as r:
                    if r.status in (429, 418):
                        wait = int(r.headers.get("Retry-After", "60"))
                        log.warning("%s: rate limit %s, пауза %ss", symbol, r.status, wait)
                        await asyncio.sleep(wait)
                        continue
                    r.raise_for_status()
                    rows = await r.json()
            except Exception as e:
                log.debug("%s: попытка %s не удалась (%s)", symbol, attempt + 1, e)
                await asyncio.sleep(1.5 * (attempt + 1))
                continue

            await asyncio.sleep(REST_DELAY)

            # Последний бар ещё формируется - отбрасываем его.
            closed = rows[:-1] if rows else []
            if len(closed) < PIVOT_LEFT + PIVOT_RIGHT + 2:
                return None
            highs = [float(k[2]) for k in closed]
            lows = [float(k[3]) for k in closed]
            return highs, lows

    return None


async def refresh_levels(session: aiohttp.ClientSession):
    """Полный пересчёт EQ по всем символам."""
    t0 = time.monotonic()
    try:
        symbols = await fetch_symbols(session)
    except Exception as e:
        log.error("Не удалось получить список символов: %s", e)
        return

    log.info("Пересчёт EQ: %s символов", len(symbols))
    sem = asyncio.Semaphore(REST_CONCURRENCY)

    results = await asyncio.gather(
        *(fetch_klines(session, sem, s) for s in symbols),
        return_exceptions=True,
    )

    new_levels: dict[str, SymbolState] = {}
    skipped = 0

    for sym, res in zip(symbols, results):
        if isinstance(res, Exception) or res is None:
            skipped += 1
            continue
        highs, lows = res
        eq = compute_eq(highs, lows)
        if eq is None or eq <= 0:
            skipped += 1
            continue

        old = levels.get(sym)
        st = SymbolState(eq=eq)
        # Уровень изменился - сбрасываем сторону, следующий тик задаст базу заново.
        # Кулдаун переносим, чтобы пересчёт не обнулял защиту от спама.
        if old is not None:
            st.last_alert_ts = old.last_alert_ts
            if abs(old.eq - eq) / eq < 1e-9:
                st.side = old.side
        new_levels[sym] = st

    levels.clear()
    levels.update(new_levels)
    log.info(
        "EQ готов: %s уровней, пропущено %s, заняло %.1fs",
        len(levels), skipped, time.monotonic() - t0,
    )


# ============================================================
# TELEGRAM
# ============================================================

async def send_telegram(session: aiohttp.ClientSession, text: str):
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": text,
        "parse_mode": "HTML",
        "disable_web_page_preview": True,
    }
    for attempt in range(3):
        try:
            async with session.post(
                url, json=payload, timeout=aiohttp.ClientTimeout(total=20)
            ) as r:
                if r.status == 429:
                    body = await r.json()
                    wait = body.get("parameters", {}).get("retry_after", 5)
                    await asyncio.sleep(wait)
                    continue
                if r.status != 200:
                    log.warning("Telegram %s: %s", r.status, await r.text())
                return
        except Exception as e:
            log.warning("Telegram ошибка: %s", e)
            await asyncio.sleep(2 * (attempt + 1))


def fmt_price(p: float) -> str:
    if p >= 100:
        return f"{p:.2f}"
    if p >= 1:
        return f"{p:.4f}"
    return f"{p:.6f}".rstrip("0")


def tv_link(symbol: str) -> str:
    """Ссылка на график TradingView. На Android откроется в приложении,
    если в Telegram выключен встроенный браузер."""
    return f"https://www.tradingview.com/chart/?symbol=BINANCE%3A{symbol}.P"


def format_batch(items: list[tuple]) -> str:
    head = f"Пересечение EQ {KLINE_INTERVAL.upper()}"
    lines = [f"<b>{head}</b>", ""]
    for sym, direction, price, eq in items:
        arrow = "▲" if direction == "up" else "▼"
        pct = (price - eq) / eq * 100
        lines.append(
            f'{arrow} <a href="{tv_link(sym)}"><b>{sym}</b></a>  {fmt_price(price)}'
            f"   EQ {fmt_price(eq)}  ({pct:+.2f}%)"
        )
    return "\n".join(lines)


async def telegram_worker(session: aiohttp.ClientSession):
    """Собирает алерты в пачку и отправляет одним сообщением."""
    while True:
        first = await alert_queue.get()
        batch = [first]
        deadline = time.monotonic() + BATCH_WINDOW_SEC

        while len(batch) < MAX_BATCH:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                break
            try:
                item = await asyncio.wait_for(alert_queue.get(), timeout=remaining)
                batch.append(item)
            except asyncio.TimeoutError:
                break

        await send_telegram(session, format_batch(batch))
        await asyncio.sleep(1.0)


# ============================================================
# WEBSOCKET
# ============================================================

def check_cross(symbol: str, price: float):
    """Проверяет пересечение и ставит алерт в очередь."""
    st = levels.get(symbol)
    if st is None or st.eq <= 0:
        return

    band = st.eq * DEADBAND_PCT / 100.0
    if price > st.eq + band:
        new_side = "above"
    elif price < st.eq - band:
        new_side = "below"
    else:
        return  # внутри мёртвой зоны - ничего не меняем

    if st.side is None:
        st.side = new_side       # первая установка базы, без алерта
        return

    if new_side == st.side:
        return

    st.side = new_side

    now = time.time()
    if now - st.last_alert_ts < SYMBOL_COOLDOWN_MIN * 60:
        return
    st.last_alert_ts = now

    direction = "up" if new_side == "above" else "down"
    alert_queue.put_nowait((symbol, direction, price, st.eq))
    log.info("%s  %s  цена %s  EQ %s", symbol, direction.upper(),
             fmt_price(price), fmt_price(st.eq))


async def ws_loop(session: aiohttp.ClientSession):
    """Один поток на все символы. Переподключается сам."""
    backoff = 1
    while True:
        try:
            async with session.ws_connect(
                WS_URL, heartbeat=30, timeout=aiohttp.ClientTimeout(total=None)
            ) as ws:
                log.info("WebSocket подключен")
                backoff = 1

                async for msg in ws:
                    if msg.type == aiohttp.WSMsgType.TEXT:
                        try:
                            tickers = json.loads(msg.data)
                        except Exception:
                            continue
                        if not isinstance(tickers, list):
                            continue
                        for t in tickers:
                            sym = t.get("s")
                            c = t.get("c")
                            if sym and c:
                                try:
                                    check_cross(sym, float(c))
                                except Exception as e:
                                    log.debug("check_cross %s: %s", sym, e)

                    elif msg.type in (
                        aiohttp.WSMsgType.CLOSED,
                        aiohttp.WSMsgType.ERROR,
                    ):
                        break

        except Exception as e:
            log.warning("WebSocket оборвался: %s", e)

        log.info("Переподключение через %ss", backoff)
        await asyncio.sleep(backoff)
        backoff = min(backoff * 2, 60)


# ============================================================
# ПЛАНИРОВЩИК
# ============================================================

def seconds_until_next_daily_close() -> float:
    """Дневной бар Binance закрывается в 00:00 UTC. Ждём чуть после."""
    now = datetime.now(timezone.utc)
    nxt = (now + timedelta(days=1)).replace(
        hour=0, minute=0, second=45, microsecond=0
    )
    return max((nxt - now).total_seconds(), 60)


async def refresh_scheduler(session: aiohttp.ClientSession):
    while True:
        wait = seconds_until_next_daily_close()
        log.info("Следующий пересчёт EQ через %.1f ч", wait / 3600)
        await asyncio.sleep(wait)
        await refresh_levels(session)


# ============================================================
# MAIN
# ============================================================

async def main():
    if "СЮДА" in TELEGRAM_TOKEN or "СЮДА" in TELEGRAM_CHAT_ID:
        log.error("Не заданы TELEGRAM_TOKEN / TELEGRAM_CHAT_ID")
        return

    conn = aiohttp.TCPConnector(limit=20, ttl_dns_cache=300)
    async with aiohttp.ClientSession(connector=conn) as session:
        await refresh_levels(session)
        if not levels:
            log.error("Уровни не рассчитаны, выходим")
            return

        await send_telegram(
            session,
            f"<b>EQ Scanner запущен</b>\n"
            f"Символов: {len(levels)}\n"
            f"EQ: {KLINE_INTERVAL.upper()}, pivot {PIVOT_LEFT}/{PIVOT_RIGHT}",
        )

        await asyncio.gather(
            ws_loop(session),
            refresh_scheduler(session),
            telegram_worker(session),
        )


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log.info("Остановлено")
