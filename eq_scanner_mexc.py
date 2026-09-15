#!/usr/bin/env python3
"""
EQ Scanner - MEXC USDT Perpetual Futures.

Считает дневной EQ (середина между последним pivot high и pivot low)
по ЗАКРЫТЫМ дневным барам - как индикатор EQ Multi-TF в TradingView.

Цены берёт одним WebSocket-потоком sub.tickers на все контракты сразу,
REST дёргает только раз в сутки. Алерт уходит в Telegram в момент
пересечения ценой уровня EQ.

Отличия MEXC от Binance:
  - символы вида BTC_USDT (с подчёркиванием)
  - свечи приходят колонками: data.time[], data.high[], data.low[]
  - интервалы называются Day1 / Hour4 / Min60, а не 1d / 4h / 1h
  - лимит klines: 20 запросов / 2 секунды (жёстче, чем у Binance)
  - WebSocket требует прикладной ping {"method":"ping"}
"""

import asyncio
import json
import logging
import os
import signal
import time
import zlib
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone

import aiohttp
import aiohttp.web

# ============================================================
# НАСТРОЙКИ
# ============================================================

TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")

# Параметры EQ - должны совпадать с настройками индикатора в TradingView
PIVOT_LEFT = int(os.getenv("PIVOT_LEFT", "17"))
PIVOT_RIGHT = int(os.getenv("PIVOT_RIGHT", "1"))

# Интервал MEXC: Min1 Min5 Min15 Min30 Min60 Hour4 Hour8 Day1 Week1 Month1
KLINE_INTERVAL = os.getenv("KLINE_INTERVAL", "Day1")
KLINE_LIMIT = 120

# Мёртвая зона вокруг EQ в процентах (защита от дребезга у уровня)
DEADBAND_PCT = float(os.getenv("DEADBAND_PCT", "0.05"))

# Не чаще одного алерта по символу за столько минут
SYMBOL_COOLDOWN_MIN = int(os.getenv("SYMBOL_COOLDOWN_MIN", "30"))

# Как часто перезапрашивать список контрактов
SYMBOLS_TTL_DAYS = int(os.getenv("SYMBOLS_TTL_DAYS", "15"))

# Пейсинг REST. Лимит MEXC: 20 klines / 2 сек. Держимся заметно ниже.
REST_CONCURRENCY = int(os.getenv("REST_CONCURRENCY", "3"))
REST_DELAY = float(os.getenv("REST_DELAY", "0.35"))

# Telegram: пачка алертов вместо потока сообщений
BATCH_WINDOW_SEC = 5
MAX_BATCH = 25

_here = os.path.dirname(os.path.abspath(__file__)) if "__file__" in globals() else os.getcwd()
STATE_DIR = os.getenv("STATE_DIR", _here)
SYMBOLS_CACHE = os.path.join(STATE_DIR, "mexc_symbols_cache.json")
STATE_FILE = os.path.join(STATE_DIR, "mexc_eq_state.json")

# Источник цен: auto (WS, при сбое REST) | ws | rest
FEED_MODE = os.getenv("FEED_MODE", "auto").lower()
REST_POLL_SEC = int(os.getenv("REST_POLL_SEC", "10"))

# HTTP-эндпоинт: на Render нужен (Web Service + UptimeRobot)
ENABLE_HTTP = os.getenv("ENABLE_HTTP", "1") == "1"
HTTP_PORT = int(os.getenv("PORT", "10000"))

REST_BASE = "https://contract.mexc.com"
WS_URL = "wss://contract.mexc.com/edge"

# Сколько секунд в одном баре - для расчёта параметра start
INTERVAL_SECONDS = {
    "Min1": 60, "Min5": 300, "Min15": 900, "Min30": 1800,
    "Min60": 3600, "Hour4": 14400, "Hour8": 28800,
    "Day1": 86400, "Week1": 604800, "Month1": 2592000,
}

# Некоторые точки MEXC стоят за анти-бот фильтром и режут запросы
# без внятного User-Agent. Для публичных данных это обычная практика.
WS_HEADERS = {
    "User-Agent": "eq-scanner/1.0 (+aiohttp)",
    "Origin": "https://futures.mexc.com",
}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("eq-mexc")

ban_flag = {"until": 0.0}
# last_msg — время последнего тика из WS, source — что реально питает бота
feed = {"last_msg": 0.0, "source": "none", "ws_fails": 0}


# ============================================================
# СОСТОЯНИЕ
# ============================================================

@dataclass
class SymbolState:
    eq: float
    side: str | None = None      # 'above' | 'below' | None
    last_alert_ts: float = 0.0


levels: dict[str, SymbolState] = {}
alert_queue: asyncio.Queue = asyncio.Queue()


def save_state():
    try:
        data = {s: asdict(st) for s, st in levels.items()}
        tmp = STATE_FILE + ".tmp"
        with open(tmp, "w") as f:
            json.dump({"saved_at": time.time(), "levels": data}, f)
        os.replace(tmp, STATE_FILE)
    except Exception as e:
        log.warning("Не удалось сохранить состояние: %s", e)


def load_state() -> dict[str, SymbolState]:
    try:
        with open(STATE_FILE) as f:
            raw = json.load(f)
        out = {}
        for sym, d in raw.get("levels", {}).items():
            out[sym] = SymbolState(
                eq=float(d["eq"]),
                side=d.get("side"),
                last_alert_ts=float(d.get("last_alert_ts", 0.0)),
            )
        log.info("Загружено состояние: %s символов", len(out))
        return out
    except FileNotFoundError:
        return {}
    except Exception as e:
        log.warning("Не удалось загрузить состояние: %s", e)
        return {}


# ============================================================
# РАСЧЁТ EQ
# ============================================================

def find_last_pivot(values: list[float], left: int, right: int, is_high: bool):
    """Последний pivot: центральный бар строго выше (ниже) всех соседей."""
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
    ph = find_last_pivot(highs, PIVOT_LEFT, PIVOT_RIGHT, True)
    pl = find_last_pivot(lows, PIVOT_LEFT, PIVOT_RIGHT, False)
    if ph is None or pl is None:
        return None
    return (ph + pl) / 2.0


# ============================================================
# СПИСОК КОНТРАКТОВ (с кэшем на диске)
# ============================================================

def read_symbols_cache():
    try:
        with open(SYMBOLS_CACHE) as f:
            raw = json.load(f)
        return raw.get("symbols", []), float(raw.get("fetched_at", 0))
    except FileNotFoundError:
        return [], 0.0
    except Exception as e:
        log.warning("Кэш символов повреждён: %s", e)
        return [], 0.0


def write_symbols_cache(symbols: list[str]):
    try:
        tmp = SYMBOLS_CACHE + ".tmp"
        with open(tmp, "w") as f:
            json.dump({"fetched_at": time.time(), "symbols": symbols}, f)
        os.replace(tmp, SYMBOLS_CACHE)
    except Exception as e:
        log.warning("Не удалось записать кэш символов: %s", e)


async def fetch_symbols_api(session: aiohttp.ClientSession) -> list[str]:
    """USDT-перпетуалы в статусе «торгуется»."""
    url = f"{REST_BASE}/api/v1/contract/detail"
    async with session.get(url, timeout=aiohttp.ClientTimeout(total=30)) as r:
        if r.status in (403, 451):
            ban_flag["until"] = time.time() + 3600
            raise RuntimeError(f"Доступ заблокирован ({r.status})")
        if r.status == 429:
            ban_flag["until"] = time.time() + 300
            raise RuntimeError("429 на contract/detail")
        r.raise_for_status()
        body = await r.json()

    if not body.get("success", False):
        raise RuntimeError(f"MEXC вернул ошибку: {body.get('code')}")

    out = []
    for c in body.get("data", []):
        # futureType 1 = перпетуал, state 0 = торгуется
        if (
            c.get("futureType") == 1
            and c.get("quoteCoin") == "USDT"
            and c.get("state") == 0
        ):
            out.append(c["symbol"])
    return sorted(out)


async def load_symbols(session: aiohttp.ClientSession) -> list[str]:
    cached, fetched_at = read_symbols_cache()
    age_days = (time.time() - fetched_at) / 86400 if fetched_at else 1e9

    if cached and age_days < SYMBOLS_TTL_DAYS:
        log.info("Символы из кэша: %s шт (возраст %.1f дн)", len(cached), age_days)
        return cached

    try:
        fresh = await fetch_symbols_api(session)
        if fresh:
            write_symbols_cache(fresh)
            added = set(fresh) - set(cached)
            removed = set(cached) - set(fresh)
            log.info("Символы обновлены: %s шт (+%s / -%s)",
                     len(fresh), len(added), len(removed))
            if cached and added:
                log.info("Новые: %s", sorted(added))
            if cached and removed:
                log.info("Убраны: %s", sorted(removed))
            return fresh
    except Exception as e:
        log.error("Не удалось обновить список контрактов: %s", e)

    if cached:
        log.warning("Работаю на устаревшем кэше: %s шт", len(cached))
        return cached

    return []


# ============================================================
# KLINES
# ============================================================

async def fetch_klines(
    session: aiohttp.ClientSession,
    sem: asyncio.Semaphore,
    symbol: str,
    retries: int = 3,
):
    """
    Возвращает (highs, lows) по ЗАКРЫТЫМ барам или None.
    MEXC отдаёт данные колонками, а не списком свечей.
    """
    url = f"{REST_BASE}/api/v1/contract/kline/{symbol}"
    step = INTERVAL_SECONDS.get(KLINE_INTERVAL, 86400)
    start = int(time.time()) - (KLINE_LIMIT + 5) * step
    params = {"interval": KLINE_INTERVAL, "start": start}

    async with sem:
        if time.time() < ban_flag["until"]:
            return None

        for attempt in range(retries):
            try:
                async with session.get(
                    url, params=params, timeout=aiohttp.ClientTimeout(total=20)
                ) as r:
                    if r.status in (403, 451):
                        ban_flag["until"] = time.time() + 3600
                        log.error("Доступ заблокирован (%s) — пересчёт прерван", r.status)
                        return None
                    if r.status == 429:
                        log.warning("%s: 429, пауза 5s", symbol)
                        await asyncio.sleep(5)
                        continue
                    r.raise_for_status()
                    body = await r.json()
            except asyncio.CancelledError:
                raise
            except Exception as e:
                log.debug("%s: попытка %s (%s)", symbol, attempt + 1, e)
                await asyncio.sleep(1.5 * (attempt + 1))
                continue

            await asyncio.sleep(REST_DELAY)

            if not body.get("success", False):
                return None

            data = body.get("data") or {}
            highs_all = data.get("high") or []
            lows_all = data.get("low") or []
            if len(highs_all) != len(lows_all) or not highs_all:
                return None

            # Последний бар ещё формируется — отбрасываем
            highs = [float(x) for x in highs_all[:-1]]
            lows = [float(x) for x in lows_all[:-1]]

            if len(highs) < PIVOT_LEFT + PIVOT_RIGHT + 2:
                return None
            return highs, lows

    return None


async def refresh_levels(session: aiohttp.ClientSession):
    t0 = time.monotonic()

    if time.time() < ban_flag["until"]:
        left = int(ban_flag["until"] - time.time())
        log.warning("Доступ ещё заблокирован (%ss), пересчёт пропущен", left)
        return

    symbols = await load_symbols(session)
    if not symbols:
        log.error("Список контрактов пуст — пересчёт невозможен")
        return

    log.info("Пересчёт EQ: %s контрактов (это займёт пару минут)", len(symbols))
    sem = asyncio.Semaphore(REST_CONCURRENCY)

    results = await asyncio.gather(
        *(fetch_klines(session, sem, s) for s in symbols),
        return_exceptions=True,
    )

    new_levels: dict[str, SymbolState] = {}
    skipped = 0

    for sym, res in zip(symbols, results):
        if isinstance(res, BaseException) or res is None:
            skipped += 1
            continue
        highs, lows = res
        eq = compute_eq(highs, lows)
        if eq is None or eq <= 0:
            skipped += 1
            continue

        st = SymbolState(eq=eq)
        old = levels.get(sym)
        if old is not None:
            st.last_alert_ts = old.last_alert_ts
            if abs(old.eq - eq) / eq < 1e-9:
                st.side = old.side
        new_levels[sym] = st

    if not new_levels:
        log.error("Ни одного уровня не получено, старые оставлены")
        return

    levels.clear()
    levels.update(new_levels)
    save_state()
    log.info("EQ готов: %s уровней, пропущено %s, заняло %.1fs",
             len(levels), skipped, time.monotonic() - t0)


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
        except asyncio.CancelledError:
            raise
        except Exception as e:
            log.warning("Telegram ошибка: %s", e)
            await asyncio.sleep(2 * (attempt + 1))


def fmt_price(p: float) -> str:
    if p >= 100:
        return f"{p:.2f}"
    if p >= 1:
        return f"{p:.4f}"
    return f"{p:.8f}".rstrip("0")


def tv_link(symbol: str) -> str:
    """MEXC:BTCUSDT.P — на TradingView символ без подчёркивания."""
    clean = symbol.replace("_", "")
    return f"https://www.tradingview.com/chart/?symbol=MEXC%3A{clean}.P"


def format_batch(items: list[tuple]) -> str:
    lines = [f"<b>MEXC · пересечение EQ {KLINE_INTERVAL}</b>", ""]
    for sym, direction, price, eq in items:
        arrow = "▲" if direction == "up" else "▼"
        pct = (price - eq) / eq * 100
        lines.append(
            f'{arrow} <a href="{tv_link(sym)}"><b>{sym}</b></a>  {fmt_price(price)}'
            f"   EQ {fmt_price(eq)}  ({pct:+.2f}%)"
        )
    return "\n".join(lines)


async def telegram_worker(session: aiohttp.ClientSession):
    while True:
        first = await alert_queue.get()
        batch = [first]
        deadline = time.monotonic() + BATCH_WINDOW_SEC

        while len(batch) < MAX_BATCH:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                break
            try:
                batch.append(await asyncio.wait_for(alert_queue.get(), timeout=remaining))
            except asyncio.TimeoutError:
                break

        await send_telegram(session, format_batch(batch))
        await asyncio.sleep(1.0)


# ============================================================
# REST-ФИД (запасной источник цен)
# ============================================================

async def fetch_all_tickers(session: aiohttp.ClientSession):
    """Один запрос — цены всех контрактов. Лимит MEXC: 10 / 2 сек."""
    url = f"{REST_BASE}/api/v1/contract/ticker"
    try:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=20)) as r:
            if r.status in (403, 451):
                ban_flag["until"] = time.time() + 600
                log.error("REST-тикеры заблокированы (%s)", r.status)
                return None
            if r.status == 429:
                log.warning("REST-тикеры: 429, пауза 10s")
                await asyncio.sleep(10)
                return None
            r.raise_for_status()
            body = await r.json()
    except asyncio.CancelledError:
        raise
    except Exception as e:
        log.debug("REST-тикеры: %s", e)
        return None

    if not body.get("success", False):
        return None
    data = body.get("data")
    if isinstance(data, dict):
        data = [data]
    return data if isinstance(data, list) else None


async def rest_poll_loop(session: aiohttp.ClientSession):
    """
    Опрашивает цены по REST. В режиме auto работает только тогда,
    когда WebSocket молчит дольше 30 секунд.
    """
    if FEED_MODE == "ws":
        return

    while True:
        await asyncio.sleep(REST_POLL_SEC)

        if FEED_MODE == "auto" and time.time() - feed["last_msg"] < 30:
            continue
        if time.time() < ban_flag["until"]:
            continue

        data = await fetch_all_tickers(session)
        if not data:
            continue

        if feed["source"] != "rest":
            log.info("Источник цен: REST-опрос раз в %ss", REST_POLL_SEC)
        feed["source"] = "rest"

        for t in data:
            sym = t.get("symbol")
            price = t.get("lastPrice")
            if sym and price is not None:
                try:
                    check_cross(sym, float(price))
                except Exception as e:
                    log.debug("check_cross %s: %s", sym, e)


# ============================================================
# WEBSOCKET
# ============================================================

def check_cross(symbol: str, price: float):
    st = levels.get(symbol)
    if st is None or st.eq <= 0:
        return

    band = st.eq * DEADBAND_PCT / 100.0
    if price > st.eq + band:
        new_side = "above"
    elif price < st.eq - band:
        new_side = "below"
    else:
        return

    if st.side is None:
        st.side = new_side          # первая установка базы, без алерта
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
    log.info("%s  %s  цена %s  EQ %s",
             symbol, direction.upper(), fmt_price(price), fmt_price(st.eq))


def handle_ws_payload(raw: str):
    try:
        msg = json.loads(raw)
    except Exception:
        return
    if not isinstance(msg, dict):
        return
    if msg.get("channel") != "push.tickers":
        return
    feed["last_msg"] = time.time()
    if feed["source"] != "ws":
        log.info("Источник цен: WebSocket")
        feed["source"] = "ws"
    data = msg.get("data")
    if not isinstance(data, list):
        return
    for t in data:
        sym = t.get("symbol")
        price = t.get("lastPrice")
        if sym and price is not None:
            try:
                check_cross(sym, float(price))
            except Exception as e:
                log.debug("check_cross %s: %s", sym, e)


async def ws_ping(ws):
    """MEXC требует прикладной ping, протокольного heartbeat недостаточно."""
    while True:
        await asyncio.sleep(15)
        try:
            await ws.send_json({"method": "ping"})
        except Exception:
            return


async def ws_loop(session: aiohttp.ClientSession):
    if FEED_MODE == "rest":
        log.info("FEED_MODE=rest — WebSocket не используется")
        return

    backoff = 1
    while True:
        ping_task = None
        try:
            async with session.ws_connect(
                WS_URL,
                headers=WS_HEADERS,
                timeout=aiohttp.ClientTimeout(total=None),
            ) as ws:
                await ws.send_json({
                    "method": "sub.tickers",
                    "param": {},
                    "gzip": False,
                })
                log.info("WebSocket подключен, подписка sub.tickers отправлена")
                backoff = 1
                feed["ws_fails"] = 0
                ping_task = asyncio.create_task(ws_ping(ws))

                while True:
                    # тикеры приходят раз в 2с, 60с молчания = мёртвое соединение
                    msg = await asyncio.wait_for(ws.receive(), timeout=60)

                    if msg.type == aiohttp.WSMsgType.TEXT:
                        handle_ws_payload(msg.data)
                    elif msg.type == aiohttp.WSMsgType.BINARY:
                        # подстраховка, если gzip:false не подхватился
                        try:
                            handle_ws_payload(
                                zlib.decompress(msg.data, 16 + zlib.MAX_WBITS).decode()
                            )
                        except Exception:
                            pass
                    elif msg.type in (
                        aiohttp.WSMsgType.CLOSED,
                        aiohttp.WSMsgType.CLOSING,
                        aiohttp.WSMsgType.ERROR,
                    ):
                        break

        except asyncio.CancelledError:
            raise
        except asyncio.TimeoutError:
            log.warning("WebSocket молчит 60с — переподключаюсь")
            feed["ws_fails"] += 1
        except Exception as e:
            feed["ws_fails"] += 1
            txt = str(e)
            if "403" in txt or "401" in txt:
                # отказ на рукопожатии: биржа не пускает с этого IP,
                # частить бесполезно — ждём дольше, цены идут через REST
                backoff = max(backoff, 300)
                if feed["ws_fails"] in (1, 5) or feed["ws_fails"] % 20 == 0:
                    log.warning("WebSocket отклонён (%s), попытка №%s. "
                                "Цены берутся по REST.", txt[:60], feed["ws_fails"])
            else:
                log.warning("WebSocket оборвался: %s", e)
        finally:
            if ping_task:
                ping_task.cancel()

        await asyncio.sleep(backoff)
        backoff = min(backoff * 2, 600)


# ============================================================
# ПЛАНИРОВЩИКИ
# ============================================================

def seconds_until_next_daily_close() -> float:
    now = datetime.now(timezone.utc)
    nxt = (now + timedelta(days=1)).replace(hour=0, minute=1, second=0, microsecond=0)
    return max((nxt - now).total_seconds(), 60)


async def refresh_scheduler(session: aiohttp.ClientSession):
    while True:
        wait = seconds_until_next_daily_close()
        log.info("Следующий пересчёт EQ через %.1f ч", wait / 3600)
        await asyncio.sleep(wait)
        await refresh_levels(session)


async def retry_until_ready(session: aiohttp.ClientSession):
    while True:
        await asyncio.sleep(3600)
        if levels:
            continue
        log.info("Повторная попытка пересчёта EQ")
        await refresh_levels(session)
        if levels:
            await send_telegram(
                session, f"<b>MEXC EQ Scanner: уровни получены</b>\nКонтрактов: {len(levels)}"
            )


async def state_saver():
    while True:
        await asyncio.sleep(300)
        save_state()


# ============================================================
# HTTP
# ============================================================

async def keepalive_server():
    async def handler(request):
        ban_left = max(0, int(ban_flag["until"] - time.time()))
        body = (
            f"ok\n"
            f"exchange: MEXC\n"
            f"levels: {len(levels)}\n"
            f"interval: {KLINE_INTERVAL}\n"
            f"pivot: {PIVOT_LEFT}/{PIVOT_RIGHT}\n"
            f"ban_left: {ban_left}s\n"
            f"feed: {feed['source']}\n"
            f"ws_fails: {feed['ws_fails']}\n"
        )
        return aiohttp.web.Response(text=body, content_type="text/plain")

    app = aiohttp.web.Application()
    app.router.add_get("/", handler)
    app.router.add_get("/health", handler)
    runner = aiohttp.web.AppRunner(app)
    await runner.setup()
    await aiohttp.web.TCPSite(runner, "0.0.0.0", HTTP_PORT).start()
    log.info("HTTP-эндпоинт на порту %s", HTTP_PORT)


# ============================================================
# MAIN
# ============================================================

async def main():
    missing = []
    if not TELEGRAM_TOKEN:
        missing.append("TELEGRAM_TOKEN")
    if not TELEGRAM_CHAT_ID:
        missing.append("TELEGRAM_CHAT_ID")
    if missing:
        log.error("Не заданы: %s", ", ".join(missing))
        seen = [k for k in os.environ
                if any(w in k.upper() for w in ("TELE", "CHAT", "TOKEN"))]
        log.error("Похожие переменные в окружении: %s", seen or "нет")
        return

    if KLINE_INTERVAL not in INTERVAL_SECONDS:
        log.error("Неверный KLINE_INTERVAL=%s. Допустимо: %s",
                  KLINE_INTERVAL, ", ".join(INTERVAL_SECONDS))
        return

    log.info("STATE_DIR: %s", STATE_DIR)
    levels.update(load_state())

    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            loop.add_signal_handler(sig, stop.set)
        except NotImplementedError:
            pass

    conn = aiohttp.TCPConnector(limit=20, ttl_dns_cache=300)
    async with aiohttp.ClientSession(connector=conn) as session:
        if ENABLE_HTTP:
            await keepalive_server()

        await refresh_levels(session)

        if levels:
            await send_telegram(
                session,
                f"<b>MEXC EQ Scanner запущен</b>\n"
                f"Контрактов: {len(levels)}\n"
                f"EQ: {KLINE_INTERVAL}, pivot {PIVOT_LEFT}/{PIVOT_RIGHT}",
            )
        else:
            log.error("Уровни не рассчитаны. Повтор через час.")
            await send_telegram(
                session,
                "<b>MEXC EQ Scanner: уровни не рассчитаны</b>\n"
                "Биржа не отдала данные. Повтор через час.",
            )

        tasks = [
            asyncio.create_task(ws_loop(session)),
            asyncio.create_task(rest_poll_loop(session)),
            asyncio.create_task(refresh_scheduler(session)),
            asyncio.create_task(telegram_worker(session)),
            asyncio.create_task(retry_until_ready(session)),
            asyncio.create_task(state_saver()),
        ]

        await stop.wait()
        log.info("Получен сигнал остановки, сохраняю состояние")
        save_state()
        for t in tasks:
            t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        save_state()
        log.info("Остановлено")
