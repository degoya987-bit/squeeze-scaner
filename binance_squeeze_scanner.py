# -*- coding: utf-8 -*-
"""
Сканер Binance: экстремум импульса + сжатие волатильности (Squeeze)
=====================================================================
Логика сетапа (3 условия одновременно):

1) KINETIC SPIKE — аналог Kinetic Momentum Vectors [BigBeluga]
   kinetic = close - SMA(close, 100)            (Trend Baseline Length = 100)
   norm    = (kinetic - min) / (max - min)       за Normalization Lookback = 100
   spike   = norm >= Spike Threshold (по умолчанию 1.0)
   => импульс обновляет экстремум за 100 баров

2) SQUEEZE — сжатие волатильности
   Полосы Боллинджера (20, 2.0) внутри канала Кельтнера (20, 1.5)
   и/или BB-ширина в нижних X% за lookback (настраивается)

3) EXTREME — цена обновляет high за N баров (фильтр направления)

Антибан-архитектура:
- Реальное время ТОЛЬКО через WebSocket (1 соединение на все пары).
- REST используется один раз при старте для подгрузки истории,
  с лимитером по весам (вес klines=2, лимит безопасный).
- При ответе 429/418 — пауза и экспоненциальный backoff, никакого долбёжки.
- Повторные запросы истории не делаются чаще REFRESH_MIN.

Зависимости: pip install websockets aiohttp numpy
Запуск: python binance_squeeze_scanner.py
"""

import asyncio
import json
import os
import time
import math
import logging
from collections import defaultdict, deque

import numpy as np
import aiohttp
import websockets

# ============================ НАСТРОЙКИ ============================

CONFIG = {
    # --- рынок ---
    "quote": "USDT",            # сканируем USDT-M перпетуалы (фьючерсы)
    "min_volume_usdt": 0,       # 0 = все пары; можно поставить, например, 500_000
    "timeframes": ["1h", "2h"], # таймфреймы сканирования

    # --- Kinetic Momentum Vector (по настройкам BigBeluga) ---
    "baseline_len": 100,        # Trend Baseline Length
    "norm_lookback": 100,       # Normalization Lookback
    "spike_threshold": 1.0,     # Spike Threshold (0-1)

    # --- Squeeze ---
    "bb_len": 20, "bb_mult": 2.0,
    "kc_len": 20, "kc_mult": 1.5,
    "use_bbw_percentile": True, # доп.условие: ширина BB в нижнем перцентиле
    "bbw_lookback": 100,
    "bbw_percentile": 25,       # ширина BB ниже 25-го перцентиля за 100 баров
    "squeeze_lookback_bars": 5, # сжатие допускается в любой из 5 баров до спайка

    # --- экстремум цены ---
    "extreme_lookback": 20,     # high обновил максимум за 20 баров
    "min_move_atr": 1.0,        # спайк-свеча: диапазон >= 1.0*ATR(14) и тело вверх

    # --- сигналы ---
    "cooldown_hours": 6,        # не спамить по одной паре чаще, чем раз в 6ч
    # Токены лучше задавать переменными окружения (Render → Environment),
    # чтобы не светить их в коде на GitHub:
    "telegram_token": os.environ.get("TELEGRAM_TOKEN", ""),
    "telegram_chat_id": os.environ.get("TELEGRAM_CHAT_ID", ""),

    # --- антибан ---
    "rest_weight_per_min": 2000,    # наш потолок (лимит фьючерсов 2400/мин, держим запас)
    "history_bars": 120,            # сколько баров истории тянуть (>= norm_lookback+bb_len)
    "history_refresh_min": 60,      # переподгрузка истории не чаще (на случай реконнекта)
    "request_delay": 0.12,          # пауза между REST-запросами истории (сек)
}

BASE_REST = "https://fapi.binance.com"          # USDⓈ-M фьючерсы
BASE_WS = "wss://fstream.binance.com/stream?streams="

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
log = logging.getLogger("scanner")

# ========================= ИНДИКАТОРЫ (math) =========================

def sma(x, n):
    if len(x) < n:
        return None
    return float(np.convolve(x, np.ones(n) / n, mode="valid")[-1])

def atr(high, low, close, n):
    if len(close) < n + 1:
        return None
    trs = []
    for i in range(-n, 0):
        h, l, pc = high[i], low[i], close[i - 1]
        trs.append(max(h - l, abs(h - pc), abs(l - pc)))
    return float(np.mean(trs))

def kinetic_norm(close):
    """Аналог KMV: отклонение от baseline, нормализованное в 0..1 за lookback."""
    bl = CONFIG["baseline_len"]
    lb = CONFIG["norm_lookback"]
    if len(close) < bl + lb:
        return None
    base_series = np.convolve(close, np.ones(bl) / bl, mode="valid")
    kinetic = close[len(close) - len(base_series):] - base_series
    window = kinetic[-lb:]
    mn, mx = window.min(), window.max()
    if mx - mn == 0:
        return 0.0
    return float((kinetic[-1] - mn) / (mx - mn))

def squeeze_state(high, low, close):
    """True если BB внутри KC и (опц.) BB-ширина в нижнем перцентиле."""
    bl, bm = CONFIG["bb_len"], CONFIG["bb_mult"]
    kl, km = CONFIG["kc_len"], CONFIG["kc_mult"]
    if len(close) < max(bl, kl) + 1:
        return False, None
    window = close[-bl:]
    mean = window.mean()
    std = window.std(ddof=0)
    bb_up, bb_lo = mean + bm * std, mean - bm * std
    a = atr(high, low, close, kl)
    if a is None:
        return False, None
    mid = sma(close, kl)
    kc_up, kc_lo = mid + km * a, mid - km * a
    inside = bb_up < kc_up and bb_lo > kc_lo

    bbw_ok = True
    if CONFIG["use_bbw_percentile"]:
        lb = CONFIG["bbw_lookback"]
        if len(close) < bl + lb:
            bbw_ok = inside
        else:
            widths = []
            for i in range(-lb, 0):
                w = close[i - bl + 1:i + 1]
                if len(w) < bl:
                    continue
                m = w.mean()
                s = w.std(ddof=0)
                widths.append(4 * bm * s / m if m else 0)  # относительная ширина
            cur = 4 * bm * std / mean if mean else 0
            thr = np.percentile(widths, CONFIG["bbw_percentile"]) if widths else 0
            bbw_ok = cur <= thr
    # Сжатие = BB внутри KC ИЛИ ширина BB в нижнем перцентиле (мягкое условие)
    return bool(inside or bbw_ok), {
        "bb_inside_kc": inside,
        "bb_width_pct": float(4 * bm * std / mean * 100) if mean else None,
    }

def squeeze_recent(high, low, close, bars_back):
    """Сжатие было в одном из последних `bars_back` баров (не обязательно на спайке)."""
    for off in range(1, bars_back + 1):
        if len(close) <= off:
            break
        ok, info = squeeze_state(high[:-off], low[:-off], close[:-off])
        if ok:
            return True, info
    return False, None

def new_extreme(high, lookback):
    if len(high) < lookback + 1:
        return False
    return high[-1] >= max(high[-lookback - 1:-1])

def strong_move(open_, high, low, close):
    """Спайк-свеча должна быть настоящим движением, а не шумом:
    бычья свеча и диапазон >= min_move_atr * ATR(14)."""
    a = atr(high, low, close, 14)
    if a is None or a == 0:
        return False
    rng = high[-1] - low[-1]
    return close[-1] > open_[-1] and rng >= CONFIG["min_move_atr"] * a

# ========================= ХРАНИЛИЩЕ БАРОВ =========================

class SymbolState:
    def __init__(self, bars):
        # bars: {tf: deque of dict(o,h,l,c,v, closed)}
        self.bars = bars
        self.last_signal = 0.0

STATE = {}  # symbol -> SymbolState

# ========================= АНТИБАН: ВЕСОВОЙ ЛИМИТЕР =========================

class WeightLimiter:
    """Не даём превысить CONFIG['rest_weight_per_min'] веса в минуту."""
    def __init__(self, per_min):
        self.per_min = per_min
        self.hits = deque()

    async def acquire(self, weight):
        while True:
            now = time.monotonic()
            while self.hits and now - self.hits[0][0] > 60:
                self.hits.popleft()
            used = sum(w for _, w in self.hits)
            if used + weight <= self.per_min:
                self.hits.append((now, weight))
                return
            wait = 60 - (now - self.hits[0][0]) + 0.5
            log.warning("Весовой лимит: пауза %.1f сек (антибан)", wait)
            await asyncio.sleep(wait)

LIMITER = WeightLimiter(CONFIG["rest_weight_per_min"])
BACKOFF = {"until": 0.0}

async def rest_get(session, path, params, weight, retries=4):
    """REST с обработкой 429/418 и экспоненциальным backoff."""
    for attempt in range(retries):
        now = time.time()
        if now < BACKOFF["until"]:
            await asyncio.sleep(BACKOFF["until"] - now)
        await LIMITER.acquire(weight)
        try:
            async with session.get(BASE_REST + path, params=params, timeout=15) as r:
                if r.status in (429, 418):
                    pause = min(2 ** attempt * 30, 300)
                    log.warning("HTTP %s от Binance — стоп на %s сек (антибан)", r.status, pause)
                    BACKOFF["until"] = time.time() + pause
                    await asyncio.sleep(pause)
                    continue
                r.raise_for_status()
                return await r.json()
        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            log.warning("REST ошибка %s, попытка %d", e, attempt + 1)
            await asyncio.sleep(2 ** attempt * 2)
    return None

# ========================= ЗАГРУЗКА ИСТОРИИ =========================

async def load_symbols(session):
    data = await rest_get(session, "/fapi/v1/exchangeInfo", None, weight=1)
    if not data:
        return []
    syms = [s["symbol"] for s in data["symbols"]
            if s["status"] == "TRADING"
            and s["quoteAsset"] == CONFIG["quote"]
            and s.get("contractType") == "PERPETUAL"]
    if CONFIG["min_volume_usdt"] > 0:
        tickers = await rest_get(session, "/fapi/v1/ticker/24hr", None, weight=40)
        if tickers:
            vol = {t["symbol"]: float(t["quoteVolume"]) for t in tickers}
            syms = [s for s in syms if vol.get(s, 0) >= CONFIG["min_volume_usdt"]]
    log.info("Пар для сканирования: %d", len(syms))
    return sorted(syms)

async def load_history(session, symbol):
    """Подгружаем по history_bars закрытых баров на каждый ТФ. Вес klines = 2."""
    bars = {}
    for tf in CONFIG["timeframes"]:
        data = await rest_get(
            session, "/fapi/v1/klines",
            {"symbol": symbol, "interval": tf, "limit": CONFIG["history_bars"]},
            weight=2,
        )
        await asyncio.sleep(CONFIG["request_delay"])  # мягкий pacing
        if not data:
            return None
        dq = deque(maxlen=300)
        for k in data[:-1]:  # последний бар не закрыт — пропускаем
            dq.append({
                "t": k[0],
                "o": float(k[1]), "h": float(k[2]),
                "l": float(k[3]), "c": float(k[4]),
                "closed": True,
            })
        bars[tf] = dq
    return bars

async def bootstrap():
    timeout = aiohttp.ClientTimeout(total=30)
    headers = {"User-Agent": "squeeze-scanner/1.0"}
    async with aiohttp.ClientSession(timeout=timeout, headers=headers) as session:
        symbols = await load_symbols(session)
        if not symbols:
            log.error("Не удалось получить список пар")
            return []
        ok = []
        for i, sym in enumerate(symbols):
            bars = await load_history(session, sym)
            if bars:
                STATE[sym] = SymbolState(bars)
                ok.append(sym)
            if (i + 1) % 25 == 0:
                log.info("История загружена: %d/%d", i + 1, len(symbols))
        log.info("Готово: %d пар с историей. Подключаю WebSocket...", len(ok))
        return ok

# ========================= ОБНОВЛЕНИЕ И СИГНАЛЫ =========================

def on_kline(symbol, tf, k):
    st = STATE.get(symbol)
    if st is None:
        return
    dq = st.bars.get(tf)
    if dq is None:
        return
    bar = {
        "t": k["t"], "o": float(k["o"]), "h": float(k["h"]),
        "l": float(k["l"]), "c": float(k["c"]), "closed": k["x"],
    }
    if dq and dq[-1]["t"] == k["t"]:
        dq[-1] = bar
    else:
        dq.append(bar)
    # Считаем сетап ТОЛЬКО на закрытом баре — без ложных срабатываний
    if k["x"]:
        check_signal(symbol, tf, st)

def check_signal(symbol, tf, st):
    dq = st.bars[tf]
    closed = [b for b in dq if b["closed"]]
    if len(closed) < CONFIG["norm_lookback"] + 5:
        return
    o = np.array([b["o"] for b in closed])
    h = np.array([b["h"] for b in closed])
    l = np.array([b["l"] for b in closed])
    c = np.array([b["c"] for b in closed])

    kn = kinetic_norm(c)
    if kn is None:
        return
    spike = kn >= CONFIG["spike_threshold"] - 1e-9

    sq, sq_info = squeeze_recent(h, l, c, CONFIG["squeeze_lookback_bars"])
    extreme = new_extreme(h, CONFIG["extreme_lookback"])
    strong = strong_move(o, h, l, c)

    if spike and sq and extreme and strong:
        now = time.time()
        if now - st.last_signal < CONFIG["cooldown_hours"] * 3600:
            return
        st.last_signal = now
        msg = (
            f"🚀 SETUP {symbol} [{tf}]\n"
            f"Цена: {c[-1]:.8g}\n"
            f"Kinetic norm: {kn:.2f} (экстремум за {CONFIG['norm_lookback']} баров)\n"
            f"Squeeze: сжатие за последние {CONFIG['squeeze_lookback_bars']} баров"
            f" (BB in KC: {sq_info['bb_inside_kc']}, ширина BB {sq_info['bb_width_pct']:.2f}%)\n"
            f"High обновлён за {CONFIG['extreme_lookback']} баров"
        )
        log.info("СИГНАЛ: %s %s kn=%.2f", symbol, tf, kn)
        asyncio.create_task(send_telegram(msg))

async def send_telegram(text):
    token, chat = CONFIG["telegram_token"], CONFIG["telegram_chat_id"]
    if not token or not chat:
        print("\n" + text + "\n")
        return
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    try:
        async with aiohttp.ClientSession() as s:
            await s.post(url, json={"chat_id": chat, "text": text}, timeout=10)
    except Exception as e:
        log.warning("Telegram ошибка: %s", e)
        print(text)

# ========================= WEBSOCKET =========================

async def ws_loop(symbols):
    """Один комбинированный поток: symbol@kline_1h / symbol@kline_2h для всех пар."""
    streams = []
    for s in symbols:
        for tf in CONFIG["timeframes"]:
            streams.append(f"{s.lower()}@kline_{tf}")
    # Binance: до 1024 потоков на соединение — делим на чанки
    chunk_size = 900
    chunks = [streams[i:i + chunk_size] for i in range(0, len(streams), chunk_size)]
    await asyncio.gather(*(ws_worker(c) for c in chunks))

async def ws_worker(streams):
    url = BASE_WS + "/".join(streams)
    while True:
        try:
            async with websockets.connect(url, ping_interval=180, ping_timeout=30,
                                          max_queue=2000) as ws:
                log.info("WS подключён (%d потоков)", len(streams))
                async for raw in ws:
                    try:
                        msg = json.loads(raw)
                        data = msg.get("data", {})
                        if data.get("e") == "kline":
                            k = data["k"]
                            on_kline(data["s"], k["i"], k)
                    except Exception as e:
                        log.warning("WS msg ошибка: %s", e)
        except Exception as e:
            log.warning("WS отвалился: %s — реконнект через 10 сек", e)
            await asyncio.sleep(10)

# ========================= HEALTH-CHECK ДЛЯ RENDER =========================

async def health_server():
    """Render (Web Service) требует открытый порт — отдаём 200 OK на /.
    Заодно можно пинговать UptimeRobot'ом, чтобы free-инстанс не засыпал."""
    from aiohttp import web

    async def ok(request):
        return web.Response(text=f"scanner alive, pairs: {len(STATE)}")

    app = web.Application()
    app.router.add_get("/", ok)
    runner = web.AppRunner(app)
    await runner.setup()
    port = int(os.environ.get("PORT", 10000))
    site = web.TCPSite(runner, "0.0.0.0", port)
    await site.start()
    log.info("Health-check порт %d открыт", port)

# ========================= MAIN =========================

async def main():
    symbols = await bootstrap()
    if not symbols:
        return
    await asyncio.gather(
        health_server(),
        ws_loop(symbols),
    )

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log.info("Остановлено пользователем")
