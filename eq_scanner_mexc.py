#!/usr/bin/env python3
"""
MEXC Scanner - многоуровневые сигналы по EQ 4H + ликвидности M/W.

ТРИ ЛИНИИ (считаются по ЗАКРЫТЫМ барам — без перерисовки):
  1. EQ 4H          — середина между последним pivot high и pivot low на 4H
  2. Ликвидность M  — экстремум за LOOKBACK_M закрытых месяцев
  3. Ликвидность W  — экстремум за LOOKBACK_W закрытых недель

Для лонга берутся верхние уровни (highs), для шорта — нижние (lows).
EQ 4H общая для обоих направлений.

ТИРЫ (лонг; шорт зеркально):
  Условие входа: цена выше EQ 4H И выше хотя бы одной ликвидности
  ●   обычный       — пробита вторая линия, 2 из 3 под ценой
  ●●  сильный       — пробита третья линия, 3 из 3 под ценой
  ●●● очень сильный — пробито сопротивление сжатия R240 при 3 из 3 под ценой

R240 — уровень сжатия на 4H: два последних pivot high сошлись в пределах
CONV_MULT × ATR на момент подтверждения пивота. Для шорта зеркально S240.

Все линии считаются из ОДНОГО запроса 4H-свечей: месячные и недельные
экстремумы агрегируются по календарным границам UTC.
"""

import asyncio
import json
import logging
import os
import signal
import time
import zlib
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta, timezone

import aiohttp
import aiohttp.web

# ============================================================
# НАСТРОЙКИ
# ============================================================

TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")

# --- EQ 4H (совпадает с Зоной 1 индикатора EQ Multi-TF) ---
EQ_INTERVAL = "Hour4"
PIVOT_LEFT = int(os.getenv("PIVOT_LEFT", "17"))
PIVOT_RIGHT = int(os.getenv("PIVOT_RIGHT", "2"))
ATR_LEN = int(os.getenv("ATR_LEN", "14"))
CONV_MULT = float(os.getenv("CONV_MULT", "0.5"))      # порог сжатия × ATR

# --- Ликвидность (совпадает с MTF Liquidity) ---
LOOKBACK_M = int(os.getenv("LOOKBACK_M", "2"))        # месяцев
LOOKBACK_W = int(os.getenv("LOOKBACK_W", "4"))        # недель

# --- Направления и порог ---
ENABLE_LONG = os.getenv("ENABLE_LONG", "1") == "1"
ENABLE_SHORT = os.getenv("ENABLE_SHORT", "1") == "1"
MIN_TIER = int(os.getenv("MIN_TIER", "1"))            # 1 / 2 / 3

# --- Фильтры шума ---
DEADBAND_PCT = float(os.getenv("DEADBAND_PCT", "0.05"))
SYMBOL_COOLDOWN_MIN = int(os.getenv("SYMBOL_COOLDOWN_MIN", "30"))

# --- Исключение акций / TradFi ---
# MEXC называет их вразнобой (AAPLSTOCK_USDT, NVIDIA_USDT, TESLA_USDT),
# поэтому фильтруем по тегам сектора conceptPlate, а не по имени.
EXCLUDE_TRADFI = os.getenv("EXCLUDE_TRADFI", "1") == "1"
EXCLUDE_TAGS = [t.strip().lower() for t in os.getenv(
    "EXCLUDE_TAGS",
    "tradfi,stock,commodities,metals,forex,indices,etf"
).split(",") if t.strip()]
# Ручные списки: подстроки в имени символа, через запятую
EXCLUDE_SYMBOLS = [s.strip().upper() for s in
                   os.getenv("EXCLUDE_SYMBOLS", "").split(",") if s.strip()]
KEEP_SYMBOLS = [s.strip().upper() for s in
                os.getenv("KEEP_SYMBOLS", "").split(",") if s.strip()]

# --- Прочее ---
SYMBOLS_TTL_DAYS = int(os.getenv("SYMBOLS_TTL_DAYS", "15"))
# Лимит MEXC на klines: 20 запросов / 2 сек = 10/сек.
# 3 потока с паузой 0.5с дают ~6/сек — 60% лимита, с запасом от 429.
REST_CONCURRENCY = int(os.getenv("REST_CONCURRENCY", "3"))
REST_DELAY = float(os.getenv("REST_DELAY", "0.5"))
BLOCK_PENALTY_SEC = int(os.getenv("BLOCK_PENALTY_SEC", "600"))

FEED_MODE = os.getenv("FEED_MODE", "auto").lower()    # auto | ws | rest
REST_POLL_SEC = int(os.getenv("REST_POLL_SEC", "10"))

BATCH_WINDOW_SEC = 5
MAX_BATCH = 20

_here = os.path.dirname(os.path.abspath(__file__)) if "__file__" in globals() else os.getcwd()
STATE_DIR = os.getenv("STATE_DIR", _here)
SYMBOLS_CACHE = os.path.join(STATE_DIR, "mexc_symbols_cache.json")
STATE_FILE = os.path.join(STATE_DIR, "mexc_eq_state.json")

ENABLE_HTTP = os.getenv("ENABLE_HTTP", "1") == "1"
HTTP_PORT = int(os.getenv("PORT", "10000"))

REST_BASE = "https://contract.mexc.com"
WS_URL = "wss://contract.mexc.com/edge"
BAR_SEC = 14400                                        # 4H

UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36")
REST_HEADERS = {
    "User-Agent": UA,
    "Accept": "application/json",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://futures.mexc.com/",
    "Origin": "https://futures.mexc.com",
}
WS_HEADERS = {"User-Agent": UA, "Origin": "https://futures.mexc.com"}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("mexc")

ban_flag = {"until": 0.0}
feed = {"last_msg": 0.0, "source": "none", "ws_fails": 0}
stats = {"long": 0, "short": 0, "t1": 0, "t2": 0, "t3": 0}


async def log_block(r, where: str):
    """Печатает, кто именно отказал — Cloudflare или сама биржа."""
    try:
        body = (await r.text())[:300].replace("\n", " ")
    except Exception:
        body = "(тело не прочиталось)"
    log.error("%s: HTTP %s | Server=%s CF-RAY=%s | %s", where, r.status,
              r.headers.get("Server", "?"), r.headers.get("CF-RAY", "-"), body)


# ============================================================
# СОСТОЯНИЕ
# ============================================================

@dataclass
class Levels:
    eq: float = 0.0
    m_high: float = 0.0
    m_low: float = 0.0
    w_high: float = 0.0
    w_low: float = 0.0
    r240: float = 0.0        # 0 = сжатия нет
    s240: float = 0.0


@dataclass
class SymState:
    lv: Levels = field(default_factory=Levels)
    rank_long: int = -1      # -1 = база не установлена, алерты не шлём
    rank_short: int = -1
    over_r240: int = -1      # -1 нет уровня, 0 под ним, 1 над ним
    under_s240: int = -1
    ts_long: float = 0.0
    ts_short: float = 0.0
    tier_long: int = 0
    tier_short: int = 0


levels: dict[str, SymState] = {}
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


def load_state() -> dict[str, SymState]:
    try:
        with open(STATE_FILE) as f:
            raw = json.load(f)
        out = {}
        for sym, d in raw.get("levels", {}).items():
            out[sym] = SymState(
                lv=Levels(**d.get("lv", {})),
                rank_long=int(d.get("rank_long", -1)),
                rank_short=int(d.get("rank_short", -1)),
                over_r240=int(d.get("over_r240", -1)),
                under_s240=int(d.get("under_s240", -1)),
                ts_long=float(d.get("ts_long", 0.0)),
                ts_short=float(d.get("ts_short", 0.0)),
                tier_long=int(d.get("tier_long", 0)),
                tier_short=int(d.get("tier_short", 0)),
            )
        log.info("Загружено состояние: %s символов", len(out))
        return out
    except FileNotFoundError:
        return {}
    except Exception as e:
        log.warning("Не удалось загрузить состояние: %s", e)
        return {}


# ============================================================
# РАСЧЁТ ЛИНИЙ
# ============================================================

def find_pivots(values, left: int, right: int, is_high: bool):
    """Все пивоты как (индекс, значение). Строгое сравнение — как в Pine."""
    out = []
    n = len(values)
    for i in range(left, n - right):
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
            out.append((i, v))
    return out


def atr_series(highs, lows, closes, length):
    """ATR по Уайлдеру (RMA) — как ta.atr в Pine."""
    n = len(closes)
    if n < 2 or n < length:
        return [0.0] * n
    tr = [highs[0] - lows[0]]
    for i in range(1, n):
        tr.append(max(highs[i] - lows[i],
                      abs(highs[i] - closes[i - 1]),
                      abs(lows[i] - closes[i - 1])))
    out = [0.0] * n
    prev = sum(tr[:length]) / length
    out[length - 1] = prev
    for i in range(length, n):
        prev = (prev * (length - 1) + tr[i]) / length
        out[i] = prev
    return out


def month_key(ts: int):
    d = datetime.fromtimestamp(ts, timezone.utc)
    return (d.year, d.month)


def week_key(ts: int):
    iso = datetime.fromtimestamp(ts, timezone.utc).isocalendar()
    return (iso[0], iso[1])


def period_extremes(times, highs, lows, key_fn, lookback):
    """
    Группирует бары по календарному периоду и возвращает (макс, мин)
    по последним `lookback` ЗАКРЫТЫМ периодам. Текущий период отбрасывается.
    Если закрытых периодов меньше — берём сколько есть (новые монеты).
    """
    buckets = {}
    order = []
    for t, h, l in zip(times, highs, lows):
        k = key_fn(t)
        b = buckets.get(k)
        if b is None:
            buckets[k] = [h, l]
            order.append(k)
        else:
            if h > b[0]:
                b[0] = h
            if l < b[1]:
                b[1] = l

    closed = order[:-1]
    if not closed:
        return None, None
    closed = closed[-lookback:]
    return (max(buckets[k][0] for k in closed),
            min(buckets[k][1] for k in closed))


def compute_levels(times, highs, lows, closes):
    """Все линии из одного массива 4H-свечей (текущий бар уже отброшен)."""
    if len(closes) < PIVOT_LEFT + PIVOT_RIGHT + ATR_LEN + 5:
        return None

    ph = find_pivots(highs, PIVOT_LEFT, PIVOT_RIGHT, True)
    pl = find_pivots(lows, PIVOT_LEFT, PIVOT_RIGHT, False)
    if not ph or not pl:
        return None

    res1_i, res1 = ph[-1]
    sup1_i, sup1 = pl[-1]
    eq = (res1 + sup1) / 2.0

    atr = atr_series(highs, lows, closes, ATR_LEN)

    # Сжатие: два последних пивота сошлись. ATR берём на момент подтверждения
    # пивота, а не текущий — иначе импульс раздувает порог задним числом.
    r240 = 0.0
    if len(ph) >= 2:
        a = atr[res1_i] if res1_i < len(atr) else 0.0
        if a > 0 and abs(res1 - ph[-2][1]) <= a * CONV_MULT:
            r240 = res1

    s240 = 0.0
    if len(pl) >= 2:
        a = atr[sup1_i] if sup1_i < len(atr) else 0.0
        if a > 0 and abs(sup1 - pl[-2][1]) <= a * CONV_MULT:
            s240 = sup1

    m_hi, m_lo = period_extremes(times, highs, lows, month_key, LOOKBACK_M)
    w_hi, w_lo = period_extremes(times, highs, lows, week_key, LOOKBACK_W)
    if m_hi is None or w_hi is None:
        return None

    return Levels(eq=eq, m_high=m_hi, m_low=m_lo,
                  w_high=w_hi, w_low=w_lo, r240=r240, s240=s240)


# ============================================================
# СПИСОК КОНТРАКТОВ
# ============================================================

def filter_signature() -> str:
    """Меняется при правке фильтра — тогда кэш считается устаревшим."""
    return "|".join([
        "1" if EXCLUDE_TRADFI else "0",
        ",".join(EXCLUDE_TAGS),
        ",".join(EXCLUDE_SYMBOLS),
        ",".join(KEEP_SYMBOLS),
    ])


def is_excluded(contract) -> str:
    """Возвращает причину исключения или пустую строку."""
    sym = str(contract.get("symbol", "")).upper()

    for pat in KEEP_SYMBOLS:
        if pat in sym:
            return ""

    for pat in EXCLUDE_SYMBOLS:
        if pat in sym:
            return f"список EXCLUDE_SYMBOLS ({pat})"

    if not EXCLUDE_TRADFI:
        return ""

    for plate in (contract.get("conceptPlate") or []):
        pl = str(plate).lower()
        for tag in EXCLUDE_TAGS:
            if tag in pl:
                return f"тег {plate}"

    # По документации: typeLabel 1 = TradFi, 2 = stock
    if contract.get("typeLabel") in (1, 2):
        return f"typeLabel={contract.get('typeLabel')}"

    return ""


def read_symbols_cache():
    try:
        with open(SYMBOLS_CACHE) as f:
            raw = json.load(f)
        if raw.get("filter") != filter_signature():
            log.info("Фильтр символов изменился — кэш сброшен")
            return [], 0.0
        return raw.get("symbols", []), float(raw.get("fetched_at", 0))
    except FileNotFoundError:
        return [], 0.0
    except Exception as e:
        log.warning("Кэш символов повреждён: %s", e)
        return [], 0.0


def write_symbols_cache(symbols):
    try:
        tmp = SYMBOLS_CACHE + ".tmp"
        with open(tmp, "w") as f:
            json.dump({"fetched_at": time.time(), "symbols": symbols,
                       "filter": filter_signature()}, f)
        os.replace(tmp, SYMBOLS_CACHE)
    except Exception as e:
        log.warning("Не удалось записать кэш символов: %s", e)


async def fetch_symbols_api(session):
    url = f"{REST_BASE}/api/v1/contract/detail"
    async with session.get(url, timeout=aiohttp.ClientTimeout(total=30)) as r:
        if r.status in (403, 451):
            await log_block(r, "contract/detail")
            ban_flag["until"] = time.time() + BLOCK_PENALTY_SEC
            raise RuntimeError(f"Доступ заблокирован ({r.status})")
        if r.status == 429:
            ban_flag["until"] = time.time() + 300
            raise RuntimeError("429 на contract/detail")
        r.raise_for_status()
        body = await r.json()

    if not body.get("success", False):
        raise RuntimeError(f"MEXC вернул ошибку: {body.get('code')}")

    out = []
    dropped = []
    for c in body.get("data", []):
        if not (c.get("futureType") == 1 and c.get("quoteCoin") == "USDT"
                and c.get("state") == 0):
            continue
        reason = is_excluded(c)
        if reason:
            dropped.append((c["symbol"], reason))
        else:
            out.append(c["symbol"])

    if dropped:
        log.info("Исключено не-крипто: %s шт", len(dropped))
        # печатаем весь список, чтобы можно было проверить и подправить теги
        for i in range(0, len(dropped), 8):
            log.info("  " + " | ".join(f"{s} [{r}]" for s, r in dropped[i:i + 8]))
    elif EXCLUDE_TRADFI:
        log.warning("Фильтр TradFi включён, но ничего не отсеяно — "
                    "проверь EXCLUDE_TAGS")

    return sorted(out)


async def load_symbols(session):
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

def bars_needed() -> int:
    per_day = 86400 // BAR_SEC
    need_m = (LOOKBACK_M + 1) * 31 * per_day
    need_w = (LOOKBACK_W + 1) * 7 * per_day
    need_p = PIVOT_LEFT + PIVOT_RIGHT + ATR_LEN + 60
    return min(max(need_m, need_w, need_p) + 30, 1900)


async def fetch_klines(session, sem, symbol, retries=3):
    """4H-свечи. MEXC отдаёт колонками: data.time[], data.high[], ..."""
    url = f"{REST_BASE}/api/v1/contract/kline/{symbol}"
    start = int(time.time()) - bars_needed() * BAR_SEC
    params = {"interval": EQ_INTERVAL, "start": start}

    async with sem:
        if time.time() < ban_flag["until"]:
            return None

        for attempt in range(retries):
            try:
                async with session.get(
                    url, params=params, timeout=aiohttp.ClientTimeout(total=25)
                ) as r:
                    if r.status in (403, 451):
                        await log_block(r, f"kline/{symbol}")
                        ban_flag["until"] = time.time() + BLOCK_PENALTY_SEC
                        return None
                    if r.status == 429:
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
            d = body.get("data") or {}
            t, h, l, c = d.get("time"), d.get("high"), d.get("low"), d.get("close")
            if not t or not h or not l or not c:
                return None
            n = min(len(t), len(h), len(l), len(c))
            if n < 3:
                return None
            # последний бар ещё формируется — отбрасываем
            return ([int(x) for x in t[:n - 1]],
                    [float(x) for x in h[:n - 1]],
                    [float(x) for x in l[:n - 1]],
                    [float(x) for x in c[:n - 1]])
    return None


async def refresh_levels(session):
    t0 = time.monotonic()
    if time.time() < ban_flag["until"]:
        log.warning("Доступ заблокирован (%ss), пересчёт пропущен",
                    int(ban_flag["until"] - time.time()))
        return

    symbols = await load_symbols(session)
    if not symbols:
        log.error("Список контрактов пуст")
        return

    log.info("Пересчёт: %s контрактов × %s баров 4H",
             len(symbols), bars_needed())
    sem = asyncio.Semaphore(REST_CONCURRENCY)
    results = await asyncio.gather(
        *(fetch_klines(session, sem, s) for s in symbols),
        return_exceptions=True,
    )

    new_levels: dict[str, SymState] = {}
    skipped = 0
    n_conv = 0

    for sym, res in zip(symbols, results):
        if isinstance(res, BaseException) or res is None:
            skipped += 1
            continue
        lv = compute_levels(*res)
        if lv is None or lv.eq <= 0:
            skipped += 1
            continue
        if lv.r240 > 0 or lv.s240 > 0:
            n_conv += 1

        st = SymState(lv=lv)
        old = levels.get(sym)
        if old is not None:
            st.ts_long = old.ts_long
            st.ts_short = old.ts_short
            st.tier_long = old.tier_long
            st.tier_short = old.tier_short
            # линии не изменились — сохраняем базу, иначе сбрасываем
            if (abs(old.lv.eq - lv.eq) < 1e-12
                    and abs(old.lv.m_high - lv.m_high) < 1e-12
                    and abs(old.lv.w_high - lv.w_high) < 1e-12):
                st.rank_long = old.rank_long
                st.over_r240 = old.over_r240
            if (abs(old.lv.eq - lv.eq) < 1e-12
                    and abs(old.lv.m_low - lv.m_low) < 1e-12
                    and abs(old.lv.w_low - lv.w_low) < 1e-12):
                st.rank_short = old.rank_short
                st.under_s240 = old.under_s240
        new_levels[sym] = st

    if not new_levels:
        log.error("Ни одного уровня не получено, старые оставлены")
        return

    levels.clear()
    levels.update(new_levels)
    save_state()
    log.info("Готово: %s уровней, со сжатием %s, пропущено %s, заняло %.0fs",
             len(levels), n_conv, skipped, time.monotonic() - t0)


# ============================================================
# ЛОГИКА СИГНАЛА
# ============================================================

def calc_rank(price, lines, prev, above: bool) -> int:
    """
    Сколько линий пройдено. above=True — считаем линии ПОД ценой (лонг),
    иначе НАД ценой (шорт). Гистерезис: ранг растёт только при выходе
    за мёртвую зону и падает тоже только за ней — у самой линии держится.
    """
    d = DEADBAND_PCT / 100.0
    if above:
        strict = sum(1 for x in lines if x > 0 and price > x * (1 + d))
        loose = sum(1 for x in lines if x > 0 and price > x * (1 - d))
    else:
        strict = sum(1 for x in lines if x > 0 and price < x * (1 - d))
        loose = sum(1 for x in lines if x > 0 and price < x * (1 + d))

    if prev < 0:
        return strict
    if strict > prev:
        return strict
    if loose < prev:
        return loose
    return prev


def check_symbol(symbol: str, price: float):
    st = levels.get(symbol)
    if st is None or st.lv.eq <= 0 or price <= 0:
        return

    lv = st.lv
    d = DEADBAND_PCT / 100.0
    now = time.time()
    cooldown = SYMBOL_COOLDOWN_MIN * 60

    # ---------- ЛОНГ ----------
    if ENABLE_LONG:
        old_rank = st.rank_long
        new_rank = calc_rank(price, [lv.eq, lv.m_high, lv.w_high], old_rank, True)

        if lv.r240 <= 0:
            over = -1
        else:
            over = 1 if price > lv.r240 * (1 + d) else 0
        old_over = st.over_r240

        # предусловие: выше EQ 4H и выше хотя бы одной ликвидности
        gate = price > lv.eq * (1 + d) and (
            price > lv.m_high * (1 + d) or price > lv.w_high * (1 + d))

        tier = 0
        if gate and old_rank >= 0:
            if new_rank == 3 and old_over == 0 and over == 1:
                tier = 3                      # пробой R240 при трёх линиях
            elif new_rank == 3 and old_rank < 3:
                tier = 3 if over == 1 else 2
            elif new_rank == 2 and old_rank < 2:
                tier = 1

        st.rank_long = new_rank
        st.over_r240 = over
        if new_rank <= 1:
            st.tier_long = 0

        if tier >= MIN_TIER and tier > 0:
            if now - st.ts_long >= cooldown or tier > st.tier_long:
                st.ts_long = now
                st.tier_long = tier
                alert_queue.put_nowait(("long", tier, symbol, price, lv, new_rank))
                stats["long"] += 1
                stats[f"t{tier}"] += 1
                log.info("%s ЛОНГ тир%s  %s  ранг %s/3",
                         symbol, tier, fmt_price(price), new_rank)

    # ---------- ШОРТ ----------
    if ENABLE_SHORT:
        old_rank = st.rank_short
        new_rank = calc_rank(price, [lv.eq, lv.m_low, lv.w_low], old_rank, False)

        if lv.s240 <= 0:
            under = -1
        else:
            under = 1 if price < lv.s240 * (1 - d) else 0
        old_under = st.under_s240

        gate = price < lv.eq * (1 - d) and (
            price < lv.m_low * (1 - d) or price < lv.w_low * (1 - d))

        tier = 0
        if gate and old_rank >= 0:
            if new_rank == 3 and old_under == 0 and under == 1:
                tier = 3
            elif new_rank == 3 and old_rank < 3:
                tier = 3 if under == 1 else 2
            elif new_rank == 2 and old_rank < 2:
                tier = 1

        st.rank_short = new_rank
        st.under_s240 = under
        if new_rank <= 1:
            st.tier_short = 0

        if tier >= MIN_TIER and tier > 0:
            if now - st.ts_short >= cooldown or tier > st.tier_short:
                st.ts_short = now
                st.tier_short = tier
                alert_queue.put_nowait(("short", tier, symbol, price, lv, new_rank))
                stats["short"] += 1
                stats[f"t{tier}"] += 1
                log.info("%s ШОРТ тир%s  %s  ранг %s/3",
                         symbol, tier, fmt_price(price), new_rank)


# ============================================================
# TELEGRAM
# ============================================================

def fmt_price(p: float) -> str:
    if p >= 100:
        return f"{p:.2f}"
    if p >= 1:
        return f"{p:.4f}"
    return f"{p:.8f}".rstrip("0")


def tv_link(symbol: str) -> str:
    return f"https://www.tradingview.com/chart/?symbol=MEXC%3A{symbol.replace('_','')}.P"


TIER_NAME = {1: "обычный", 2: "сильный", 3: "очень сильный"}
TIER_MARK = {1: "●", 2: "●●", 3: "●●●"}


def format_batch(items) -> str:
    out = ["<b>MEXC · EQ 4H + ликвидность</b>", ""]
    for direction, tier, sym, price, lv, rank in items:
        is_long = direction == "long"
        arrow = "▲ ЛОНГ" if is_long else "▼ ШОРТ"
        l_m = lv.m_high if is_long else lv.m_low
        l_w = lv.w_high if is_long else lv.w_low
        r_lvl = lv.r240 if is_long else lv.s240

        out.append(f'{TIER_MARK[tier]} <a href="{tv_link(sym)}"><b>{sym}</b></a>'
                   f"  {arrow} · {TIER_NAME[tier]}")
        out.append(f"   {fmt_price(price)}  ·  {rank}/3 линий пройдено")
        out.append(f"   EQ4H {fmt_price(lv.eq)} · W {fmt_price(l_w)}"
                   f" · M {fmt_price(l_m)}")
        if tier == 3 and r_lvl > 0:
            out.append(f"   {'R240' if is_long else 'S240'} "
                       f"{fmt_price(r_lvl)} пробит")
        out.append("")
    return "\n".join(out).rstrip()


async def send_telegram(session, text: str):
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {"chat_id": TELEGRAM_CHAT_ID, "text": text,
               "parse_mode": "HTML", "disable_web_page_preview": True}
    for attempt in range(3):
        try:
            async with session.post(
                url, json=payload, timeout=aiohttp.ClientTimeout(total=20)
            ) as r:
                if r.status == 429:
                    body = await r.json()
                    await asyncio.sleep(
                        body.get("parameters", {}).get("retry_after", 5))
                    continue
                if r.status != 200:
                    log.warning("Telegram %s: %s", r.status, await r.text())
                return
        except asyncio.CancelledError:
            raise
        except Exception as e:
            log.warning("Telegram ошибка: %s", e)
            await asyncio.sleep(2 * (attempt + 1))


async def telegram_worker(session):
    while True:
        first = await alert_queue.get()
        batch = [first]
        deadline = time.monotonic() + BATCH_WINDOW_SEC
        while len(batch) < MAX_BATCH:
            rem = deadline - time.monotonic()
            if rem <= 0:
                break
            try:
                batch.append(await asyncio.wait_for(alert_queue.get(), timeout=rem))
            except asyncio.TimeoutError:
                break
        batch.sort(key=lambda x: -x[1])          # сильные наверх
        await send_telegram(session, format_batch(batch))
        await asyncio.sleep(1.0)


# ============================================================
# ИСТОЧНИКИ ЦЕН
# ============================================================

async def fetch_all_tickers(session):
    """Один запрос — цены всех контрактов. Лимит MEXC: 10 / 2 сек."""
    url = f"{REST_BASE}/api/v1/contract/ticker"
    try:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=20)) as r:
            if r.status in (403, 451):
                await log_block(r, "contract/ticker")
                ban_flag["until"] = time.time() + BLOCK_PENALTY_SEC
                return None
            if r.status == 429:
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


async def rest_poll_loop(session):
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
            sym, price = t.get("symbol"), t.get("lastPrice")
            if sym and price is not None:
                try:
                    check_symbol(sym, float(price))
                except Exception as e:
                    log.debug("check %s: %s", sym, e)


def handle_ws_payload(raw: str):
    try:
        msg = json.loads(raw)
    except Exception:
        return
    if not isinstance(msg, dict) or msg.get("channel") != "push.tickers":
        return
    feed["last_msg"] = time.time()
    if feed["source"] != "ws":
        log.info("Источник цен: WebSocket")
        feed["source"] = "ws"
    data = msg.get("data")
    if not isinstance(data, list):
        return
    for t in data:
        sym, price = t.get("symbol"), t.get("lastPrice")
        if sym and price is not None:
            try:
                check_symbol(sym, float(price))
            except Exception as e:
                log.debug("check %s: %s", sym, e)


async def ws_ping(ws):
    """MEXC требует прикладной ping, протокольного heartbeat мало."""
    while True:
        await asyncio.sleep(15)
        try:
            await ws.send_json({"method": "ping"})
        except Exception:
            return


async def ws_loop(session):
    if FEED_MODE == "rest":
        log.info("FEED_MODE=rest — WebSocket не используется")
        return
    backoff = 1
    while True:
        ping_task = None
        try:
            async with session.ws_connect(
                WS_URL, headers=WS_HEADERS,
                timeout=aiohttp.ClientTimeout(total=None)
            ) as ws:
                await ws.send_json({"method": "sub.tickers", "param": {},
                                    "gzip": False})
                log.info("WebSocket подключен, подписка отправлена")
                backoff = 1
                feed["ws_fails"] = 0
                ping_task = asyncio.create_task(ws_ping(ws))
                while True:
                    msg = await asyncio.wait_for(ws.receive(), timeout=60)
                    if msg.type == aiohttp.WSMsgType.TEXT:
                        handle_ws_payload(msg.data)
                    elif msg.type == aiohttp.WSMsgType.BINARY:
                        try:
                            handle_ws_payload(zlib.decompress(
                                msg.data, 16 + zlib.MAX_WBITS).decode())
                        except Exception:
                            pass
                    elif msg.type in (aiohttp.WSMsgType.CLOSED,
                                      aiohttp.WSMsgType.CLOSING,
                                      aiohttp.WSMsgType.ERROR):
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
                backoff = max(backoff, 300)
                if feed["ws_fails"] in (1, 5) or feed["ws_fails"] % 20 == 0:
                    log.warning("WebSocket отклонён (%s), попытка №%s. Цены по REST.",
                                txt[:60], feed["ws_fails"])
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

def seconds_until_next_4h_close() -> float:
    """4H-бары закрываются в 00/04/08/12/16/20 UTC."""
    now = datetime.now(timezone.utc)
    hour = (now.hour // 4 + 1) * 4
    base = now.replace(minute=1, second=0, microsecond=0)
    nxt = (base + timedelta(days=1)).replace(hour=0) if hour >= 24 \
        else base.replace(hour=hour)
    return max((nxt - now).total_seconds(), 60)


async def refresh_scheduler(session):
    while True:
        wait = seconds_until_next_4h_close()
        log.info("Следующий пересчёт через %.1f ч", wait / 3600)
        await asyncio.sleep(wait)
        await refresh_levels(session)


async def retry_until_ready(session):
    while True:
        await asyncio.sleep(max(BLOCK_PENALTY_SEC, 300))
        if levels:
            continue
        log.info("Повторная попытка пересчёта")
        await refresh_levels(session)
        if levels:
            await send_telegram(
                session, f"<b>Уровни получены</b>\nКонтрактов: {len(levels)}")


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
        n_r = sum(1 for s in levels.values() if s.lv.r240 > 0)
        n_s = sum(1 for s in levels.values() if s.lv.s240 > 0)
        body = (
            f"ok\nexchange: MEXC\n"
            f"levels: {len(levels)}\n"
            f"eq_tf: {EQ_INTERVAL}  pivot: {PIVOT_LEFT}/{PIVOT_RIGHT}\n"
            f"lookback: M{LOOKBACK_M} W{LOOKBACK_W}\n"
            f"compression: R240 {n_r} / S240 {n_s}\n"
            f"min_tier: {MIN_TIER}\n"
            f"exclude_tradfi: {EXCLUDE_TRADFI}  tags: {','.join(EXCLUDE_TAGS)}\n"
            f"feed: {feed['source']}  ws_fails: {feed['ws_fails']}\n"
            f"ban_left: {ban_left}s\n"
            f"alerts: long {stats['long']} short {stats['short']} | "
            f"t1 {stats['t1']} t2 {stats['t2']} t3 {stats['t3']}\n"
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
    missing = [k for k, v in (("TELEGRAM_TOKEN", TELEGRAM_TOKEN),
                              ("TELEGRAM_CHAT_ID", TELEGRAM_CHAT_ID)) if not v]
    if missing:
        log.error("Не заданы: %s", ", ".join(missing))
        seen = [k for k in os.environ
                if any(w in k.upper() for w in ("TELE", "CHAT", "TOKEN"))]
        log.error("Похожие переменные в окружении: %s", seen or "нет")
        return

    log.info("STATE_DIR: %s", STATE_DIR)
    log.info("EQ %s pivot %s/%s | ликв. M%s W%s | сжатие %s×ATR%s | "
             "лонг %s шорт %s | мин.тир %s",
             EQ_INTERVAL, PIVOT_LEFT, PIVOT_RIGHT, LOOKBACK_M, LOOKBACK_W,
             CONV_MULT, ATR_LEN, ENABLE_LONG, ENABLE_SHORT, MIN_TIER)
    log.info("Фильтр не-крипто: %s | теги: %s",
             "вкл" if EXCLUDE_TRADFI else "выкл", ", ".join(EXCLUDE_TAGS))
    levels.update(load_state())

    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            loop.add_signal_handler(sig, stop.set)
        except NotImplementedError:
            pass

    conn = aiohttp.TCPConnector(limit=20, ttl_dns_cache=300)
    async with aiohttp.ClientSession(connector=conn, headers=REST_HEADERS) as session:
        if ENABLE_HTTP:
            await keepalive_server()

        await refresh_levels(session)

        if levels:
            n_r = sum(1 for s in levels.values() if s.lv.r240 > 0)
            await send_telegram(
                session,
                f"<b>MEXC Scanner запущен</b>\n"
                f"Контрактов: {len(levels)}\n"
                f"EQ 4H pivot {PIVOT_LEFT}/{PIVOT_RIGHT}\n"
                f"Ликвидность: M{LOOKBACK_M} · W{LOOKBACK_W}\n"
                f"Со сжатием R240: {n_r}",
            )
        else:
            log.error("Уровни не рассчитаны. Повтор через %s мин.",
                      BLOCK_PENALTY_SEC // 60)

        tasks = [
            asyncio.create_task(ws_loop(session)),
            asyncio.create_task(rest_poll_loop(session)),
            asyncio.create_task(refresh_scheduler(session)),
            asyncio.create_task(telegram_worker(session)),
            asyncio.create_task(retry_until_ready(session)),
            asyncio.create_task(state_saver()),
        ]
        await stop.wait()
        log.info("Остановка, сохраняю состояние")
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
