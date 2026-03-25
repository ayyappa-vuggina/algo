#!/usr/bin/env python3
"""
╔══════════════════════════════════════════════════════════════════════════╗
║   KITE EMA 9/50 AUTO TRADING BOT  ─  NIFTY 50 & SENSEX Index F&O       ║
║                                                                          ║
║   Signal   :  EMA-9 crossover / crossunder EMA-50  on 1-min bars        ║
║   BUY CE   :  EMA-9 crosses ABOVE EMA-50  (Golden Cross)                ║
║   BUY PE   :  EMA-9 crosses BELOW EMA-50  (Death Cross)                 ║
║   Risk     :  10-point SL  |  TP: NIFTY 20, SENSEX 50                   ║
║   Size     :  10 % of available equity per trade                        ║
║   Scan     :  Every 1 second via KiteTicker WebSocket                   ║
║   Exit     :  Auto square-off at 15:20 IST — MIS product                ║
╚══════════════════════════════════════════════════════════════════════════╝

Dependencies:
    pip install kiteconnect pandas
"""

import sys
import time
import logging
import threading
from datetime import datetime, timedelta
from collections import deque
from typing import cast

try:
    from zoneinfo import ZoneInfo
    IST = ZoneInfo("Asia/Kolkata")
except ImportError:
    import pytz
    ZoneInfo = None
    IST = pytz.timezone("Asia/Kolkata")

import pandas as pd
from kiteconnect import KiteConnect, KiteTicker

# ───────────────────────────────────────────────────────────────────────────────
# CREDENTIALS  (keep these secret — never commit to version control)
# ───────────────────────────────────────────────────────────────────────────────
API_KEY      = "v7hyrrhlh85v4l1e"
ACCESS_TOKEN = "HezK50vACfk6ugYERPS51D9igNe3oHyE"

# ───────────────────────────────────────────────────────────────────────────────
# STRATEGY PARAMETERS
# ───────────────────────────────────────────────────────────────────────────────
STOP_LOSS_POINTS = 10   # fixed option premium stop-loss distance in points (all indices)
LOTS_PER_TRADE   = 1    # always trade exactly 1 lot

EMA_SHORT     = 9
EMA_LONG      = 50
WARMUP_BARS   = EMA_LONG + 10   # minimum completed 1-min candles before trading

# ───────────────────────────────────────────────────────────────────────────────
# MARKET TIMING  (IST)
# ───────────────────────────────────────────────────────────────────────────────
TRADE_START   = (9, 15)   # no entry before 09:15
NO_NEW_ENTRY  = (15, 0)   # no new entries after 15:00
SQUAREOFF_AT  = (15, 20)  # force exit all positions by 15:20
MARKET_CLOSE  = (15, 31)  # stop all processing after 15:31

# ───────────────────────────────────────────────────────────────────────────────
# INDEX CONFIGURATION
# ───────────────────────────────────────────────────────────────────────────────
INDICES = {
    "NIFTY": {
        "underlying_exchange": "NSE",
        "underlying_symbol":   "NIFTY 50",
        "fno_exchange":        "NFO",
        "instrument_name":     "NIFTY",   # as it appears in NFO instruments file
        "strike_gap":          50,
        "target_points":       20,
    },
    "SENSEX": {
        "underlying_exchange": "BSE",
        "underlying_symbol":   "SENSEX",
        "fno_exchange":        "BFO",
        "instrument_name":     "SENSEX",
        "strike_gap":          100,
        "target_points":       50,
    },
}

# ───────────────────────────────────────────────────────────────────────────────
# LOGGING
# ───────────────────────────────────────────────────────────────────────────────
_fmt = "%(asctime)s [%(levelname)-8s] %(message)s"
_dfmt = "%Y-%m-%d %H:%M:%S"
logging.basicConfig(
    level=logging.INFO,
    format=_fmt,
    datefmt=_dfmt,
    handlers=[
        logging.FileHandler("ema_bot.log", encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ],
)
log = logging.getLogger("EMA_BOT")


# ═══════════════════════════════════════════════════════════════════════════════
#  CANDLE BUILDER
# ═══════════════════════════════════════════════════════════════════════════════

class CandleBuilder:
    """
    Aggregates streaming price ticks into completed 1-minute OHLC candles.

    Thread-safe:  push_tick() and seed_candles() may be called from different
                  threads (WebSocket callback vs. startup seeding).
    """

    def __init__(self, max_candles: int = WARMUP_BARS + 20):
        self._candles: deque = deque(maxlen=max_candles)
        self._current: dict | None = None
        self._lock = threading.Lock()

    # ── public ────────────────────────────────────────────────────────────────

    def seed_candles(self, historical: list) -> None:
        """Seed the builder with a list of kite historical_data dicts."""
        with self._lock:
            for c in historical:
                ts = c["date"]
                if hasattr(ts, "astimezone"):
                    ts = ts.astimezone(IST).replace(second=0, microsecond=0)
                self._candles.append({
                    "time":  ts,
                    "open":  float(c["open"]),
                    "high":  float(c["high"]),
                    "low":   float(c["low"]),
                    "close": float(c["close"]),
                })

    def push_tick(self, price: float, ts: datetime) -> dict | None:
        """
        Feed a live price tick.
        Returns the **completed** candle dict when the minute rolls over,
        otherwise returns None (still building the current bar).
        """
        minute_floor = ts.replace(second=0, microsecond=0)
        with self._lock:
            current = self._current
            if current is None:
                self._current = _new_candle(price, minute_floor)
                return None
            current = cast(dict, current)

            if minute_floor > current["time"]:
                # ── minute has rolled: finalise old candle ──
                completed = dict(current)
                self._candles.append(completed)
                self._current = _new_candle(price, minute_floor)
                return completed

            # ── same minute: update OHLC ──
            current["high"]  = max(current["high"], price)
            current["low"]   = min(current["low"], price)
            current["close"] = price
            return None

    @property
    def completed_count(self) -> int:
        with self._lock:
            return len(self._candles)

    def get_closes(self) -> list:
        """Return a list of close prices for all completed candles (oldest first)."""
        with self._lock:
            return [c["close"] for c in self._candles]

    def is_ready(self) -> bool:
        """True once enough candles have been collected for EMA calculations."""
        return self.completed_count >= WARMUP_BARS


def _new_candle(price: float, ts: datetime) -> dict:
    return {"time": ts, "open": price, "high": price, "low": price, "close": price}


# ═══════════════════════════════════════════════════════════════════════════════
#  EMA & CROSSOVER LOGIC
# ═══════════════════════════════════════════════════════════════════════════════

def _ema(series: pd.Series, period: int) -> pd.Series:
    """EMA identical to TradingView ta.ema() — ewm(span, adjust=False)."""
    return series.ewm(span=period, adjust=False).mean()


def _crossover_signals(fast: pd.Series, slow: pd.Series):
    """
    Detect EMA crossover on the two most recent CLOSED candles.

    Pine Script exact match:
      ta.crossover  : fast[1] < slow[1]  AND  fast[0] > slow[0]  (bullish)
      ta.crossunder : fast[1] > slow[1]  AND  fast[0] < slow[0]  (bearish)

    Strict < / > ensures no false signal fires when EMAs are flat/equal.
    """
    if len(fast) < 2:
        return False, False
    f_prev, f_curr = float(fast.iloc[-2]), float(fast.iloc[-1])
    s_prev, s_curr = float(slow.iloc[-2]), float(slow.iloc[-1])
    long_sig  = (f_prev < s_prev) and (f_curr > s_curr)   # bullish crossover
    short_sig = (f_prev > s_prev) and (f_curr < s_curr)   # bearish crossunder
    return long_sig, short_sig


def detect_crossover(closes: list) -> str | None:
    """
    Wrapper used by _on_ticks.
    Converts close list → pandas Series, computes EMA 9/50,
    then returns 'BUY_CE', 'BUY_PE', or None.
    Never fires on partial (in-progress) bars — only on candle close data.
    """
    if len(closes) < WARMUP_BARS:
        return None

    series = pd.Series(closes, dtype=float)
    ema9   = _ema(series, EMA_SHORT)
    ema50  = _ema(series, EMA_LONG)

    long_sig, short_sig = _crossover_signals(ema9, ema50)

    if long_sig:
        return "BUY_CE"
    if short_sig:
        return "BUY_PE"

    return None


# ═══════════════════════════════════════════════════════════════════════════════
#  MAIN BOT CLASS
# ═══════════════════════════════════════════════════════════════════════════════

class EMABot:
    """
    Kite EMA 9/50 live trading bot for NIFTY & SENSEX Index F&O.
    """

    def __init__(self):
        self.kite = KiteConnect(api_key=API_KEY)
        self.kite.set_access_token(ACCESS_TOKEN)

        self._state_lock = threading.Lock()
        self._running    = False

        # Per-index runtime state (populated in _initialise_index)
        self.state: dict[str, dict] = {}

        # Instrument DataFrames (loaded once at startup)
        self._nfo_df: pd.DataFrame = pd.DataFrame()
        self._bfo_df: pd.DataFrame = pd.DataFrame()

    # ── INITIALISATION ────────────────────────────────────────────────────────

    def _load_fno_instruments(self) -> None:
        """Fetch and cache all NFO and BFO instrument definitions."""
        log.info("Loading NFO instruments …")
        try:
            self._nfo_df = pd.DataFrame(self.kite.instruments("NFO"))
            # expiry column: ensure datetime.date
            if not self._nfo_df.empty and "expiry" in self._nfo_df.columns:
                self._nfo_df["expiry"] = pd.to_datetime(self._nfo_df["expiry"]).dt.date
            log.info(f"  NFO: {len(self._nfo_df)} instruments loaded")
        except Exception as e:
            log.error(f"  NFO instruments load failed: {e}")

        log.info("Loading BFO instruments …")
        try:
            self._bfo_df = pd.DataFrame(self.kite.instruments("BFO"))
            if not self._bfo_df.empty and "expiry" in self._bfo_df.columns:
                self._bfo_df["expiry"] = pd.to_datetime(self._bfo_df["expiry"]).dt.date
            log.info(f"  BFO: {len(self._bfo_df)} instruments loaded")
        except Exception as e:
            log.warning(f"  BFO instruments load failed (SENSEX options may not work): {e}")

    def _get_underlying_token(self, exchange: str, symbol: str) -> int:
        """Fetch the instrument token for an index (e.g. 'NIFTY 50' on NSE)."""
        instruments = self.kite.instruments(exchange)
        for inst in instruments:
            if inst["tradingsymbol"] == symbol:
                return int(inst["instrument_token"])
        raise RuntimeError(f"Instrument '{symbol}' not found on {exchange}")

    def _initialise_index(self, name: str, cfg: dict) -> None:
        token = self._get_underlying_token(cfg["underlying_exchange"], cfg["underlying_symbol"])
        self.state[name] = {
            "name":        name,
            "cfg":         cfg,
            "token":       token,
            "builder":     CandleBuilder(),
            "position":    None,   # active position dict or None
            "last_signal": None,   # last signal that triggered a trade
        }
        log.info(f"[{name}] Underlying token = {token} ({cfg['underlying_symbol']})")

    def _seed_historical_candles(self, name: str) -> None:
        """
        Pre-load the last WARMUP_BARS completed 1-minute candles so that
        EMA calculations are valid from the very first live tick.
        """
        st    = self.state[name]
        token = st["token"]
        to_dt = datetime.now(IST).replace(second=0, microsecond=0)
        # Request extra bars to account for market gaps / holidays
        fr_dt = to_dt - timedelta(minutes=max(WARMUP_BARS * 2, 120))

        log.info(f"[{name}] Seeding historical 1-min candles …")
        try:
            candles = self.kite.historical_data(
                instrument_token=token,
                from_date=fr_dt.strftime("%Y-%m-%d %H:%M:%S"),
                to_date=to_dt.strftime("%Y-%m-%d %H:%M:%S"),
                interval="minute",
            )
            st["builder"].seed_candles(candles)
            log.info(f"[{name}] Seeded {len(candles)} candles  "
                     f"(builder has {st['builder'].completed_count} stored)")
        except Exception as e:
            log.error(f"[{name}] Historical seed failed: {e}")

    # ── OPTIONS LOOKUP ────────────────────────────────────────────────────────

    def _nearest_expiry(self, df: pd.DataFrame, name_prefix: str):
        """Return the nearest upcoming expiry date for the instrument name."""
        today  = datetime.now(IST).date()
        subset = df[df["name"] == name_prefix]
        if subset.empty:
            return None
        future_expiries = sorted({
            e for e in subset["expiry"].dropna() if e >= today
        })
        return future_expiries[0] if future_expiries else None

    def _get_atm_option(self, name: str, spot: float, opt_type: str) -> dict | None:
        """
        Locate the ATM CE or PE option instrument for NIFTY or SENSEX.

        Searches the cached instrument list for:
          • Matching name prefix (NIFTY / SENSEX)
          • Nearest future expiry
          • instrument_type == opt_type ('CE' or 'PE')
          • Strike closest to spot (ATM = round to strike_gap)

        Returns a dict with instrument details, or None if not found.
        """
        cfg    = self.state[name]["cfg"]
        prefix = cfg["instrument_name"]
        gap    = cfg["strike_gap"]
        df     = self._nfo_df if cfg["fno_exchange"] == "NFO" else self._bfo_df

        if df.empty:
            log.error(f"[{name}] Instrument DataFrame is empty — cannot find option")
            return None

        expiry = self._nearest_expiry(df, prefix)
        if expiry is None:
            log.error(f"[{name}] No upcoming expiry found for '{prefix}'")
            return None

        atm_strike = round(spot / gap) * gap

        # Try ATM ± up to 3 strikes
        for delta_mult in [0, 1, -1, 2, -2, 3, -3]:
            strike = atm_strike + delta_mult * gap
            mask = (
                (df["name"]            == prefix)
                & (df["instrument_type"] == opt_type)
                & (df["expiry"]          == expiry)
                & (df["strike"]          == float(strike))
            )
            hit = df[mask]
            if not hit.empty:
                row = hit.iloc[0]
                log.info(
                    f"[{name}] Found {opt_type}  {row['tradingsymbol']}  "
                    f"strike={strike}  expiry={expiry}  "
                    f"lot={row.get('lot_size', '?')}"
                )
                return row.to_dict()

        log.error(f"[{name}] ATM {opt_type} option not found near strike {atm_strike} (expiry {expiry})")
        return None

    # ── CAPITAL & POSITION SIZING ─────────────────────────────────────────────

    def _available_capital(self) -> float:
        try:
            m = self.kite.margins()
            return float(m["equity"]["available"]["live_balance"])
        except Exception as e:
            log.error(f"Margin fetch failed: {e}")
            return 0.0

    def _calc_quantity(self, lot_size: int) -> int:
        """Always trade exactly LOTS_PER_TRADE lot(s)."""
        return LOTS_PER_TRADE * lot_size

    # ── TRADE ENTRY ───────────────────────────────────────────────────────────

    def _enter_trade(self, name: str, signal: str, index_close: float) -> None:
        """
        Execute an entry order for the given signal.
          'BUY_CE' → buy ATM CE option
          'BUY_PE' → buy ATM PE option
        If a position already exists in the opposite direction, reverse it first.
        """
        opt_type = "CE" if signal == "BUY_CE" else "PE"
        st       = self.state[name]

        with self._state_lock:
            pos = st["position"]
            if pos is not None:
                if pos["opt_type"] == opt_type:
                    log.info(f"[{name}] Already holding {opt_type} — duplicate signal ignored")
                    return
                log.info(f"[{name}] Reversing {pos['opt_type']} → {opt_type}")
                self._exit_trade_unlocked(name, "REVERSE")

        # Find the ATM option instrument
        instr = self._get_atm_option(name, index_close, opt_type)
        if instr is None:
            return

        fno_exchange  = self.state[name]["cfg"]["fno_exchange"]
        tradingsymbol = instr["tradingsymbol"]
        token         = int(instr["instrument_token"])
        lot_size      = int(instr.get("lot_size", 1))

        # Fetch live premium (LTP) of the option
        try:
            q_key = f"{fno_exchange}:{tradingsymbol}"
            quote = self.kite.quote(q_key)
            ltp   = float(quote[q_key]["last_price"])
        except Exception as e:
            log.error(f"[{name}] Option LTP fetch failed for {tradingsymbol}: {e}")
            return

        if ltp <= 0:
            log.error(f"[{name}] Option LTP is 0 for {tradingsymbol} — trade skipped")
            return

        qty      = self._calc_quantity(lot_size)

        target_points = float(st["cfg"].get("target_points", 0))
        if STOP_LOSS_POINTS <= 0 or target_points <= 0:
            log.error(
                f"[{name}] Invalid risk config: STOP_LOSS_POINTS={STOP_LOSS_POINTS}, "
                f"target_points={target_points}"
            )
            return

        sl_price = round(ltp - STOP_LOSS_POINTS, 1)
        tp_price = round(ltp + target_points, 1)

        if sl_price <= 0 or tp_price <= ltp:
            log.error(
                f"[{name}] Invalid SL/TP computed for {tradingsymbol}: "
                f"ltp={ltp} SL={sl_price} TP={tp_price}"
            )
            return

        rr_ratio = round(target_points / STOP_LOSS_POINTS, 2)
        log.info(
            f"[{name}] ▶ ENTRY {opt_type}  sym={tradingsymbol}  "
            f"ltp={ltp}  qty={qty}  SL={sl_price} (-{STOP_LOSS_POINTS})  "
            f"TP={tp_price} (+{target_points})  RR=1:{rr_ratio}"
        )

        # ── Place market entry order ──────────────────────────────────────────
        try:
            entry_oid = self.kite.place_order(
                variety=self.kite.VARIETY_REGULAR,
                exchange=fno_exchange,
                tradingsymbol=tradingsymbol,
                transaction_type=self.kite.TRANSACTION_TYPE_BUY,
                quantity=qty,
                order_type=self.kite.ORDER_TYPE_MARKET,
                product=self.kite.PRODUCT_MIS,
                tag="EMA_ENTRY",
            )
            log.info(f"[{name}] Entry order_id = {entry_oid}")
        except Exception as e:
            log.error(f"[{name}] Entry order failed: {e}")
            return

        # ── Place SL order ────────────────────────────────────────────────────
        # BFO does NOT support SL-M orders (discontinued by exchange).
        # Use SL (limit) with a small market-protection buffer instead.
        # NFO supports SL-M, so we keep SL-M for NFO.
        sl_oid = None
        try:
            if fno_exchange == "BFO":
                # SL limit price = trigger * (1 - 0.5%) as market protection buffer
                sl_limit_price = round(sl_price * 0.995, 1)
                sl_oid = self.kite.place_order(
                    variety=self.kite.VARIETY_REGULAR,
                    exchange=fno_exchange,
                    tradingsymbol=tradingsymbol,
                    transaction_type=self.kite.TRANSACTION_TYPE_SELL,
                    quantity=qty,
                    order_type=self.kite.ORDER_TYPE_SL,
                    product=self.kite.PRODUCT_MIS,
                    trigger_price=sl_price,
                    price=sl_limit_price,
                    tag="EMA_SL",
                )
                log.info(f"[{name}] SL (limit) order_id = {sl_oid}  trigger={sl_price}  limit={sl_limit_price}")
            else:
                sl_oid = self.kite.place_order(
                    variety=self.kite.VARIETY_REGULAR,
                    exchange=fno_exchange,
                    tradingsymbol=tradingsymbol,
                    transaction_type=self.kite.TRANSACTION_TYPE_SELL,
                    quantity=qty,
                    order_type=self.kite.ORDER_TYPE_SLM,
                    product=self.kite.PRODUCT_MIS,
                    trigger_price=sl_price,
                    tag="EMA_SL",
                )
                log.info(f"[{name}] SL-M order_id = {sl_oid}  trigger={sl_price}")
        except Exception as e:
            log.error(f"[{name}] SL order failed: {e}")

        # ── Place Limit TP order ──────────────────────────────────────────────
        tp_oid = None
        try:
            tp_oid = self.kite.place_order(
                variety=self.kite.VARIETY_REGULAR,
                exchange=fno_exchange,
                tradingsymbol=tradingsymbol,
                transaction_type=self.kite.TRANSACTION_TYPE_SELL,
                quantity=qty,
                order_type=self.kite.ORDER_TYPE_LIMIT,
                product=self.kite.PRODUCT_MIS,
                price=tp_price,
                tag="EMA_TP",
            )
            log.info(f"[{name}] TP  order_id = {tp_oid}  price={tp_price}")
        except Exception as e:
            log.error(f"[{name}] TP order failed: {e}")

        # ── Store position state ──────────────────────────────────────────────
        with self._state_lock:
            self.state[name]["position"] = {
                "tradingsymbol": tradingsymbol,
                "token":         token,
                "exchange":      fno_exchange,
                "opt_type":      opt_type,
                "qty":           qty,
                "lot_size":      lot_size,
                "entry_price":   ltp,
                "sl_price":      sl_price,
                "tp_price":      tp_price,
                "entry_time":    datetime.now(IST),
                "entry_oid":     entry_oid,
                "sl_oid":        sl_oid,
                "tp_oid":        tp_oid,
            }

    # ── TRADE EXIT ────────────────────────────────────────────────────────────

    def _exit_trade_unlocked(self, name: str, reason: str) -> None:
        """Exit the open position for `name`. Caller must hold _state_lock or handle it."""
        pos = self.state[name].get("position")
        if pos is None:
            return

        log.info(f"[{name}] ◀ EXIT  reason={reason}  sym={pos['tradingsymbol']}")

        # Cancel pending SL and TP orders
        for key in ("sl_oid", "tp_oid"):
            oid = pos.get(key)
            if oid:
                try:
                    self.kite.cancel_order(
                        variety=self.kite.VARIETY_REGULAR, order_id=oid
                    )
                    log.info(f"[{name}]   Cancelled order {oid} ({key})")
                except Exception as e:
                    log.warning(f"[{name}]   Cancel {oid} failed (may already be complete): {e}")

        # Place market exit order
        try:
            exit_oid = self.kite.place_order(
                variety=self.kite.VARIETY_REGULAR,
                exchange=pos["exchange"],
                tradingsymbol=pos["tradingsymbol"],
                transaction_type=self.kite.TRANSACTION_TYPE_SELL,
                quantity=pos["qty"],
                order_type=self.kite.ORDER_TYPE_MARKET,
                product=self.kite.PRODUCT_MIS,
                tag="EMA_EXIT",
            )
            log.info(f"[{name}]   Exit order_id = {exit_oid}")
        except Exception as e:
            log.error(f"[{name}]   Exit order failed: {e}")

        self.state[name]["position"] = None

    def _exit_trade(self, name: str, reason: str) -> None:
        with self._state_lock:
            self._exit_trade_unlocked(name, reason)

    # ── POSITION MONITOR THREAD ───────────────────────────────────────────────

    def _monitor_positions(self) -> None:
        """
        Background thread: polls order statuses every second.
        Handles:
          • SL hit  → cancels TP order, clears position
          • TP hit  → cancels SL order, clears position
          • Auto square-off at SQUAREOFF_AT
        """
        while self._running:
            now = datetime.now(IST)

            # ── Auto square-off ────────────────────────────────────────────────
            if (now.hour, now.minute) >= SQUAREOFF_AT:
                for name in list(self.state.keys()):
                    with self._state_lock:
                        if self.state[name]["position"] is not None:
                            log.warning(
                                f"[{name}] AUTO SQUAREOFF at {now.strftime('%H:%M')}"
                            )
                            self._exit_trade_unlocked(name, "SQUAREOFF")

            # ── SL / TP fill check ─────────────────────────────────────────────
            for name in list(self.state.keys()):
                with self._state_lock:
                    pos = self.state[name].get("position")
                if pos is None:
                    continue

                for trigger_key, cancel_key, label in [
                    ("sl_oid", "tp_oid", "SL_HIT"),
                    ("tp_oid", "sl_oid", "TP_HIT"),
                ]:
                    oid = pos.get(trigger_key)
                    if not oid:
                        continue
                    try:
                        last_update = self.kite.order_history(oid)[-1]
                        if last_update["status"] == "COMPLETE":
                            log.info(
                                f"[{name}] ■ {label}  order={oid}  "
                                f"fill_price={last_update.get('average_price', '?')}"
                            )
                            cancel_oid = pos.get(cancel_key)
                            if cancel_oid:
                                try:
                                    self.kite.cancel_order(
                                        variety=self.kite.VARIETY_REGULAR,
                                        order_id=cancel_oid,
                                    )
                                except Exception:
                                    pass
                            with self._state_lock:
                                self.state[name]["position"] = None
                            break
                    except Exception as e:
                        log.debug(f"[{name}] Order history check error ({oid}): {e}")

            time.sleep(1)

    # ── WEBSOCKET HANDLERS ────────────────────────────────────────────────────

    def _on_ticks(self, ws, ticks: list) -> None:
        now = datetime.now(IST)

        # Ignore ticks outside trading window
        if (now.hour, now.minute) < TRADE_START:
            return
        if (now.hour, now.minute) > MARKET_CLOSE:
            return

        # Build quick token → LTP map
        ltp_map: dict[int, float] = {
            t["instrument_token"]: t["last_price"]
            for t in ticks
            if "last_price" in t and t["last_price"] > 0
        }

        for name, st in self.state.items():
            token = st["token"]
            if token not in ltp_map:
                continue

            price     = ltp_map[token]
            completed = st["builder"].push_tick(price, now)

            # Only process when a 1-min bar has just completed
            if completed is None:
                continue

            if not st["builder"].is_ready():
                log.debug(
                    f"[{name}] Warming up … {st['builder'].completed_count}/{WARMUP_BARS} bars"
                )
                continue

            closes = st["builder"].get_closes()
            signal = detect_crossover(closes)

            if signal is None:
                continue

            series     = pd.Series(closes, dtype=float)
            ema9_vals  = _ema(series, EMA_SHORT)
            ema50_vals = _ema(series, EMA_LONG)
            log.info(
                f"[{name}] CROSSOVER  signal={signal}  "
                f"close={completed['close']}  "
                f"EMA9={float(ema9_vals.iloc[-1]):.2f}  EMA50={float(ema50_vals.iloc[-1]):.2f}  "
                f"bar_time={completed['time']}"
            )

            # Only enter new positions before NO_NEW_ENTRY time
            if (now.hour, now.minute) >= NO_NEW_ENTRY:
                log.info(f"[{name}] Signal {signal} ignored — past no-new-entry time")
                continue

            # Prevent duplicate signal re-entry on the same cross
            if signal == st.get("last_signal"):
                log.debug(f"[{name}] Signal {signal} already processed — skipping")
                continue

            st["last_signal"] = signal
            # Run entry in a separate thread to keep tick handler non-blocking
            threading.Thread(
                target=self._enter_trade,
                args=(name, signal, completed["close"]),
                daemon=True,
                name=f"Entry_{name}",
            ).start()

    def _on_connect(self, ws, response) -> None:
        tokens = [st["token"] for st in self.state.values()]
        log.info(f"WebSocket connected — subscribing to tokens: {tokens}")
        ws.subscribe(tokens)
        ws.set_mode(ws.MODE_LTP, tokens)

    def _on_close(self, ws, code, reason) -> None:
        log.warning(f"WebSocket closed — code={code}  reason={reason}")

    def _on_error(self, ws, code, reason) -> None:
        log.error(f"WebSocket error  — code={code}  reason={reason}")

    def _on_reconnect(self, ws, attempt: int) -> None:
        log.warning(f"WebSocket reconnecting … attempt #{attempt}")

    def _on_noreconnect(self, ws) -> None:
        log.error("WebSocket max reconnect attempts reached — manual restart required")

    # ── MAIN RUN LOOP ─────────────────────────────────────────────────────────

    def run(self) -> None:
        log.info("=" * 70)
        log.info("  KITE EMA 9/50 BOT  —  NIFTY & SENSEX Index F&O  (1-min bars)")
        log.info("=" * 70)

        # 1. Load instruments once
        self._load_fno_instruments()

        # 2. Initialise per-index state and seed historical candles
        for name, cfg in INDICES.items():
            self._initialise_index(name, cfg)
            self._seed_historical_candles(name)

        # 3. Start position monitor background thread
        self._running = True
        monitor_thread = threading.Thread(
            target=self._monitor_positions,
            daemon=True,
            name="PositionMonitor",
        )
        monitor_thread.start()
        log.info("Position monitor thread started")

        # 4. Start KiteTicker WebSocket
        ticker = KiteTicker(API_KEY, ACCESS_TOKEN)
        ticker.on_ticks      = self._on_ticks
        ticker.on_connect    = self._on_connect
        ticker.on_close      = self._on_close
        ticker.on_error      = self._on_error
        ticker.on_reconnect  = self._on_reconnect
        ticker.on_noreconnect = self._on_noreconnect

        log.info("Starting KiteTicker WebSocket …")
        ticker.connect(threaded=True)

        # 5. Keep main thread alive until market close or keyboard interrupt
        try:
            while True:
                now = datetime.now(IST)
                if (now.hour, now.minute) > MARKET_CLOSE:
                    log.info(f"Market closed ({MARKET_CLOSE[0]}:{MARKET_CLOSE[1]:02d} IST) — shutting down")
                    break
                time.sleep(10)
        except KeyboardInterrupt:
            log.info("Interrupted by user — shutting down …")
        finally:
            self._running = False
            ticker.close()
            log.info("Bot stopped cleanly.")


# ═══════════════════════════════════════════════════════════════════════════════
#  ENTRY POINT
# ═══════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    bot = EMABot()
    bot.run()
