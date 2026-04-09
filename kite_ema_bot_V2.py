#!/usr/bin/env python3
"""
+======================================================================+
|   KITE EMA 9/50 AUTO TRADING BOT  -  NIFTY 50 & SENSEX Index F&O    |
|                                                                      |
|   NEW STRATEGY (Option Premium Chart Based):                         |
|                                                                      |
|   1. Get NIFTY/SENSEX spot price, compute ATM strike                 |
|   2. Find current-week ATM CE and ATM PE option instruments          |
|   3. Subscribe to BOTH option premium live ticks (NOT the index)     |
|   4. Build separate 1-min candle charts for CE premium & PE premium  |
|   5. Compute EMA-9 and EMA-50 on each option's own price chart       |
|   6. When EMA-9 crosses ABOVE EMA-50 on CE chart -> BUY CE          |
|   7. When EMA-9 crosses ABOVE EMA-50 on PE chart -> BUY PE          |
|                                                                      |
|   Logic: When index price moves UP, CE premium rises, causing        |
|          EMA-9 > EMA-50 crossover on the CE chart -> BUY CE          |
|          When index price moves DOWN, PE premium rises, causing      |
|          EMA-9 > EMA-50 crossover on the PE chart -> BUY PE          |
|                                                                      |
|   Risk:  10-point SL  |  TP: NIFTY 20, SENSEX 50                    |
|   Size:  1 lot per trade                                             |
|   Scan:  Every tick via KiteTicker WebSocket                         |
|   Exit:  Auto square-off at 15:20 IST - MIS product                 |
+======================================================================+

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

# ---------------------------------------------------------------------------
# CREDENTIALS  (keep these secret - never commit to version control)
# ---------------------------------------------------------------------------
API_KEY      = "v7hyrrhlh85v4l1e"
ACCESS_TOKEN = "HezK50vACfk6ugYERPS51D9igNe3oHyE"

# ---------------------------------------------------------------------------
# STRATEGY PARAMETERS
# ---------------------------------------------------------------------------
STOP_LOSS_POINTS = 10   # fixed option premium stop-loss distance in points
LOTS_PER_TRADE   = 1    # always trade exactly 1 lot

EMA_SHORT     = 9
EMA_LONG      = 50
WARMUP_BARS   = EMA_LONG + 10   # minimum completed 1-min candles before trading

# ---------------------------------------------------------------------------
# MARKET TIMING  (IST)
# ---------------------------------------------------------------------------
TRADE_START   = (9, 15)   # no entry before 09:15
NO_NEW_ENTRY  = (15, 0)   # no new entries after 15:00
SQUAREOFF_AT  = (15, 20)  # force exit all positions by 15:20
MARKET_CLOSE  = (15, 31)  # stop all processing after 15:31

# ---------------------------------------------------------------------------
# INDEX CONFIGURATION
# ---------------------------------------------------------------------------
INDICES = {
    "NIFTY": {
        "underlying_exchange": "NSE",
        "underlying_symbol":   "NIFTY 50",
        "fno_exchange":        "NFO",
        "instrument_name":     "NIFTY",
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

# ---------------------------------------------------------------------------
# LOGGING
# ---------------------------------------------------------------------------
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


# ===========================================================================
#  CANDLE BUILDER  (builds 1-min OHLC candles from live ticks)
# ===========================================================================

class CandleBuilder:
    """
    Aggregates streaming price ticks into completed 1-minute OHLC candles.
    Each option (CE / PE) gets its own CandleBuilder instance so we track
    the option premium chart independently.
    """

    def __init__(self, label="", max_candles=WARMUP_BARS + 20):
        self._candles = deque(maxlen=max_candles)
        self._current = None
        self._lock = threading.Lock()
        self._label = label

    def seed_candles(self, historical):
        """Seed with historical kite candle dicts."""
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

    def push_tick(self, price, ts):
        """
        Feed a live price tick.
        Returns the completed candle dict when the minute rolls over,
        otherwise returns None.
        """
        minute_floor = ts.replace(second=0, microsecond=0)
        with self._lock:
            current = self._current
            if current is None:
                self._current = _new_candle(price, minute_floor)
                return None

            if minute_floor > current["time"]:
                completed = dict(current)
                self._candles.append(completed)
                self._current = _new_candle(price, minute_floor)
                return completed

            current["high"]  = max(current["high"], price)
            current["low"]   = min(current["low"], price)
            current["close"] = price
            return None

    @property
    def completed_count(self):
        with self._lock:
            return len(self._candles)

    def get_closes(self):
        """Return close prices of all completed candles (oldest first)."""
        with self._lock:
            return [c["close"] for c in self._candles]

    def is_ready(self):
        """True once enough candles exist for EMA calculation."""
        return self.completed_count >= WARMUP_BARS


def _new_candle(price, ts):
    return {"time": ts, "open": price, "high": price, "low": price, "close": price}


# ===========================================================================
#  EMA & CROSSOVER LOGIC  (applied on option premium chart)
# ===========================================================================

def _ema(series, period):
    """EMA identical to TradingView ta.ema() - ewm(span, adjust=False)."""
    return series.ewm(span=period, adjust=False).mean()


def detect_ema9_cross_above_ema50(closes):
    """
    Check if EMA-9 just crossed ABOVE EMA-50 on the option's own premium chart.

    This is the ONLY signal we care about:
      - On CE chart: EMA-9 crosses above EMA-50 means CE premium is rising -> BUY CE
      - On PE chart: EMA-9 crosses above EMA-50 means PE premium is rising -> BUY PE

    Returns True if crossover happened, False otherwise.
    """
    if len(closes) < WARMUP_BARS:
        return False

    series = pd.Series(closes, dtype=float)
    ema9   = _ema(series, EMA_SHORT)
    ema50  = _ema(series, EMA_LONG)

    if len(ema9) < 2:
        return False

    f_prev = float(ema9.iloc[-2])
    f_curr = float(ema9.iloc[-1])
    s_prev = float(ema50.iloc[-2])
    s_curr = float(ema50.iloc[-1])

    # EMA-9 crosses ABOVE EMA-50
    crossover = (f_prev < s_prev) and (f_curr > s_curr)
    return crossover


def get_ema_values(closes):
    """Return current EMA-9 and EMA-50 values for logging."""
    series = pd.Series(closes, dtype=float)
    ema9   = _ema(series, EMA_SHORT)
    ema50  = _ema(series, EMA_LONG)
    return float(ema9.iloc[-1]), float(ema50.iloc[-1])


# ===========================================================================
#  MAIN BOT CLASS
# ===========================================================================

class EMABot:
    """
    Kite EMA 9/50 live trading bot.

    NEW LOGIC:
    - Subscribes to BOTH ATM CE and ATM PE option premium ticks
    - Builds independent 1-min candle charts for each option
    - Applies EMA 9/50 on each option's premium chart separately
    - BUY signal = EMA-9 crosses ABOVE EMA-50 on that option's chart
    - When NIFTY goes UP -> CE premium rises -> EMA cross on CE -> BUY CE
    - When NIFTY goes DOWN -> PE premium rises -> EMA cross on PE -> BUY PE
    """

    def __init__(self):
        self.kite = KiteConnect(api_key=API_KEY)
        self.kite.set_access_token(ACCESS_TOKEN)

        self._state_lock = threading.Lock()
        self._running    = False

        # Per-index runtime state
        self.state = {}

        # token -> (index_name, opt_type) mapping for tick routing
        self._token_map = {}

        # Instrument DataFrames
        self._nfo_df = pd.DataFrame()
        self._bfo_df = pd.DataFrame()

    # -- INITIALISATION ----------------------------------------------------

    def _load_fno_instruments(self):
        """Fetch and cache all NFO and BFO instrument definitions."""
        log.info("Loading NFO instruments ...")
        try:
            self._nfo_df = pd.DataFrame(self.kite.instruments("NFO"))
            if not self._nfo_df.empty and "expiry" in self._nfo_df.columns:
                self._nfo_df["expiry"] = pd.to_datetime(self._nfo_df["expiry"]).dt.date
            log.info("  NFO: %d instruments loaded", len(self._nfo_df))
        except Exception as e:
            log.error("  NFO instruments load failed: %s", e)

        log.info("Loading BFO instruments ...")
        try:
            self._bfo_df = pd.DataFrame(self.kite.instruments("BFO"))
            if not self._bfo_df.empty and "expiry" in self._bfo_df.columns:
                self._bfo_df["expiry"] = pd.to_datetime(self._bfo_df["expiry"]).dt.date
            log.info("  BFO: %d instruments loaded", len(self._bfo_df))
        except Exception as e:
            log.warning("  BFO instruments load failed: %s", e)

    def _get_spot_price(self, exchange, symbol):
        """Fetch the current spot/index price via LTP quote."""
        try:
            key = "%s:%s" % (exchange, symbol)
            quote = self.kite.ltp(key)
            ltp = float(quote[key]["last_price"])
            log.info("  Spot price %s = %.2f", key, ltp)
            return ltp
        except Exception as e:
            log.error("  Failed to fetch spot price for %s:%s: %s", exchange, symbol, e)
            return None

    def _nearest_weekly_expiry(self, df, name_prefix):
        """Return the nearest upcoming (current week) expiry date."""
        today = datetime.now(IST).date()
        subset = df[df["name"] == name_prefix]
        if subset.empty:
            return None
        future_expiries = sorted({
            e for e in subset["expiry"].dropna() if e >= today
        })
        return future_expiries[0] if future_expiries else None

    def _find_atm_option_instrument(self, name, spot, opt_type):
        """
        Find the current-week ATM CE or PE option instrument.
        Returns the instrument dict or None.
        """
        cfg    = INDICES[name]
        prefix = cfg["instrument_name"]
        gap    = cfg["strike_gap"]
        df     = self._nfo_df if cfg["fno_exchange"] == "NFO" else self._bfo_df

        if df.empty:
            log.error("[%s] Instrument DataFrame is empty", name)
            return None

        expiry = self._nearest_weekly_expiry(df, prefix)
        if expiry is None:
            log.error("[%s] No upcoming expiry found for '%s'", name, prefix)
            return None

        atm_strike = round(spot / gap) * gap

        for delta_mult in [0, 1, -1, 2, -2, 3, -3]:
            strike = atm_strike + delta_mult * gap
            mask = (
                (df["name"] == prefix)
                & (df["instrument_type"] == opt_type)
                & (df["expiry"] == expiry)
                & (df["strike"] == float(strike))
            )
            hit = df[mask]
            if not hit.empty:
                row = hit.iloc[0]
                log.info(
                    "[%s] Found ATM %s: %s  strike=%s  expiry=%s  lot=%s",
                    name, opt_type, row["tradingsymbol"], strike, expiry,
                    row.get("lot_size", "?")
                )
                return row.to_dict()

        log.error("[%s] ATM %s option not found near strike %s (expiry %s)",
                  name, opt_type, atm_strike, expiry)
        return None

    def _initialise_index(self, name, cfg):
        """
        For each index:
        1. Get spot price to determine ATM strike
        2. Find current-week ATM CE and ATM PE instruments
        3. Create separate CandleBuilders for CE and PE premium charts
        4. Seed historical candles for both options
        """
        log.info("[%s] Initialising ...", name)

        spot = self._get_spot_price(cfg["underlying_exchange"], cfg["underlying_symbol"])
        if spot is None:
            log.error("[%s] Cannot get spot price - skipping this index", name)
            return

        atm_strike = round(spot / cfg["strike_gap"]) * cfg["strike_gap"]
        log.info("[%s] Spot=%.2f  ATM Strike=%d", name, spot, atm_strike)

        ce_instr = self._find_atm_option_instrument(name, spot, "CE")
        pe_instr = self._find_atm_option_instrument(name, spot, "PE")

        if ce_instr is None or pe_instr is None:
            log.error("[%s] Could not find both CE and PE options - skipping", name)
            return

        ce_token = int(ce_instr["instrument_token"])
        pe_token = int(pe_instr["instrument_token"])

        self.state[name] = {
            "name":         name,
            "cfg":          cfg,
            "spot":         spot,
            "atm_strike":   atm_strike,
            "ce_instr":     ce_instr,
            "pe_instr":     pe_instr,
            "ce_token":     ce_token,
            "pe_token":     pe_token,
            "ce_builder":   CandleBuilder(label="%s_CE" % name),
            "pe_builder":   CandleBuilder(label="%s_PE" % name),
            "position":     None,
            "ce_last_signal": False,
            "pe_last_signal": False,
        }

        self._token_map[ce_token] = (name, "CE")
        self._token_map[pe_token] = (name, "PE")

        log.info("[%s] CE token=%d (%s)  PE token=%d (%s)",
                 name, ce_token, ce_instr["tradingsymbol"],
                 pe_token, pe_instr["tradingsymbol"])

        self._seed_option_candles(name, "CE", ce_instr, "ce_builder")
        self._seed_option_candles(name, "PE", pe_instr, "pe_builder")

    def _seed_option_candles(self, name, opt_type, instr, builder_key):
        """Pre-load historical 1-min candles for an option premium chart."""
        token = int(instr["instrument_token"])
        to_dt = datetime.now(IST).replace(second=0, microsecond=0)
        fr_dt = to_dt - timedelta(minutes=max(WARMUP_BARS * 2, 120))

        log.info("[%s] Seeding %s historical candles ...", name, opt_type)
        try:
            candles = self.kite.historical_data(
                instrument_token=token,
                from_date=fr_dt.strftime("%Y-%m-%d %H:%M:%S"),
                to_date=to_dt.strftime("%Y-%m-%d %H:%M:%S"),
                interval="minute",
            )
            self.state[name][builder_key].seed_candles(candles)
            count = self.state[name][builder_key].completed_count
            log.info("[%s] %s seeded %d candles (builder has %d)",
                     name, opt_type, len(candles), count)
        except Exception as e:
            log.error("[%s] %s historical seed failed: %s", name, opt_type, e)

    # -- TRADE ENTRY -------------------------------------------------------

    def _enter_trade(self, name, opt_type, entry_price):
        """
        Execute a BUY order for the option whose premium chart just showed
        EMA-9 crossing above EMA-50.
        """
        st  = self.state[name]
        cfg = st["cfg"]

        with self._state_lock:
            pos = st["position"]
            if pos is not None:
                if pos["opt_type"] == opt_type:
                    log.info("[%s] Already holding %s - duplicate signal ignored",
                             name, opt_type)
                    return
                log.info("[%s] Reversing %s -> %s", name, pos["opt_type"], opt_type)
                self._exit_trade_unlocked(name, "REVERSE")

        instr = st["ce_instr"] if opt_type == "CE" else st["pe_instr"]
        fno_exchange  = cfg["fno_exchange"]
        tradingsymbol = instr["tradingsymbol"]
        token         = int(instr["instrument_token"])
        lot_size      = int(instr.get("lot_size", 1))

        try:
            q_key = "%s:%s" % (fno_exchange, tradingsymbol)
            quote = self.kite.quote(q_key)
            ltp   = float(quote[q_key]["last_price"])
        except Exception as e:
            log.error("[%s] Option LTP fetch failed for %s: %s",
                      name, tradingsymbol, e)
            ltp = entry_price

        if ltp <= 0:
            log.error("[%s] Option LTP is 0 for %s - trade skipped",
                      name, tradingsymbol)
            return

        qty = LOTS_PER_TRADE * lot_size
        target_points = float(cfg.get("target_points", 0))

        if STOP_LOSS_POINTS <= 0 or target_points <= 0:
            log.error("[%s] Invalid risk config: SL=%s TP=%s",
                      name, STOP_LOSS_POINTS, target_points)
            return

        sl_price = round(ltp - STOP_LOSS_POINTS, 1)
        tp_price = round(ltp + target_points, 1)

        if sl_price <= 0 or tp_price <= ltp:
            log.error("[%s] Invalid SL/TP: ltp=%.1f SL=%.1f TP=%.1f",
                      name, ltp, sl_price, tp_price)
            return

        rr_ratio = round(target_points / STOP_LOSS_POINTS, 2)
        log.info(
            "[%s] >> ENTRY %s  sym=%s  ltp=%.1f  qty=%d  "
            "SL=%.1f (-%d)  TP=%.1f (+%d)  RR=1:%.2f",
            name, opt_type, tradingsymbol, ltp, qty,
            sl_price, STOP_LOSS_POINTS, tp_price, int(target_points), rr_ratio
        )

        # -- Place market entry order --
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
            log.info("[%s] Entry order_id = %s", name, entry_oid)
        except Exception as e:
            log.error("[%s] Entry order failed: %s", name, e)
            return

        # -- Place SL order --
        sl_oid = None
        try:
            if fno_exchange == "BFO":
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
                log.info("[%s] SL (limit) order_id=%s trigger=%.1f limit=%.1f",
                         name, sl_oid, sl_price, sl_limit_price)
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
                log.info("[%s] SL-M order_id=%s trigger=%.1f",
                         name, sl_oid, sl_price)
        except Exception as e:
            log.error("[%s] SL order failed: %s", name, e)

        # -- Place TP order --
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
            log.info("[%s] TP order_id=%s price=%.1f", name, tp_oid, tp_price)
        except Exception as e:
            log.error("[%s] TP order failed: %s", name, e)

        # -- Store position state --
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

    # -- TRADE EXIT --------------------------------------------------------

    def _exit_trade_unlocked(self, name, reason):
        """Exit the open position. Caller must hold _state_lock."""
        pos = self.state[name].get("position")
        if pos is None:
            return

        log.info("[%s] << EXIT  reason=%s  sym=%s",
                 name, reason, pos["tradingsymbol"])

        for key in ("sl_oid", "tp_oid"):
            oid = pos.get(key)
            if oid:
                try:
                    self.kite.cancel_order(
                        variety=self.kite.VARIETY_REGULAR, order_id=oid)
                    log.info("[%s]   Cancelled order %s (%s)", name, oid, key)
                except Exception as e:
                    log.warning("[%s]   Cancel %s failed: %s", name, oid, e)

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
            log.info("[%s]   Exit order_id = %s", name, exit_oid)
        except Exception as e:
            log.error("[%s]   Exit order failed: %s", name, e)

        self.state[name]["position"] = None

    def _exit_trade(self, name, reason):
        with self._state_lock:
            self._exit_trade_unlocked(name, reason)

    # -- POSITION MONITOR THREAD -------------------------------------------

    def _monitor_positions(self):
        """
        Background thread: polls order statuses every second.
        Handles SL hit, TP hit, and auto square-off.
        """
        while self._running:
            now = datetime.now(IST)

            if (now.hour, now.minute) >= SQUAREOFF_AT:
                for name in list(self.state.keys()):
                    with self._state_lock:
                        if self.state[name]["position"] is not None:
                            log.warning("[%s] AUTO SQUAREOFF at %s",
                                        name, now.strftime("%H:%M"))
                            self._exit_trade_unlocked(name, "SQUAREOFF")

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
                                "[%s] ## %s  order=%s  fill=%.2f",
                                name, label, oid,
                                last_update.get("average_price", 0)
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
                        log.debug("[%s] Order history check error (%s): %s",
                                  name, oid, e)

            time.sleep(1)

    # -- WEBSOCKET HANDLERS ------------------------------------------------

    def _on_ticks(self, ws, ticks):
        """
        Process live ticks from OPTION premium feeds (CE and PE).
        Each tick is routed to the correct CandleBuilder based on token.
        When a 1-min candle completes, check EMA-9 vs EMA-50 crossover
        on that option's premium chart.
        """
        now = datetime.now(IST)

        if (now.hour, now.minute) < TRADE_START:
            return
        if (now.hour, now.minute) > MARKET_CLOSE:
            return

        for tick in ticks:
            token = tick.get("instrument_token")
            ltp   = tick.get("last_price", 0)

            if token is None or ltp <= 0:
                continue

            route = self._token_map.get(token)
            if route is None:
                continue

            index_name, opt_type = route
            st = self.state.get(index_name)
            if st is None:
                continue

            builder_key = "ce_builder" if opt_type == "CE" else "pe_builder"
            builder = st[builder_key]

            completed = builder.push_tick(ltp, now)

            if completed is None:
                continue

            if not builder.is_ready():
                log.debug("[%s_%s] Warming up ... %d/%d bars",
                          index_name, opt_type,
                          builder.completed_count, WARMUP_BARS)
                continue

            closes = builder.get_closes()
            crossover = detect_ema9_cross_above_ema50(closes)
            ema9_val, ema50_val = get_ema_values(closes)

            signal_key = ("ce_last_signal" if opt_type == "CE"
                          else "pe_last_signal")

            if not crossover:
                st[signal_key] = False
                continue

            # Crossover detected on this option's premium chart!
            log.info(
                "[%s] *** EMA CROSSOVER on %s chart ***  "
                "premium_close=%.2f  EMA9=%.2f  EMA50=%.2f  "
                "bar_time=%s",
                index_name, opt_type, completed["close"],
                ema9_val, ema50_val, completed["time"]
            )

            if (now.hour, now.minute) >= NO_NEW_ENTRY:
                log.info("[%s] %s signal ignored - past no-new-entry time",
                         index_name, opt_type)
                continue

            if st[signal_key]:
                log.debug("[%s] %s signal already processed - skipping",
                          index_name, opt_type)
                continue

            st[signal_key] = True

            log.info(
                "[%s] >>> BUY %s  "
                "(EMA-9 crossed above EMA-50 on %s premium chart)",
                index_name, opt_type, opt_type
            )

            threading.Thread(
                target=self._enter_trade,
                args=(index_name, opt_type, completed["close"]),
                daemon=True,
                name="Entry_%s_%s" % (index_name, opt_type),
            ).start()

    def _on_connect(self, ws, response):
        """Subscribe to all option tokens (CE + PE for each index)."""
        tokens = []
        for name, st in self.state.items():
            tokens.append(st["ce_token"])
            tokens.append(st["pe_token"])

        log.info("WebSocket connected - subscribing to option tokens: %s",
                 tokens)

        for name, st in self.state.items():
            log.info("  [%s] CE: %s (token=%d)",
                     name, st["ce_instr"]["tradingsymbol"], st["ce_token"])
            log.info("  [%s] PE: %s (token=%d)",
                     name, st["pe_instr"]["tradingsymbol"], st["pe_token"])

        ws.subscribe(tokens)
        ws.set_mode(ws.MODE_LTP, tokens)

    def _on_close(self, ws, code, reason):
        log.warning("WebSocket closed - code=%s  reason=%s", code, reason)

    def _on_error(self, ws, code, reason):
        log.error("WebSocket error  - code=%s  reason=%s", code, reason)

    def _on_reconnect(self, ws, attempt):
        log.warning("WebSocket reconnecting ... attempt #%d", attempt)

    def _on_noreconnect(self, ws):
        log.error("WebSocket max reconnect - manual restart required")

    # -- MAIN RUN LOOP -----------------------------------------------------

    def run(self):
        log.info("=" * 72)
        log.info("  KITE EMA 9/50 BOT  -  OPTION PREMIUM CHART STRATEGY")
        log.info("  Watch ATM CE + PE charts independently")
        log.info("  EMA-9 crosses ABOVE EMA-50 on option chart -> BUY it")
        log.info("  NIFTY up -> CE premium rises -> buy CE")
        log.info("  NIFTY down -> PE premium rises -> buy PE")
        log.info("=" * 72)

        # 1. Load instruments
        self._load_fno_instruments()

        # 2. Initialise per-index state
        for name, cfg in INDICES.items():
            self._initialise_index(name, cfg)

        if not self.state:
            log.error("No indices initialised successfully - exiting")
            return

        for name, st in self.state.items():
            log.info(
                "[%s] ATM Strike=%d  CE=%s  PE=%s",
                name, st["atm_strike"],
                st["ce_instr"]["tradingsymbol"],
                st["pe_instr"]["tradingsymbol"]
            )
            log.info(
                "[%s] CE candles=%d  PE candles=%d  (need %d for warmup)",
                name,
                st["ce_builder"].completed_count,
                st["pe_builder"].completed_count,
                WARMUP_BARS
            )

        # 3. Start position monitor thread
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
        ticker.on_ticks       = self._on_ticks
        ticker.on_connect     = self._on_connect
        ticker.on_close       = self._on_close
        ticker.on_error       = self._on_error
        ticker.on_reconnect   = self._on_reconnect
        ticker.on_noreconnect = self._on_noreconnect

        log.info("Starting KiteTicker WebSocket (option premium feeds) ...")
        ticker.connect(threaded=True)

        # 5. Keep main thread alive
        try:
            while True:
                now = datetime.now(IST)
                if (now.hour, now.minute) > MARKET_CLOSE:
                    log.info(
                        "Market closed (%d:%02d IST) - shutting down",
                        MARKET_CLOSE[0], MARKET_CLOSE[1]
                    )
                    break
                time.sleep(10)
        except KeyboardInterrupt:
            log.info("Interrupted by user - shutting down ...")
        finally:
            self._running = False
            ticker.close()
            log.info("Bot stopped cleanly.")


# ===========================================================================
#  ENTRY POINT
# ===========================================================================

if __name__ == "__main__":
    bot = EMABot()
    bot.run()

