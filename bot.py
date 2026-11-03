#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Laya STONKS — System 1 Trading Bot (Binance)

Concepto Laya: decisiones rápidas basadas en señales técnicas pre-filtradas,
no análisis profundo. Scalping / Midterm con SL/TP estrictos.

ADVERTENCIA: Este código es educativo. No garantiza ganancias.
Con $20 y comisiones Binance (~0.1% spot), el capital se consume rápido.
Ejecuta en modo "paper" (simulado) por defecto hasta confirmar.

Características:
- Modo live con Binance spot TESTNET (velas reales, dinero ficticio) o PAPER (sintético).
- Filtros anti-churn: extensión ATR, cuerpo de vela, filtro horario UTC,
  correlación con BTC (timeframe superior) y confirmación de volumen HTF.
- Gestión de posiciones: SL/TP fijos + trailing stop basado en ATR
  (activación %, multiplicador ATR, paso mínimo, distancia máxima).
- Comisión configurable (spot/futures) aplicada al PnL y al cash.
- Registro de trades en backtest_log.json.

System 2 — Deep Analysis (LLM configurable en config.json: provider google
(Gemini) u OpenAI-compatible como NVIDIA/OpenRouter):
- Contexto fundamental en tiempo real: Fear & Greed, CoinGecko macro, RSS news, MTF 1h.
- Filtro de entradas basado en análisis LLM (use_for_filter) y
  ajuste opcional de SL/TP (use_for_sl_tp_adjust).

Uso:
    python bot.py            # un ciclo
    python bot.py --loop     # bucle continuo (tick_interval de config.json)
"""

import json
import math
import os
import re
import sys
import time
import zlib
import requests
import xml.etree.ElementTree as ET
from datetime import datetime, timezone

# Análisis profundo (System 2)
from deep_analysis import DeepAnalysisEngine

# Librerías: instalar con: pip install python-binance pandas numpy
# Si no están disponibles, el bot fallará intencionalmente para que lo notes.
try:
    import pandas as pd
    import numpy as np
except ImportError as e:
    print(f"FALTA DEPENDENCIA: {e}. Instala: pip install pandas numpy requests python-binance")
    sys.exit(1)

CONFIG_FILE = "config.json"

# --- Capa de contexto fundamental (igual que server.py, 100% gratuita) ---
FNG_TTL = 600.0      # Fear & Greed (alternative.me): 10 min
GLOBAL_TTL = 600.0   # Macro global (CoinGecko /global): 10 min
NEWS_TTL = 300.0     # Titulares RSS (CoinDesk + Cointelegraph): 5 min
MTF_TTL = 300.0      # Sesgo multi-temporal (candles 1h): 5 min

_ctx_session = requests.Session()
_ctx_session.headers.update({"User-Agent": "LayaSTONKS/1.0"})

_ctx_cache = {}
_ctx_timestamps = {}

NEWS_BULL = ("rally", "surge", "soar", "gain", "bullish", "jump", "climb", "upside",
             "breakout", "inflow", "adoption", "record", "rebound", "recover",
             "all-time high", "ath")
NEWS_BEAR = ("crash", "plunge", "slump", "bearish", "fall", "sink", "tumble", "hack",
             "exploit", "ban", "crackdown", "outflow", "dump", "bloodbath", "lawsuit",
             "fraud", "bankruptcy")


def _ctx_get_cached(key, ttl, fetcher):
    now = time.time()
    if key in _ctx_cache and (now - _ctx_timestamps.get(key, 0)) < ttl:
        return _ctx_cache[key]
    try:
        data = fetcher()
        _ctx_cache[key] = data
        _ctx_timestamps[key] = now
        return data
    except Exception as e:
        if key in _ctx_cache:
            return dict(_ctx_cache[key], stale=True, error=str(e)[:70])
        return {"error": str(e)[:70]}


def fetch_fng():
    """Fear & Greed Index desde alternative.me (sin claves)."""
    r = _ctx_session.get("https://api.alternative.me/fng/", params={"limit": 1}, timeout=8)
    r.raise_for_status()
    data = r.json().get("data", [{}])[0]
    v = int(data.get("value", 50))
    label_en = data.get("value_classification", "Neutral")
    # Etiquetas en español
    if v <= 24:
        label_es = "Miedo extremo"
    elif v <= 44:
        label_es = "Miedo"
    elif v <= 54:
        label_es = "Neutral"
    elif v <= 74:
        label_es = "Codicia"
    else:
        label_es = "Codicia extrema"
    return {"value": v, "label": label_es, "label_en": label_en, "ts": data.get("timestamp")}


def fetch_global():
    """Macro global desde CoinGecko /global (sin claves)."""
    r = _ctx_session.get("https://api.coingecko.com/api/v3/global", timeout=8)
    r.raise_for_status()
    d = r.json().get("data", {})
    return {
        "mcap": f"${d.get('total_market_cap', {}).get('usd', 0) / 1e12:.1f}T",
        "change_24h": round(d.get("market_cap_change_percentage_24h_usd", 0), 1),
        "btc_dom": round(d.get("market_cap_percentage", {}).get("btc", 0), 1),
        "eth_dom": round(d.get("market_cap_percentage", {}).get("eth", 0), 1),
    }


def fetch_news():
    """Titulares RSS de CoinDesk + Cointelegraph (lexicon scoring)."""
    titles = []
    # CoinDesk
    try:
        r = _ctx_session.get("https://www.coindesk.com/arc/outboundfeeds/rss/", timeout=8)
        root = ET.fromstring(r.content)
        for item in root.findall(".//item")[:10]:
            title = item.findtext("title")
            if title:
                titles.append(title)
    except Exception:
        pass
    # Cointelegraph
    try:
        r = _ctx_session.get("https://cointelegraph.com/rss", timeout=8)
        root = ET.fromstring(r.content)
        for item in root.findall(".//item")[:10]:
            title = item.findtext("title")
            if title:
                titles.append(title)
    except Exception:
        pass

    if not titles:
        return {"sentiment": 0, "label": "Neutral", "samples": []}

    pos = neg = 0
    for t in titles:
        tl = t.lower()
        p = sum(1 for w in NEWS_BULL if re.search(r"\b" + re.escape(w) + r"\b", tl))
        n = sum(1 for w in NEWS_BEAR if re.search(r"\b" + re.escape(w) + r"\b", tl))
        if p > n:
            pos += 1
        elif n > p:
            neg += 1
    total = pos + neg
    score = round(max(-100, min(100, (pos - neg) / total * 100)), 1) if total else 0
    if score <= -25:
        label = "Pesimista"
    elif score >= 25:
        label = "Optimista"
    else:
        label = "Neutral"
    return {"sentiment": score, "label": label, "samples": titles[:5]}


def ema_series(values, span):
    out = []
    alpha = 2.0 / (span + 1.0)
    prev = None
    for v in values:
        prev = v if prev is None else alpha * v + (1.0 - alpha) * prev
        out.append(prev)
    return out


def rsi_series(closes, period=14):
    out = [None] * len(closes)
    if len(closes) <= period:
        return out
    gains, losses = [], []
    for i in range(1, len(closes)):
        ch = closes[i] - closes[i - 1]
        gains.append(max(ch, 0.0))
        losses.append(max(-ch, 0.0))

    def rsi_of(ag, al):
        if al <= 0:
            return 100.0
        return 100.0 - 100.0 / (1.0 + ag / al)

    avg_gain = sum(gains[:period]) / period
    avg_loss = sum(losses[:period]) / period
    out[period] = rsi_of(avg_gain, avg_loss)
    for i in range(period, len(gains)):
        avg_gain = (avg_gain * (period - 1) + gains[i]) / period
        avg_loss = (avg_loss * (period - 1) + losses[i]) / period
        out[i + 1] = rsi_of(avg_gain, avg_loss)
    return out


def fetch_mtf(pair, timeframe="1h", limit=200):
    """Descarga velas 1h reales de Binance y calcula el sesgo multi-temporal."""
    try:
        url = "https://testnet.binance.vision/api/v3/klines"
        params = {"symbol": pair, "interval": timeframe, "limit": limit}
        r = _ctx_session.get(url, params=params, timeout=10)
        r.raise_for_status()
        raw = r.json()
        candles = []
        for k in raw:
            try:
                candles.append({
                    "time": int(int(k[0]) // 1000),
                    "open": float(k[1]), "high": float(k[2]), "low": float(k[3]),
                    "close": float(k[4]), "volume": float(k[5]),
                })
            except Exception:
                continue
        if len(candles) < 30:
            return {"trend": "SIN DATOS", "rsi": None, "timeframe": timeframe}
        closes = [c["close"] for c in candles]
        ef = ema_series(closes, 9)[-1]
        es = ema_series(closes, 21)[-1]
        rsi = rsi_series(closes, 14)[-1]
        px = closes[-1]
        if ef is None or es is None:
            return {"trend": "SIN DATOS", "rsi": None, "timeframe": timeframe}
        if ef > es and px >= es:
            trend = "ALCISTA"
        elif ef < es and px <= es:
            trend = "BAJISTA"
        else:
            trend = "LATERAL"
        return {"trend": trend, "rsi": round(rsi, 1) if rsi else None, "timeframe": timeframe}
    except Exception:
        return {"trend": "ERROR", "rsi": None, "timeframe": timeframe}


def get_context(pair="BTCUSDT"):
    """Obtiene todo el contexto fundamental + MTF para un par."""
    fng = _ctx_get_cached("fng", FNG_TTL, fetch_fng)
    global_ctx = _ctx_get_cached("global", GLOBAL_TTL, fetch_global)
    news = _ctx_get_cached("news", NEWS_TTL, fetch_news)
    mtf = _ctx_get_cached(f"mtf_{pair}", MTF_TTL, lambda: fetch_mtf(pair))
    return {"fng": fng, "global": global_ctx, "news": news, "mtf": {pair: mtf}}


INTERVAL_MIN = {
    "1m": 1, "3m": 3, "5m": 5, "15m": 15, "30m": 30,
    "1h": 60, "2h": 120, "4h": 240, "6h": 360, "8h": 480,
    "12h": 720, "1d": 1440,
}


def interval_to_minutes(interval):
    try:
        return INTERVAL_MIN.get(str(interval).lower(), 5)
    except Exception:
        return 5


def load_config(path=CONFIG_FILE):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def log(msg):
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    print(f"[{ts}] {msg}")


class LayaSystemOneBot:
    """System 1: señales rápidas con filtros; System 2 (LLM) como filtro de calidad."""

    def __init__(self, cfg):
        self.cfg = cfg
        self.mode = cfg.get("mode", "paper")
        self.pair = cfg.get("pair", "BTCUSDT").upper()
        self.capital = float(cfg.get("capital_usd", 20))
        self.max_pos = float(cfg.get("max_position_usd", 5))
        self.sl_pct = float(cfg.get("sl_pct", 0.025))
        self.tp_pct = float(cfg.get("tp_pct", 0.05))
        self.max_daily_loss = float(cfg.get("max_daily_loss_usd", 2))
        self.max_open = int(cfg.get("max_trades_open", 1))
        self.strategy = cfg.get("strategy", {})
        self.ind = cfg.get("indicators", {})
        self.ema_fast = int(self.ind.get("ema_fast", 9))
        self.ema_slow = int(self.ind.get("ema_slow", 21))
        self.rsi_period = int(self.ind.get("rsi_period", 14))
        self.vol_factor = float(self.ind.get("volume_factor", 1.2))

        # Parámetros de riesgo avanzado
        self.time_filter_enabled = bool(self.strategy.get("time_filter_enabled", True))
        self.time_filter_start = int(self.strategy.get("time_filter_start", 0))
        self.time_filter_end = int(self.strategy.get("time_filter_end", 8))
        self.btc_corr_filter = bool(self.strategy.get("btc_corr_filter", True))
        self.btc_corr_threshold = float(self.strategy.get("btc_corr_threshold", 0.7))
        self.btc_corr_lookback = int(self.strategy.get("btc_corr_lookback", 20))
        self.vol_confirm_tf = self.strategy.get("vol_confirm_tf", "1h")
        self.vol_confirm_factor = float(self.strategy.get("vol_confirm_factor", 1.0))
        self.trail_activation_pct = float(self.strategy.get("trail_activation_pct", 0.015))
        self.trail_atr_mult = float(self.strategy.get("trail_atr_mult", 1.5))
        self.trail_step_atr = float(self.strategy.get("trail_step_atr", 0.5))
        self.trail_max_pct = float(self.strategy.get("trail_max_pct", 0.08))
        self.cooldown_bars = int(self.strategy.get("cooldown_bars", 3))
        self.cooldown_loss_mult = float(self.strategy.get("cooldown_loss_mult", 2.0))

        # Comisión según tipo de mercado (spot / futuros en paper)
        self.fee_pct = self._resolve_fee_pct()

        # Estado de la cuenta
        self.cash = self.capital
        self.realized_pnl = 0.0
        self.daily_realized = 0.0
        self.history = []            # trades cerrados
        self.events = []             # feed de eventos para dashboard/server
        self.open_positions = {}     # {pair: {...}}
        self.last_close_ts = {}      # {pair: epoch} para cooldown
        self._day = datetime.now(timezone.utc).date()

        # Análisis profundo (System 2)
        self.deep = DeepAnalysisEngine(cfg)

        # En modo live conecta a Binance; en paper simula señales.
        self.testnet = bool(cfg.get("binance", {}).get("testnet", False))
        self.live_client = None
        if self.mode == "live" and cfg.get("binance", {}).get("api_key"):
            try:
                from binance.client import Client
                self.live_client = Client(
                    cfg["binance"]["api_key"],
                    cfg["binance"]["api_secret"],
                    testnet=self.testnet,
                )
                entorno = "TESTNET (dinero ficticio, sin riesgo real)" if self.testnet else "MAINNET (DINERO REAL)"
                log(f"Cliente Binance inicializado en modo LIVE: {entorno}.")
            except Exception as e:
                log(f"ERROR al conectar Binance: {e}")
                log("Sugerencia: pip install python-binance (y si es testnet, claves de testnet.binance.vision).")
                self.mode = "paper"
        if self.live_client is None:
            estado_tn = "testnet desde config: " + ("true" if self.testnet else "false")
            log(f"Modo PAPER activo. Par: {self.pair}. Capital: ${self.capital} ({estado_tn})")

    # ------------------------------------------------------------------ util ---
    def _resolve_fee_pct(self):
        fees = self.cfg.get("fees", {})
        if not fees.get("enabled", False):
            return 0.0
        key = "futures_pct" if self.strategy.get("futures_paper", False) else "spot_pct"
        return float(fees.get(key, 0.001))

    def add_task(self, kind, msg):
        """Registra un evento (para logging y para el feed del dashboard)."""
        self.events.append({
            "ts": datetime.now(timezone.utc).isoformat(),
            "kind": kind,
            "msg": msg,
        })
        log(f"[{kind}] {msg}")

    def _daily_reset(self):
        today = datetime.now(timezone.utc).date()
        if self._day != today:
            self._day = today
            self.daily_realized = 0.0
            log("Nuevo día UTC: contador de pérdida diaria reiniciado.")

    def _cooldown_remaining(self, pair, interval_minutes):
        """Segundos restantes de cooldown para un par (longer si la última entrada fue pérdida)."""
        last = self.last_close_ts.get(pair)
        if not last:
            return 0.0
        mult = self.cooldown_loss_mult
        # Guardamos también si cerró en pérdida para multiplicar el cooldown.
        if self.history:
            t = self.history[-1]
            mult = self.cooldown_loss_mult if (t.get("pair") == pair and float(t.get("pnl", 0)) < 0) else 1.0
        cooldown_s = self.cooldown_bars * interval_minutes * 60.0 * mult
        elapsed = time.time() - last
        return max(0.0, cooldown_s - elapsed)

    # ------------------------------------------------------------- klines ---
    def get_klines(self, interval="5m", limit=100, symbol=None, verbose=True):
        """Descarga velas: reales si hay cliente Binance, sintéticas en paper."""
        symbol = (symbol or self.pair).upper()
        if self.live_client is not None:
            try:
                raw = self.live_client.get_klines(symbol=symbol, interval=interval, limit=limit)
                cols = ["open_time", "open", "high", "low", "close", "volume",
                        "close_time", "quote_volume", "trades", "taker_base", "taker_quote", "ignore"]
                df = pd.DataFrame(raw, columns=cols)
                for c in ["open", "high", "low", "close", "volume"]:
                    df[c] = df[c].astype(float)
                ts = pd.to_datetime(pd.to_numeric(df["open_time"]).astype("int64"), unit="ms")
                df = df.set_index(pd.DatetimeIndex(ts, name="open_time"))
                df = df[["open", "high", "low", "close", "volume"]]
                if verbose:
                    env = "TESTNET" if self.testnet else "MAINNET"
                    log(f"Klines REALES ({env}) {symbol} {interval}: {len(df)} velas, "
                        f"último cierre = {df['close'].iloc[-1]:.2f}")
                return df
            except Exception as e:
                log(f"ERROR obteniendo klines reales ({symbol} {interval}): {e} — fallback a datos sintéticos.")
        # Datos sintéticos para paper: random walk con deriva por símbolo/intervalo.
        seed = zlib.crc32(f"{symbol}-{interval}".encode()) % (2 ** 32)
        rng = np.random.default_rng(seed)
        base_price = 65000.0 if symbol == "BTCUSDT" else 100.0 + (seed % 500)
        drift = math.sin(seed % 1000) * 4.0
        closes = base_price + np.cumsum(rng.normal(0.0, 15.0, size=limit)) + np.linspace(0.0, drift, limit)
        opens = np.concatenate(([closes[0]], closes[:-1]))
        highs = np.maximum(opens, closes) + rng.uniform(5, 30, size=limit)
        lows = np.minimum(opens, closes) - rng.uniform(5, 30, size=limit)
        volumes = rng.uniform(100, 300, size=limit) * rng.uniform(0.8, 1.6, size=limit)
        step = interval_to_minutes(interval)
        idx = pd.date_range(end=datetime.now(timezone.utc), periods=limit, freq=f"{step}min", tz="UTC")
        df = pd.DataFrame({
            "open": opens, "high": highs, "low": lows,
            "close": closes, "volume": volumes,
        }, index=pd.DatetimeIndex(idx, name="open_time"))
        if verbose:
            log(f"Klines SINTÉTICAS {symbol} {interval}: {len(df)} velas, último cierre = {df['close'].iloc[-1]:.2f}")
        return df

    # --------------------------------------------------------- indicadores ---
    def calculate_indicators(self, df):
        df = df.copy()
        df["ema_fast"] = df["close"].ewm(span=self.ema_fast, adjust=False).mean()
        df["ema_slow"] = df["close"].ewm(span=self.ema_slow, adjust=False).mean()
        # RSI (Wilder aproximado con rolling mean)
        delta = df["close"].diff()
        gain = delta.clip(lower=0)
        loss = -delta.clip(upper=0)
        avg_gain = gain.rolling(window=self.rsi_period, min_periods=1).mean()
        avg_loss = loss.rolling(window=self.rsi_period, min_periods=1).mean()
        rs = avg_gain / (avg_loss + 1e-9)
        df["rsi"] = 100 - (100 / (1 + rs))
        # Volumen relativo
        df["volume_mean"] = df["volume"].rolling(window=20, min_periods=1).mean()
        df["volume_ratio"] = df["volume"] / (df["volume_mean"] + 1e-9)
        # ATR (Average True Range), período 14
        high_low = df["high"] - df["low"]
        high_close = (df["high"] - df["close"].shift()).abs()
        low_close = (df["low"] - df["close"].shift()).abs()
        tr = pd.concat([high_low, high_close, low_close], axis=1).max(axis=1)
        df["atr"] = tr.rolling(window=14, min_periods=1).mean()
        # Body ratio (cuerpo / rango total) y extensión ATR vs EMA lenta
        body = (df["close"] - df["open"]).abs()
        rng_range = df["high"] - df["low"]
        df["body_ratio"] = body / (rng_range + 1e-9)
        df["ext_ratio"] = (df["close"] - df["ema_slow"]) / (df["atr"] + 1e-9)
        return df

    # ----------------------------------------------------------- evaluación ---
    def _time_filter_ok(self, df):
        """True si hora UTC de la última vela NO cae en la ventana prohibida."""
        if not self.time_filter_enabled:
            return True
        if isinstance(df.index, pd.DatetimeIndex):
            hour_utc = df.index[-1].hour
            ok = not (self.time_filter_start <= hour_utc < self.time_filter_end)
            return ok
        return True  # sin timestamps no se puede filtrar: no bloquear

    def _btc_corr_ok(self, df):
        """True si NO vamos contra la tendencia de BTC con alta correlación."""
        if not self.btc_corr_filter:
            return True
        try:
            btc_df = self.get_klines(interval="1h", limit=self.btc_corr_lookback,
                                     symbol="BTCUSDT", verbose=False)
            if len(btc_df) <= 10:
                return True
            last = df.iloc[-1]
            btc_ema = btc_df["close"].ewm(span=self.ema_slow, adjust=False).mean().iloc[-1]
            btc_trend = 1 if float(btc_df["close"].iloc[-1]) > float(btc_ema) else -1
            pair_trend = 1 if float(last["close"]) > float(df["ema_fast"].iloc[-1]) else -1
            if pair_trend == btc_trend:
                return True  # a favor de BTC: pase
            btc_returns = btc_df["close"].pct_change().dropna()
            pair_returns = df["close"].pct_change().dropna()
            min_len = min(len(btc_returns), len(pair_returns), self.btc_corr_lookback)
            if min_len < 10:
                return True
            corr = float(btc_returns.tail(min_len).corr(pair_returns.tail(min_len)))
            if pd.notna(corr) and abs(corr) > self.btc_corr_threshold:
                return False  # alta correlación y en contra de BTC: filtrar
        except Exception:
            pass
        return True

    def _vol_confirm_ok(self):
        """Confirmación de volumen en timeframe superior (vol_confirm_tf)."""
        if not self.vol_confirm_tf or self.vol_confirm_factor <= 0:
            return True
        try:
            htf_df = self.get_klines(interval=self.vol_confirm_tf, limit=30, verbose=False)
            if len(htf_df) <= 20:
                return True
            htf_vol_mean = float(htf_df["volume"].rolling(20).mean().iloc[-1])
            htf_last_vol = float(htf_df["volume"].iloc[-1])
            if htf_last_vol < htf_vol_mean * self.vol_confirm_factor:
                return False
        except Exception:
            pass
        return True

    def evaluate_signal(self, df):
        """Laya System 1: señal rápida + filtro anti-churn. Devuelve (señal, precio, fuerzaL, fuerzaS, atr, body, ext)."""
        if df is None or len(df) < 2:
            return "NONE", None, 0.0, 0.0, 0.0, 0.0, 0.0
        last = df.iloc[-1]
        prev = df.iloc[-2]
        for col in ["close", "ema_fast", "ema_slow", "rsi", "volume_ratio"]:
            if pd.isna(last[col]):
                return "NONE", None, 0.0, 0.0, 0.0, 0.0, 0.0
        close = float(last["close"])
        ema_f = float(last["ema_fast"])
        ema_s = float(last["ema_slow"])
        rsi = float(last["rsi"])
        vol_ratio = float(last["volume_ratio"])
        atr = float(last["atr"]) if "atr" in last and pd.notna(last["atr"]) else 0.0
        body_ratio = float(last["body_ratio"]) if "body_ratio" in last and pd.notna(last["body_ratio"]) else 0.0
        ext_ratio = float(last["ext_ratio"]) if "ext_ratio" in last and pd.notna(last["ext_ratio"]) else 0.0

        # Filtros avanzados
        time_ok = self._time_filter_ok(df)
        btc_ok = self._btc_corr_ok(df)
        vol_ok = self._vol_confirm_ok()

        # Anti-churn: extensión máxima y cuerpo mínimo
        max_ext_atr = float(self.strategy.get("max_ext_atr", 0.0))
        min_body = float(self.strategy.get("min_body_ratio", 0.0))
        long_candle = close > float(prev["close"])
        long_ext = not (max_ext_atr > 0 and atr > 0 and ext_ratio > max_ext_atr)
        long_body = not (min_body > 0 and body_ratio < min_body)
        short_candle = close < float(prev["close"])
        short_ext = not (max_ext_atr > 0 and atr > 0 and (-ext_ratio) > max_ext_atr)
        short_body = not (min_body > 0 and body_ratio < min_body)

        long_ema = ema_f > ema_s
        long_rsi = 50 < rsi < 70
        long_vol = vol_ratio > self.vol_factor
        short_ema = ema_f < ema_s
        short_rsi = 30 < rsi < 50
        short_vol = vol_ratio > self.vol_factor

        long_cond = (long_ema and long_rsi and long_vol and long_candle
                     and long_ext and long_body and time_ok and btc_ok and vol_ok)
        short_cond = (short_ema and short_rsi and short_vol and short_candle
                      and short_ext and short_body and time_ok and btc_ok and vol_ok)

        if (long_ema and long_rsi and long_vol and long_candle) and not long_cond:
            log(f"FILTRO bloqueó LONG: time_ok={time_ok}, btc_ok={btc_ok}, vol_ok={vol_ok}, "
                f"ext={ext_ratio:.2f} (max {max_ext_atr}), body={body_ratio:.2f} (min {min_body})")
        if (short_ema and short_rsi and short_vol and short_candle) and not short_cond:
            log(f"FILTRO bloqueó SHORT: time_ok={time_ok}, btc_ok={btc_ok}, vol_ok={vol_ok}, "
                f"ext={ext_ratio:.2f} (max {max_ext_atr}), body={body_ratio:.2f} (min {min_body})")

        strength_long = sum([
            long_ema, long_rsi, long_vol, long_candle,
            long_ext, long_body, time_ok, btc_ok, vol_ok,
        ]) / 9.0
        strength_short = sum([
            short_ema, short_rsi, short_vol, short_candle,
            short_ext, short_body, time_ok, btc_ok, vol_ok,
        ]) / 9.0

        signal = "NONE"
        if long_cond:
            signal = "LONG"
        elif short_cond:
            signal = "SHORT"
        return signal, close, strength_long, strength_short, atr, body_ratio, ext_ratio

    # ------------------------------------------------------------ posiciones ---
    def _market_open_allowed(self, pair, interval_minutes):
        """Verifica límites de apertura (pérdida diaria, máxima posición, cooldown, cash)."""
        if self.daily_realized <= -abs(self.max_daily_loss):
            return False, "Pérdida diaria alcanzada (max_daily_loss_usd)"
        if len(self.open_positions) >= self.max_open:
            return False, "Límite de posiciones abiertas (max_trades_open)"
        remaining = self._cooldown_remaining(pair, interval_minutes)
        if remaining > 0:
            return False, f"Cooldown activo ({remaining:.0f}s restantes)"
        size_needed = self.max_pos * (1 + self.fee_pct)
        if size_needed > self.cash:
            return False, f"Cash insuficiente (${self.cash:.2f} < ${size_needed:.2f})"
        return True, ""

    def _open_position(self, pair, side, price, atr=0.0, sl_factor=1.0, tp_factor=1.0):
        """Abre una posición simulada (paper/testnet-accounting) con SL/TP + parámetros de trailing."""
        if price is None or price <= 0:
            return None
        interval_minutes = interval_to_minutes(self.cfg.get("timeframe", "5m"))
        ok, reason = self._market_open_allowed(pair, interval_minutes)
        if not ok:
            log(f"ENTRADA BLOQUEADA ({pair}): {reason}")
            return None

        side = side.upper()
        if side == "LONG":
            sl = price * (1 - self.sl_pct * sl_factor)
            tp = price * (1 + self.tp_pct * tp_factor)
            trail_activation_price = price * (1 + self.trail_activation_pct)
        else:
            sl = price * (1 + self.sl_pct * sl_factor)
            tp = price * (1 - self.tp_pct * tp_factor)
            trail_activation_price = price * (1 - self.trail_activation_pct)

        qty = self.max_pos / price
        size_usd = qty * price
        fee_in = size_usd * self.fee_pct
        self.cash -= (size_usd + fee_in)

        pos = {
            "pair": pair,
            "side": side,
            "entry": price,
            "sl": sl,
            "tp": tp,
            "qty": qty,
            "size_usd": size_usd,
            "atr_at_entry": atr if atr > 0 else 0.0,
            "trail_activation_price": trail_activation_price,
            "trail_active": False,
            "trail_sl": None,
            "highest_price": price if side == "LONG" else None,
            "lowest_price": price if side == "SHORT" else None,
            "opened_at": datetime.now(timezone.utc).isoformat(),
        }
        self.open_positions[pair] = pos

        log(f"SEÑAL ABIERTA: {side} {pair} | Precio: {price:.2f} | Qty: {qty:.6f} | "
            f"SL: {sl:.2f} | TP: {tp:.2f} | Pos mín: ${self.max_pos} | Fee in: ${fee_in:.4f}")
        log(f"  Trailing: activación {self.trail_activation_pct*100:.1f}% | mult {self.trail_atr_mult} ATR | "
            f"paso {self.trail_step_atr} ATR | dist máx {self.trail_max_pct*100:.1f}%")
        if side == "SHORT" and not self.strategy.get("futures_paper", False):
            log("NOTA: SHORT simulado (spot no permite shorts reales). Activar futures_paper/futuros reales.")
        if self.mode == "live":
            if self.testnet:
                log("NOTA: LIVE-TESTNET contabilidad simulada. Colocación de órdenes AÚN NO implementada; solo señales.")
            else:
                log("ADVERTENCIA: modo LIVE-MAINNET con dinero real y órdenes NO implementadas. Usa testnet=true.")
        self.add_task("trade", f"Posición {side} {pair} abierta @ {price:.2f}")
        return {
            "signal": side,
            "pair": pair,
            "entry": round(price, 6),
            "sl": round(sl, 2),
            "tp": round(tp, 2),
            "qty": round(qty, 6),
            "size_usd": round(size_usd, 2),
            "trail_activation_price": round(trail_activation_price, 2),
            "trail_atr_mult": self.trail_atr_mult,
            "trail_step_atr": self.trail_step_atr,
            "trail_max_pct": self.trail_max_pct,
            "opened_at": pos["opened_at"],
        }

    def _close_position(self, pair, exit_price, reason):
        """Cierra una posición, aplica comisiones, actualiza PnL/cash y registra el trade."""
        pos = self.open_positions.pop(pair, None)
        if not pos:
            return None
        side = pos["side"]
        entry = pos["entry"]
        qty = pos["qty"]
        size_usd = pos["size_usd"]

        exit_notional = qty * exit_price
        if side == "LONG":
            gross = exit_notional - (qty * entry)
        else:
            gross = (qty * entry) - exit_notional
        fee_out = exit_notional * self.fee_pct
        fee_in = size_usd * self.fee_pct
        pnl = gross - fee_in - fee_out

        self.realized_pnl += pnl
        self.daily_realized += pnl
        # Devolución de cash (con fee ya aplicada a cada lado)
        if side == "LONG":
            cash_delta = exit_notional - fee_out
        else:
            cash_delta = size_usd + gross - fee_out
        self.cash += cash_delta

        pnl_pct = (pnl / size_usd * 100) if size_usd else 0
        trade = {
            "id": len(self.history) + 1,
            "pair": pair,
            "side": side,
            "entry": entry,
            "exit": round(exit_price, 2),
            "qty": round(qty, 6),
            "size_usd": round(size_usd, 2),
            "fees": round(fee_in + fee_out, 4),
            "pnl": round(pnl, 4),
            "pnl_pct": round(pnl_pct, 2),
            "reason": reason,
            "opened_at": pos.get("opened_at"),
            "closed_at": datetime.now(timezone.utc).isoformat(),
        }
        self.history.append(trade)
        sign = "+" if pnl >= 0 else ""
        self.add_task("trade", f"Posición {side} {pair} cerrada ({reason}): PnL {sign}{pnl:.2f} ({pnl_pct:+.2f}%)")

        # Cooldown (largo si el trade fue pérdida)
        self.last_close_ts[pair] = time.time()

        self.save_backtest_log({
            "timestamp": trade["closed_at"],
            "type": "closed_trade",
            **trade,
        })
        return trade

    def manage_positions(self, df, pair=None):
        """Actualiza trailing stops de posiciones abiertas y cierra por SL/TP/trailing."""
        pair = pair or self.pair
        pos = self.open_positions.get(pair)
        if not pos or df is None or len(df) == 0:
            return
        last = df.iloc[-1]
        close = float(last["close"])
        atr_row = float(last["atr"]) if "atr" in last and pd.notna(last["atr"]) else 0.0
        atr = max(atr_row, float(pos.get("atr_at_entry", 0.0)))
        side = pos["side"]
        entry = pos["entry"]
        sl = pos["sl"]
        tp = pos["tp"]

        # Precio extremo alcanzado
        if side == "LONG" and (pos.get("highest_price") is None or close > pos["highest_price"]):
            pos["highest_price"] = close
        if side == "SHORT" and (pos.get("lowest_price") is None or close < pos["lowest_price"]):
            pos["lowest_price"] = close

        # Exits fijos (SL/TP)
        if side == "LONG":
            if close >= tp:
                self._close_position(pair, tp, "TP alcanzado")
                return
            if close <= sl:
                self._close_position(pair, sl, "SL alcanzado")
                return
        else:
            if close <= tp:
                self._close_position(pair, tp, "TP alcanzado")
                return
            if close >= sl:
                self._close_position(pair, sl, "SL alcanzado")
                return

        # Trailing stop:
        # activación cuando el precio avanza trail_activation_pct a favor; distancia
        # = trail_atr_mult * ATR, limitada por trail_max_pct del precio; el paso
        # mínimo para mover el SL es trail_step_atr * ATR (reduce ruido).
        trail_dist_cap = close * self.trail_max_pct
        atr_dist = atr * self.trail_atr_mult
        dist = min(atr_dist, trail_dist_cap)
        min_step = self.trail_step_atr * atr

        if side == "LONG":
            if not pos.get("trail_active") and close >= pos.get("trail_activation_price", entry * (1 + self.trail_activation_pct)):
                pos["trail_active"] = True
                log(f"Trailing ACTIVADO (LONG) {pair} @ {close:.2f}")
            if pos.get("trail_active") and dist > 0:
                new_trail = close - dist
                if pos.get("trail_sl") is None or (new_trail - pos["trail_sl"]) >= min_step:
                    pos["trail_sl"] = new_trail
                    log(f"Trailing SL {pair}: {new_trail:.2f} (ATR={atr:.2f}, mult={self.trail_atr_mult})")
            if pos.get("trail_sl") is not None and close <= pos["trail_sl"]:
                self._close_position(pair, pos["trail_sl"], "Trailing SL")
                return
        else:
            if not pos.get("trail_active") and close <= pos.get("trail_activation_price", entry * (1 - self.trail_activation_pct)):
                pos["trail_active"] = True
                log(f"Trailing ACTIVADO (SHORT) {pair} @ {close:.2f}")
            if pos.get("trail_active") and dist > 0:
                new_trail = close + dist
                if pos.get("trail_sl") is None or pos["trail_sl"] - new_trail >= max(min_step, 0):
                    pos["trail_sl"] = new_trail
                    log(f"Trailing SL {pair}: {new_trail:.2f} (ATR={atr:.2f}, mult={self.trail_atr_mult})")
            if pos.get("trail_sl") is not None and close >= pos["trail_sl"]:
                self._close_position(pair, pos["trail_sl"], "Trailing SL")
                return

    # ------------------------------------------------------------ estado sim ---
    def _get_sim_performance_summary(self):
        """Lee sim_state.json y genera resumen de rendimiento para few-shot learning."""
        try:
            if not os.path.exists("sim_state.json"):
                return ""
            with open("sim_state.json", "r", encoding="utf-8") as f:
                data = json.load(f)
            sims = data.get("sims", {})
            parts = []
            for sid, sim in sims.items():
                name = sim.get("name", sid)
                pnl = sim.get("realized_pnl", 0.0)
                history = sim.get("history", [])
                trades = len(history)
                wins = sum(1 for t in history if float(t.get("pnl", 0)) > 0)
                wr = (wins / trades * 100) if trades > 0 else 0
                parts.append(f"{name}: PnL ${pnl:+.2f}, WR {wr:.1f}%, {trades}t")
            if not parts:
                return ""
            return "- Datos históricos del simulador: " + "; ".join(parts) + "."
        except Exception:
            return ""

    def save_backtest_log(self, result):
        log_path = "backtest_log.json"
        try:
            logs = []
            if os.path.exists(log_path):
                with open(log_path, "r", encoding="utf-8") as f:
                    try:
                        logs = json.load(f)
                        if not isinstance(logs, list):
                            logs = [logs]
                    except Exception:
                        logs = []
            logs.append(result)
            with open(log_path, "w", encoding="utf-8") as f:
                json.dump(logs, f, indent=2, ensure_ascii=False)
        except Exception as e:
            log(f"Error guardando backtest: {e}")

    # --------------------------------------------------------------- ciclo ---
    def run_cycle(self):
        """Un ciclo: klines → indicadores → gestionar posiciones → señal → deep filtro → abrir."""
        self._daily_reset()
        result_full = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "type": "cycle",
            "pair": self.pair,
            "capital": self.capital,
            "signal_laya": "NONE",
            "price": None,
            "deep_analysis": None,
            "filter_applied": False,
            "note": "",
        }
        try:
            timeframe = self.cfg.get("timeframe", "5m")
            df = self.get_klines(interval=timeframe)
            df = self.calculate_indicators(df)
            last = df.iloc[-1]

            # Gestionar posiciones abiertas (trailing stops, SL/TP)
            self.manage_positions(df, pair=self.pair)

            # Evaluar señal System 1 con todos los filtros anti-churn
            signal_laya, price, strength_long, strength_short, atr, body_ratio, ext_ratio = self.evaluate_signal(df)

            # Contexto fundamental (cacheado, API públicas sin claves)
            context = get_context(self.pair)

            prev_close = float(df.iloc[-2]["close"])
            price_info = {
                "pair": self.pair,
                "price": price if price is not None else float(last["close"]),
                "ema_fast": float(last["ema_fast"]),
                "ema_slow": float(last["ema_slow"]),
                "rsi": float(last["rsi"]),
                "volume_ratio": float(last["volume_ratio"]),
                "candle_direction": "alcista" if float(last["close"]) > prev_close else "bajista",
                "timeframe": timeframe,
                "capital": self.capital,
                "feed": "spot" if self.live_client else "synthetic",
                "atr": float(last["atr"]) if pd.notna(last["atr"]) else "N/A",
                "body_ratio": body_ratio,
                "max_ext_atr": float(self.strategy.get("max_ext_atr", 0.0)),
                "ext_ratio": ext_ratio,
                "min_body": float(self.strategy.get("min_body_ratio", 0.0)),
                "signal": signal_laya,
                "strength_long": strength_long,
                "strength_short": strength_short,
                "context": context,
                "strategy": {
                    **self.strategy,
                    "sl_pct": self.sl_pct,
                    "tp_pct": self.tp_pct,
                },
                "max_position_usd": self.max_pos,
                "max_daily_loss_usd": self.max_daily_loss,
                "sim_performance": self._get_sim_performance_summary(),
            }

            # System 2: análisis profundo (LLM)
            deep_result = None
            if self.deep and self.deep.enabled:
                try:
                    start_deep = time.time()
                    deep_result = self.deep.analyze(price_info)
                    deep_latency = (time.time() - start_deep) * 1000
                    price_info["_deep_latency_ms"] = deep_latency
                    if deep_result:
                        log(f"ANALISIS PROFUNDO ({deep_result.get('_model', '?')}): "
                            f"recomendacion={deep_result.get('recomendacion_laya', 'N/A')} "
                            f"confianza={deep_result.get('confianza', 0):.2f} ({deep_latency:.0f}ms) — "
                            f"{deep_result.get('razon_corta', '')}")
                    else:
                        log("ANALISIS PROFUNDO fallo (respuesta nula).")
                except Exception as e:
                    log(f"ERROR análisis profundo: {e}")
                    deep_result = {"recomendacion_laya": "WAIT", "razon_corta": f"Error: {e}", "confianza": 0.0}
            result_full["deep_analysis"] = deep_result

            result_full["signal_laya"] = signal_laya
            result_full["price"] = price

            # Filtro de entradas del System 2
            if self.deep and self.deep.enabled and self.deep.use_filter and deep_result:
                rec = deep_result.get("recomendacion_laya", "WAIT")
                if rec in ("ABSTAIN", "WAIT") and signal_laya != "NONE":
                    log(f"FILTRO DEEP APLICADO: Laya quería {signal_laya}, Deep recomienda {rec}. Se anula.")
                    signal_laya = "NONE"
                    result_full["filter_applied"] = True
                    result_full["note"] = f"Deep recomendó {rec}; señal Laya anulada."
                elif rec == "LONG" and signal_laya == "LONG":
                    result_full["note"] = "Deep confirma LONG; se mantiene señal."
                elif rec == "SHORT" and signal_laya == "SHORT":
                    result_full["note"] = "Deep confirma SHORT; se mantiene señal."
                else:
                    result_full["note"] = f"Deep: {rec}. Laya: {signal_laya}."

            # Ajuste opcional de SL/TP
            if self.deep and self.deep.enabled and self.deep.use_adjust and deep_result:
                adj = deep_result.get("ajuste_sl_tp", {})
                if isinstance(adj, dict):
                    result_full["deep_sl_factor"] = float(adj.get("sl_factor", 1.0))
                    result_full["deep_tp_factor"] = float(adj.get("tp_factor", 1.0))

            log(f"Estado Laya: señal={signal_laya} EMAf={float(last['ema_fast']):.2f} "
                f"EMAs={float(last['ema_slow']):.2f} RSI={float(last['rsi']):.2f} "
                f"VolRatio={float(last['volume_ratio']):.2f} StrL={strength_long:.2f} StrS={strength_short:.2f}")

            if signal_laya in ("LONG", "SHORT") and price is not None:
                sl_factor = float(result_full.get("deep_sl_factor", 1.0))
                tp_factor = float(result_full.get("deep_tp_factor", 1.0))
                trade = self._open_position(self.pair, signal_laya, price, atr=atr,
                                            sl_factor=sl_factor, tp_factor=tp_factor)
                if trade:
                    result_full.update(trade)
                    if sl_factor != 1.0 or tp_factor != 1.0:
                        result_full["note"] = (result_full.get("note", "") +
                            f" | SL/TP ajustados por Deep: slx{sl_factor:.2f}, tpx{tp_factor:.2f}")
                else:
                    result_full["note"] = result_full.get("note", "") + " | Entrada bloqueada por límites de riesgo."
            else:
                result_full["note"] = (result_full.get("note", "") +
                    (" | Laya: NONE." if signal_laya == "NONE" else " | Sin precio válido."))

            # Equity (cash + positions abiertas a precio de mercado)
            unreal = 0.0
            for p, pos_obj in self.open_positions.items():
                px = price if p == self.pair and price is not None else pos_obj["entry"]
                if pos_obj["side"] == "LONG":
                    unreal += (px - pos_obj["entry"]) * pos_obj["qty"]
                else:
                    unreal += (pos_obj["entry"] - px) * pos_obj["qty"]
            result_full["cash"] = round(self.cash, 2)
            result_full["equity"] = round(self.cash + unreal, 2)
            result_full["realized_pnl"] = round(self.realized_pnl, 4)
            result_full["daily_realized"] = round(self.daily_realized, 4)
            result_full["open_positions"] = len(self.open_positions)

            self.save_backtest_log(result_full)
            return result_full
        except Exception as e:
            log(f"ERROR en ciclo: {e}")
            result_full["note"] = f"ERROR: {e}"
            self.save_backtest_log(result_full)
            return result_full

    def run_forever(self, tick_seconds=None):
        tick = tick_seconds if tick_seconds is not None else float(self.cfg.get("tick_interval", 2.0))
        log(f"Bucle continuo iniciado (cada {tick}s). Ctrl+C para detener.")
        try:
            while True:
                self.run_cycle()
                time.sleep(tick)
        except KeyboardInterrupt:
            log("Detenido por el usuario.")


if __name__ == "__main__":
    cfg = load_config()
    bot = LayaSystemOneBot(cfg)
    if "--loop" in sys.argv:
        bot.run_forever()
    else:
        bot.run_cycle()
