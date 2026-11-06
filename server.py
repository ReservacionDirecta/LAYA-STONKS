#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Laya STONKS — Servidor multi-agente en tiempo real (cada "simulador" es un agente de trading independiente).

Modelo:
  - Varios SIMULADORES independientes en paralelo, cada uno con su propio saldo.
  - Cada simulador puede operar 1 activo (simple) o VARIOS activos compartiendo
    la misma cartera (multi-activo): ese simulador usa un único saldo (p. ej. $20).
  - El saldo inicial de cada simulador se puede cambiar (mientras esté detenido).
  - Todos los simuladores se actualizan en el mismo bucle (en paralelo lógico).

Rutas:
  GET  /                    -> dashboard.html
  GET  /api/state           -> snapshot del simulador activo + lista de simuladores
  GET  /api/config          -> configuración saneada
  GET  /api/context         -> contexto fundamental (F&G, noticias, macro, MTF)
  GET  /api/stream          -> SSE
  POST /api/backtest        -> {pair, timeframe, velas, kind} (velas reales, paper)
  POST /api/control         -> {action: start|pause|stop|reset}  (simulador activo)
  POST /api/pair            -> {pair}                            (par activo del simulador activo)
  POST /api/timeframe       -> {timeframe}                       (global)
  POST /api/sim/select      -> {id}
  POST /api/sim/create      -> {name, capital, pairs}
  POST /api/sim/delete      -> {id}
  POST /api/sim/control     -> {id, action}
  POST /api/sim/balance     -> {id, capital}

Persistencia:
  El estado de todos los simuladores (estado start/pause/stop, saldo, posiciones,
  historial, tareas y velas de TODOS sus pares) se guarda en sim_state.json:
  cada 10 s, tras cada accion de control y al cerrar el servidor. De modo que
  si se recarga la pagina o se reinicia el servidor, una simulacion iniciada
  sigue iniciada conservando su progreso.
"""

import atexit
import os
import copy
import json
import random
import re
import threading
import time
import xml.etree.ElementTree as ET
from datetime import datetime, timezone

from flask import Flask, Response, jsonify, request, send_from_directory, stream_with_context, make_response

import requests

# --- System 2: Deep Analysis (LLM) ---
try:
    from deep_analysis import DeepAnalysisEngine
    DEEP_ANALYSIS_AVAILABLE = True
except Exception as e:
    DeepAnalysisEngine = None
    DEEP_ANALYSIS_AVAILABLE = False
    print(f"[DEEP] Módulo no disponible: {e}")

CONFIG_FILE = "config.json"
PORT = int(os.environ.get("PORT", "5050"))
TICK_INTERVAL = 2.0
MAX_CANDLES = 800
MAX_TASKS = 200
MAX_HISTORY = 200

# --- Binance Testnet (datos reales, dinero ficticio) ------------------------
# 'spot'    -> Binance Spot Testnet      https://testnet.binance.vision
# 'futures' -> Binance Futuros USDⓈ-M Testnet  https://testnet.binancefuture.com
# Ambos sirven velas REALES (mismo formato de klines) y NO requieren claves
# para datos de mercado. La cartera del dashboard SIEMPRE es paper/simulada;
# contra ninguno de los dos se colocan órdenes reales.
TN_KINDS = {
    "spot": {
        "base": "https://testnet.binance.vision/api",
        "prefix": "/v3",
        "label": "Binance Spot Testnet",
    },
    "futures": {
        "base": "https://testnet.binancefuture.com",
        "prefix": "/fapi/v1",
        "label": "Binance Futuros USDⓈ-M Testnet",
    },
    "real": {
        "base": "https://data-api.binance.vision/api",
        "prefix": "/v3",
        "label": "Binance mercado real (solo lectura de datos)",
    },
}
TESTNET_POLL_INTERVAL = 10.0
# Ninguna de las dos testnets soporta velas de 5s/15s; esos periodos permanecen simulados.
BINANCE_TIMEFRAMES = {"1m": "1m", "5m": "5m", "15m": "15m", "1h": "1h", "4h": "4h", "1D": "1d"}

_tn_session = requests.Session()
_tn_session.headers.update({"User-Agent": "LayaSTONKS/1.0"})


def tn_http_get(kind, path, params=None):
    cfg = TN_KINDS.get(kind) or TN_KINDS["spot"]
    url = cfg["base"] + path
    r = _tn_session.get(url, params=params or {}, timeout=10)
    if r.status_code != 200:
        raise RuntimeError("HTTP " + str(r.status_code) + ": " + r.text[:120])
    return r.json()


def tn_supported_symbols(kind="spot"):
    cfg = TN_KINDS.get(kind) or TN_KINDS["spot"]
    info = tn_http_get(kind, cfg["prefix"] + "/exchangeInfo")
    out = set()
    for item in info.get("symbols", []):
        if item.get("status") != "TRADING":
            continue
        if kind == "futures" and item.get("contractType") != "PERPETUAL":
            continue
        out.add(item.get("symbol"))
    return out


def tn_klines(kind, symbol, interval, limit):
    """Klines REALES de testnet (spot o futuros) -> velas del formato interno."""
    cfg = TN_KINDS.get(kind) or TN_KINDS["spot"]
    raw = tn_http_get(kind, cfg["prefix"] + "/klines",
                      {"symbol": symbol, "interval": interval, "limit": int(limit)})
    out = []
    for k in raw:
        try:
            out.append({
                "time": int(k[0] // 1000),
                "open": round_price(float(k[1])),
                "high": round_price(float(k[2])),
                "low": round_price(float(k[3])),
                "close": round_price(float(k[4])),
                "volume": round(float(k[5]), 2),
            })
        except Exception:
            continue
    return out


# --- Histórico para backtest: velas reales sin claves ------------------------
# 1a fuente: la testnet elegida (spot / futures), paginando hacia atrás.
# 2a fuente (fallback): data-api.binance.vision, velas reales de spot de
# producción sin claves (mismas klines que vería un trader). En ninguna de las
# dos se colocan órdenes: SOLO son datos.

BACKTEST_MAX_VELAS = 5000


def _klines_convert(raw):
    out = []
    for k in raw or []:
        try:
            out.append({
                "time": int(k[0] // 1000),
                "open": round_price(float(k[1])),
                "high": round_price(float(k[2])),
                "low": round_price(float(k[3])),
                "close": round_price(float(k[4])),
                "volume": round(float(k[5]), 2),
            })
        except Exception:
            continue
    return out


def _klines_page(base, prefix, symbol, interval, limit, end_ms=None):
    params = {"symbol": symbol, "interval": interval, "limit": int(limit)}
    if end_ms:
        params["endTime"] = int(end_ms)
    r = _tn_session.get(base + prefix + "/klines", params=params, timeout=10)
    if r.status_code != 200:
        raise RuntimeError("HTTP " + str(r.status_code) + ": " + r.text[:110])
    return r.json()


def fetch_klines_hist(kind, symbol, interval, total=1000):
    """Velas históricas reales, SIN claves: pagina hacia atrás (endTime) en la
    testnet elegida (tope 1000 spot / 1500 futuros por llamada) y completa con
    data-api.binance.vision si la testnet no cubre el rango pedido. Devuelve
    (velas ascendentes, etiqueta de origen)."""
    try:
        total = int(total)
    except Exception:
        total = 1000
    total = max(70, min(total, BACKTEST_MAX_VELAS))
    cfg = TN_KINDS.get(kind) or TN_KINDS["spot"]
    sources = [
        (cfg["label"], cfg["base"], cfg["prefix"], 1500 if kind == "futures" else 1000),
        ("data-api.binance.vision (spot real)", "https://data-api.binance.vision", "/v3", 1000),
    ]
    need = min(total, 100)          # mínimo útil de una fuente antes de probar el fallback
    errors = []
    best = None
    for label, base, prefix, per_call in sources:
        try:
            out = {}
            end_ms = None
            while len(out) < total:
                page = _klines_page(base, prefix, symbol, interval,
                                    min(per_call, total - len(out)) if len(out) < total else 1,
                                    end_ms)
                got = _klines_convert(page)
                if not got:
                    break
                for c in got:
                    out[c["time"]] = c
                oldest = min(c["time"] for c in got)
                if end_ms is not None and oldest * 1000 >= end_ms:
                    break          # la API ya no tiene más historial antiguo
                end_ms = oldest * 1000 - 1
            if out:
                candles = [out[t] for t in sorted(out)][-total:]
                if len(candles) >= need:
                    return candles, label
                if best is None or len(candles) > len(best[0]):
                    best = (candles, label)
                errors.append(label + ": solo " + str(len(candles)) + " velas")
        except Exception as e:
            errors.append(label + ": " + str(e)[:70])
    if best:
        return best
    raise RuntimeError("; ".join(errors) if errors else "sin velas disponibles")


# --- Vista de gráfico con temporalidades cortas (5s..1D) para el dashboard ---
# NOTA: esto es SOLO visualización (independiente de la temporalidad con la
# que el agente opera). Las temporalidades sub-minuto se construyen agregando
# velas reales de 1s (spot las soporta de forma nativa; si la testnet elegida
# es futuros y no cubre 1s, se usa data-api.binance.vision como fuente).
CHART_TFS = {
    "5s": 5, "15s": 15, "30s": 30,
    "1m": 60, "5m": 300, "15m": 900, "30m": 1800,
    "1h": 3600, "4h": 14400, "1D": 86400,
}
# intervalo nativo de la API para cada temporalidad del gráfico; int = velas 1s
CHART_BASE_INTERVAL = {"5s": "", "15s": "", "30s": "",
                       "1m": "1m", "5m": "5m", "15m": "15m", "30m": "30m",
                       "1h": "1h", "4h": "4h", "1D": "1d"}


def _chart_aggregate(base, sec):
    """Agrega velas base (1s) en buckets de `sec` segundos (5s/15s/30s).
    La última vela queda en formación: se recalcula en cada refresco."""
    out = {}
    for c in base:
        t = (int(c["time"]) // sec) * sec
        e = out.get(t)
        if not e:
            out[t] = {"time": t, "open": c["open"], "high": c["high"],
                      "low": c["low"], "close": c["close"], "volume": c["volume"]}
        else:
            if c["high"] > e["high"]:
                e["high"] = c["high"]
            if c["low"] < e["low"]:
                e["low"] = c["low"]
            e["close"] = c["close"]
            e["volume"] = round(e["volume"] + c["volume"], 4)
    return [out[t] for t in sorted(out)]


def _chart_fetch_tail(kind, pair, interval, start_ms, limit):
    """Velas del testnet elegido con startTime (para refresco incremental)."""
    cfg = (TN_KINDS.get(kind) if kind else None) or TN_KINDS["spot"]
    params = {"symbol": pair, "interval": interval, "limit": int(limit)}
    if start_ms:
        params["startTime"] = int(start_ms)
    r = _tn_session.get(cfg["base"] + cfg["prefix"] + "/klines",
                        params=params, timeout=8)
    if r.status_code != 200:
        raise RuntimeError("HTTP " + str(r.status_code))
    return _klines_convert(r.json())


# --- Capa de contexto (fundamental en tiempo real, 100% gratuita) ------------
# TTLs de refresco del daemon de contexto (se lanza SOLO en main()).
FNG_TTL = 600.0      # Fear & Greed (alternative.me, sin claves): 10 min
GLOBAL_TTL = 600.0   # Macro global (CoinGecko /global): 10 min
NEWS_TTL = 300.0     # Titulares RSS (CoinDesk / Cointelegraph): 5 min
MTF_TTL = 300.0      # Sesgo multi-temporal por par (candles 1h): 5 min

# Léxico de titulares (inglés: es el idioma dominante del flujo cripto).
NEWS_BULL = ("rally", "surge", "soar", "gain", "bullish", "jump", "climb", "upside",
             "breakout", "inflow", "adoption", "record", "rebound", "recover",
             "all-time high", "ath")
NEWS_BEAR = ("crash", "plunge", "slump", "bearish", "fall", "sink", "tumble", "hack",
             "exploit", "ban", "crackdown", "outflow", "dump", "bloodbath", "lawsuit",
             "fraud", "bankruptcy")


def fng_label_es(v):
    """Etiqueta en español del Fear & Greed Index (0-100)."""
    try:
        v = int(v)
    except Exception:
        v = 50
    if v <= 24:
        return "Miedo extremo"
    if v <= 44:
        return "Miedo"
    if v <= 54:
        return "Neutral"
    if v <= 74:
        return "Codicia"
    return "Codicia extrema"


def score_news_titles(titles):
    """Puntaje de sentimiento -100..+100 para titulares RSS (léxico con límites
    de palabra). Grueso pero honesto: se documenta como señal de agitación, no
    de dirección fina. Sin titulares -> 0 (neutral)."""
    if not titles:
        return 0.0
    pos = neg = 0
    for t in titles:
        tl = str(t).lower()
        p = sum(1 for w in NEWS_BULL if re.search(r"\b" + re.escape(w) + r"\b", tl))
        n = sum(1 for w in NEWS_BEAR if re.search(r"\b" + re.escape(w) + r"\b", tl))
        if p > n:
            pos += 1
        elif n > p:
            neg += 1
    total = pos + neg
    if not total:
        return 0.0
    return round(max(-100.0, min(100.0, (pos - neg) / total * 100.0)), 1)


def sentiment_class(score):
    if score <= -25.0:
        return "Pesimista"
    if score >= 25.0:
        return "Optimista"
    return "Neutral"


def mtf_trend_from_candles(candles, ema_period=21, rsi_period=14):
    """Sesgo de temporalidad MAYOR (confluencia) sobre velas ya descargadas:
    EMA9 vs EMA21 y posición del precio; RSI como apoyo. Puro: sin red."""
    closes = [c["close"] for c in candles][-300:]
    if len(closes) < max(ema_period + 1, 6):
        return {"trend": "SIN DATOS", "rsi": None}
    ef = ema_series(closes, 9)[-1]
    es = ema_series(closes, ema_period)[-1]
    rsi = rsi_series(closes, rsi_period)[-1]
    px = closes[-1]
    if ef is None or es is None:
        return {"trend": "SIN DATOS", "rsi": None}
    if ef > es and px >= es:
        trend = "ALCISTA"
    elif ef < es and px <= es:
        trend = "BAJISTA"
    else:
        trend = "LATERAL"
    return {"trend": trend, "rsi": (round(rsi, 1) if rsi is not None else None)}


PAIRS = {
    "BTCUSDT":  ("Bitcoin",   65000.0),
    "ETHUSDT":  ("Ethereum",   3200.0),
    "BNBUSDT":  ("BNB",         580.0),
    "XRPUSDT":  ("Ripple",        0.62),
    "SOLUSDT":  ("Solana",      150.0),
    "ADAUSDT":  ("Cardano",       0.45),
    "DOGEUSDT": ("Dogecoin",      0.12),
    "LINKUSDT": ("Chainlink",      14.0),
    "AVAXUSDT": ("Avalanche",      28.0),
    "LTCUSDT":  ("Litecoin",       85.0),
    "DOTUSDT":  ("Polkadot",        6.5),
    "TRXUSDT":  ("TRON",           0.13),
}

TIMEFRAMES = {
    "5s": 5, "15s": 15, "1m": 60, "5m": 300, "15m": 900,
    "1h": 3600, "4h": 14400, "1D": 86400,
}


# ----------------------------------------------------------------------------
# Utilidades
# ----------------------------------------------------------------------------

def now_iso():
    return datetime.now(timezone.utc).isoformat()


def load_config(path=CONFIG_FILE):
    cfg_data = {}
    try:
        with open(path, "r", encoding="utf-8") as f:
            cfg_data = json.load(f)
    except Exception:
        cfg_data = {}

    # Defaults robustos para System 2 (Gemini / NVIDIA) si no están definidos
    da = cfg_data.setdefault("deep_analysis", {})
    if not da.get("provider"):
        da["provider"] = "google"
    if not da.get("model"):
        da["model"] = "gemini-3.5-flash-lite"
    if not da.get("api_key_env"):
        da["api_key_env"] = "GEMINI_API_KEY"
    if not da.get("endpoint"):
        da["endpoint"] = "https://generativelanguage.googleapis.com/v1beta/models/gemini-3.5-flash-lite:generateContent"
    if "enabled" not in da:
        da["enabled"] = True
    if "use_for_filter" not in da:
        da["use_for_filter"] = True
    return cfg_data


def save_config(cfg_obj, path=CONFIG_FILE):
    """Guarda config.json de forma atómica (tmp + replace)."""
    try:
        tmp = path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(cfg_obj, f, ensure_ascii=False, indent=2)
        os.replace(tmp, path)
        return True
    except Exception as e:
        print("Advertencia: no se pudo guardar config.json:", e)
        return False


# --- Estrategia de Laya System 1 (pulido del pulso + salidas estratégicas) ---
# Valores por defecto; config.json los sobreescribe con su bloque "strategy".
STRATEGY_DEFAULTS = {
    "tp1_pct": 0.025,          # movimiento a favor que dispara TP1 (part de precio)
    "tp1_size_pct": 0.35,      # 35% en TP1: paga comisión + aire; 65% vive para el TP grande
    "break_even": True,        # tras TP1, SL sube a la entrada (sin pérdida)
    "trail_atr_mult": 2.0,     # trailing = px favorito - ATR*mult (0 desactiva)
    "momentum_exit": False,    # señal contraria NO cierra la posición (churn -0.50 en 6 cierres)
    "time_stop_bars": 0,       # cerrar tras N barras sin SL/TP (0 desactiva)
    "cooldown_bars": 4,        # 4 barras de espera tras cerrar (20 min en 5m); con pérdida se multiplica
    "short_enabled": True,     # permitir SHORT (feed de futuros O paper-futuros)
    "futures_paper": False,     # SHORT paper apalancado sin necesitar feed de futuros (sigue en paper)
    "leverage": 5,             # apalancamiento en SHORT/ futuros (1 = spot)
    # --- Anti-churn / anti-persecución (afinado con el diagnóstico de efectividad) ---
    "momentum_grace_bars": 0,   # sin salida-momentum en las primeras N barras de la posición (0 desactiva)
    "momentum_min_strength": 0, # fuerza mínima (%) de la señal contraria para salir (0 usa la base 75%)
    "cooldown_loss_mult": 2.5,  # post-SL: cooldown x2.5 (BNB re-entró 2x en el mismo suelo $689-691)
    "max_ext_atr": 1.25,        # "tendencia fresca" vs tardía: >1.25×ATR de la EMA21 = tarde, no perseguir
    "min_body_ratio": 0.0,      # exige cuerpo real de vela: |cierre-apertura|/rango >= N (0 desactiva)
    "sl_slip_atr": 0.0,         # deslizamiento máximo del SL/trailing en ATR (0 = relleno exacto al stop)
    "ctx_filter": False,        # con F&G extremo (>=80 o <=10) solo entra si la MTF acompaña
    "maker_entry": False,       # entrada límite post-only (Maker real Binance: reduce comisiones)
    # --- Capa cuantitativa (targets escala-vol + score de calidad) ---
    # Objetivos dinámicos: SL = sl_atr_mult * ATR (acotado a [min,max] %),
    # TP = distancia SL × tp_rr y TP1 a la fracción tp1_rr_frac del TP.
    # Sustituye a los % fijos que, en barras de 5m reales, quedan a decenas
    # de ATR de distancia (todas las operaciones morían en el stop de tiempo).
    "tp_sl_mode": "atr",        # "atr" = SL/TP escalados por volatilidad | "fixed" = % fijos
    "sl_atr_mult": 1.6,         # SL 1.6×ATR: 1.3-1.4 era ruido de 5m (34 SL de mecha en 271 trades)
    "tp_rr": 2.5,               # ratio riesgo/beneficio: TP = SL × tp_rr
    "tp1_rr_frac": 0.45,        # TP1 se coloca a esa fracción de la distancia del TP
    "min_sl_pct": 0.0035,       # suelo del SL en % del precio (evita stops sub-comisión)
    "max_sl_pct": 0.025,        # techo del SL en % del precio
    # Score cuantitativo (0-100): tendencia + momentum + RSI centrado +
    # volumen + régimen de volatilidad. Señal válida solo si score >= umbral:
    # opera menos pero mejor (menos churn → menos comisiones sin velas muertas).
    "min_signal_score": 70,     # umbral estricto: entra menos pero mejor (antes 55 → churn)
    # Rango de RSI por dirección (Mejora 1: tendencia fresca en vez de techo/suelo):
    # LONG 52-64 evita comprar en 68-69 antes del agotamiento;
    # SHORT 36-48 evita vender en 31-33 cuando el rebote ya está cerca.
    "long_rsi_min": 52.0,
    "long_rsi_max": 64.0,
    "short_rsi_min": 36.0,
    "short_rsi_max": 48.0,
    # --- Motor de riesgo formal (checks pre-trade fail-fast) ---
    "risk_dd_block_pct": 6.0,   # dd de la curva sobre el capital que bloquea NUEVAS entradas (0 = off)
    "risk_reserve_pct": 10.0,   # colchón mínimo de cash (% del capital) para abrir (0 = off)
    "risk_max_exposure": 3.0,   # nocional total máximo en múltiplos del capital (0 = off)
}


def sanitize_strategy(st):
    out = dict(STRATEGY_DEFAULTS)
    if isinstance(st, dict):
        for k in out:
            if k not in st:
                continue
            v = st[k]
            try:
                if isinstance(out[k], bool):
                    out[k] = bool(v)
                elif isinstance(out[k], int):
                    out[k] = int(v)
                else:
                    out[k] = float(v)
            except Exception:
                pass
    out["leverage"] = max(1, min(int(out["leverage"]), 50))
    out["cooldown_bars"] = max(0, int(out["cooldown_bars"]))
    out["tp1_size_pct"] = max(0.05, min(float(out["tp1_size_pct"]), 0.9))
    out["trail_atr_mult"] = max(0.0, float(out["trail_atr_mult"]))
    out["time_stop_bars"] = max(0, min(int(out["time_stop_bars"]), 500))
    out["momentum_grace_bars"] = max(0, min(int(out["momentum_grace_bars"]), 500))
    out["momentum_min_strength"] = max(0.0, min(float(out["momentum_min_strength"]), 100.0))
    out["cooldown_loss_mult"] = max(1.0, min(float(out["cooldown_loss_mult"]), 10.0))
    out["max_ext_atr"] = max(0.0, min(float(out["max_ext_atr"]), 10.0))
    out["min_body_ratio"] = max(0.0, min(float(out["min_body_ratio"]), 1.0))
    out["sl_slip_atr"] = max(0.0, min(float(out["sl_slip_atr"]), 10.0))
    # Capa cuantitativa
    if out.get("tp_sl_mode") not in ("atr", "fixed"):
        out["tp_sl_mode"] = "atr"
    out["sl_atr_mult"] = max(0.2, min(float(out["sl_atr_mult"]), 10.0))
    out["tp_rr"] = max(0.5, min(float(out["tp_rr"]), 10.0))
    out["tp1_rr_frac"] = max(0.1, min(float(out["tp1_rr_frac"]), 1.0))
    out["min_sl_pct"] = max(0.0005, min(float(out["min_sl_pct"]), 0.1))
    out["max_sl_pct"] = max(0.001, min(float(out["max_sl_pct"]), 0.5))
    if out["min_sl_pct"] > out["max_sl_pct"]:
        out["min_sl_pct"], out["max_sl_pct"] = out["max_sl_pct"], out["min_sl_pct"]
    out["min_signal_score"] = max(0.0, min(float(out["min_signal_score"]), 100.0))
    # Rangos RSI por dirección (0-100, min < max; si se cruzan, fallback seguro)
    out["long_rsi_min"] = max(0.0, min(float(out["long_rsi_min"]), 100.0))
    out["long_rsi_max"] = max(0.0, min(float(out["long_rsi_max"]), 100.0))
    out["short_rsi_min"] = max(0.0, min(float(out["short_rsi_min"]), 100.0))
    out["short_rsi_max"] = max(0.0, min(float(out["short_rsi_max"]), 100.0))
    if out["long_rsi_min"] >= out["long_rsi_max"]:
        out["long_rsi_min"], out["long_rsi_max"] = STRATEGY_DEFAULTS["long_rsi_min"], STRATEGY_DEFAULTS["long_rsi_max"]
    if out["short_rsi_min"] >= out["short_rsi_max"]:
        out["short_rsi_min"], out["short_rsi_max"] = STRATEGY_DEFAULTS["short_rsi_min"], STRATEGY_DEFAULTS["short_rsi_max"]
    # Motor de riesgo
    out["risk_dd_block_pct"] = max(0.0, min(float(out["risk_dd_block_pct"]), 50.0))
    out["risk_reserve_pct"] = max(0.0, min(float(out["risk_reserve_pct"]), 80.0))
    out["risk_max_exposure"] = max(0.0, min(float(out["risk_max_exposure"]), 50.0))
    return out


def _fnum01(d, key, default):
    try:
        return max(0.0, min(float(d.get(key, default)), 0.01))
    except Exception:
        return default


def sanitize_config(cfg):
    fees = cfg.get("fees") if isinstance(cfg.get("fees"), dict) else {}
    ctx_b = cfg.get("context") if isinstance(cfg.get("context"), dict) else {}
    return {
        "mode": cfg.get("mode", "paper"),
        "pair": cfg.get("pair", "BTCUSDT"),
        "capital_usd": cfg.get("capital_usd", 20),
        "max_position_usd": cfg.get("max_position_usd", 5),
        "sl_pct": cfg.get("sl_pct", 0.025),
        "tp_pct": cfg.get("tp_pct", 0.05),
        "max_daily_loss_usd": cfg.get("max_daily_loss_usd", 2),
        "timeframe": cfg.get("timeframe", "5m"),
        "tick_interval": cfg.get("tick_interval", TICK_INTERVAL),
        "max_trades_open": cfg.get("max_trades_open", 1),
        "indicators": cfg.get("indicators", {}),
        "strategy": sanitize_strategy(cfg.get("strategy", {})),
        "fees": {
            "enabled": bool(fees.get("enabled", fees.get("fees_enabled", True))),
            "spot_pct": _fnum01(fees, "spot_pct", 0.001),
            "futures_pct": _fnum01(fees, "futures_pct", 0.0005),
            "maker_spot_pct": _fnum01(fees, "maker_spot_pct", 0.001),
            "maker_futures_pct": _fnum01(fees, "maker_futures_pct", 0.0002),
        },
        "context": {
            "enabled": bool(ctx_b.get("enabled", ctx_b.get("ctx_enabled", True))),
            "filter": bool(ctx_b.get("filter", ctx_b.get("ctx_filter", False))),
        },
        "deep_analysis": {
            "enabled": cfg.get("deep_analysis", {}).get("enabled", False),
            "provider": cfg.get("deep_analysis", {}).get("provider", ""),
            "model": cfg.get("deep_analysis", {}).get("model", ""),
        },
        "preset": cfg.get("preset", "standard"),
    }


# --- Parámetros editables en vivo desde el dashboard ------------------------
# clave editable -> (tipo, min, máx). Se reciben planas en el payload y se
# re-mapean a su bloque en config.json (ver CONFIG_BLOCK) y a su clave dentro
# del dict params compartido (ver CONFIG_PARAM, para las que difieren).
# ----------------------------------------------------------------------------
# Presets de comportamiento agentico (Conservador, Estandar, Agresivo)
# ----------------------------------------------------------------------------
AGENTIC_PRESETS = {
    "conservative": {
        "name": "Conservador (Swing / Alta precision)",
        "description": "Trades de alta calidad, mayor confirmacion, SL/TP holgados y comisiones reducidas (Maker).",
        "params": {
            "min_signal_score": 70.0,
            "sl_atr_mult": 1.6,
            "tp_rr": 2.5,
            "tp1_rr_frac": 0.45,
            "leverage": 1,
            "momentum_grace_bars": 8,
            "cooldown_bars": 4,
            "cooldown_loss_mult": 3.0,
            "max_ext_atr": 1.25,
            "min_body_ratio": 0.35,
            "trail_atr_mult": 2.5,
            "risk_reserve_pct": 15.0,
            "risk_dd_block_pct": 5.0,
            "min_sl_pct": 0.005,
            "max_sl_pct": 0.035,
            "ctx_filter": True,
            "maker_entry": True,
            "tp_sl_mode": "atr",
        }
    },
    "standard": {
        "name": "Estandar (Equilibrado / Laya 1x)",
        "description": "Balance optimo entre frecuencia y proteccion, apalancamiento seguro 1x y comisiones Maker.",
        "params": {
            "min_signal_score": 70.0,
            "sl_atr_mult": 1.6,
            "tp_rr": 2.5,
            "tp1_rr_frac": 0.5,
            "leverage": 1,
            "momentum_grace_bars": 7,
            "cooldown_bars": 4,
            "cooldown_loss_mult": 2.5,
            "max_ext_atr": 1.25,
            "min_body_ratio": 0.25,
            "trail_atr_mult": 2.0,
            "risk_reserve_pct": 10.0,
            "risk_dd_block_pct": 8.0,
            "min_sl_pct": 0.0035,
            "max_sl_pct": 0.025,
            "ctx_filter": True,
            "maker_entry": True,
            "tp_sl_mode": "atr",
        }
    },
    "aggressive": {
        "name": "Agresivo (Scalp rapido / Alta frecuencia)",
        "description": "Entradas mas dinamicas con targets mas cenidos y proteccion anti-ruido.",
        "params": {
            "min_signal_score": 70.0,
            "sl_atr_mult": 1.6,
            "tp_rr": 2.5,
            "tp1_rr_frac": 0.5,
            "leverage": 2,
            "momentum_grace_bars": 4,
            "cooldown_bars": 4,
            "cooldown_loss_mult": 2.5,
            "max_ext_atr": 1.25,
            "min_body_ratio": 0.20,
            "trail_atr_mult": 1.6,
            "risk_reserve_pct": 5.0,
            "risk_dd_block_pct": 12.0,
            "min_sl_pct": 0.0025,
            "max_sl_pct": 0.02,
            "ctx_filter": True,
            "maker_entry": True,
            "tp_sl_mode": "atr",
        }
    },
    "high_alpha": {
        "name": "High-Alpha ($50 Max Profit / Low Risk)",
        "description": "Configuracion optimizada para $50 USD: R:R 2.4, TP1 al 70%, Break-Even rapido, filtro estricto MTF 1h y apalancamiento 3x.",
        "params": {
            "min_signal_score": 70.0,
            "sl_atr_mult": 1.6,
            "tp_rr": 2.5,
            "tp1_rr_frac": 0.65,
            "tp1_size_pct": 0.35,
            "break_even": True,
            "leverage": 3,
            "momentum_grace_bars": 7,
            "momentum_min_strength": 85,
            "cooldown_bars": 4,
            "cooldown_loss_mult": 2.5,
            "max_ext_atr": 1.25,
            "min_body_ratio": 0.25,
            "trail_atr_mult": 1.8,
            "risk_reserve_pct": 10.0,
            "risk_dd_block_pct": 10.0,
            "min_sl_pct": 0.003,
            "max_sl_pct": 0.022,
            "ctx_filter": True,
            "maker_entry": True,
            "tp_sl_mode": "atr",
            "compound_pct": 40.0,
            "close_pos_filter": True,
        }
    }
}

CONFIG_SPEC = {
    # Riesgo
    "sl_pct":             ("float", 0.001, 0.5),
    "tp_pct":             ("float", 0.001, 2.0),
    "max_position_usd":   ("float", 0.5, 10000.0),
    "max_daily_loss_usd": ("float", 0.1, 10000.0),
    "max_trades_open":    ("int",   1, 50),
    # Velocidad y capital
    "capital_usd":        ("float", 1.0, 100000.0),
    "tick_interval":      ("float", 0.2, 60.0),
    # Indicadores
    "ema_fast":           ("int",   2, 200),
    "ema_slow":           ("int",   2, 400),
    "rsi_period":         ("int",   2, 100),
    "volume_factor":      ("float", 0.1, 10.0),
    # Estrategia (salidas/entradas)
    "tp1_pct":            ("float", 0.001, 1.0),
    "tp1_size_pct":       ("float", 0.05, 0.9),
    "break_even":         ("bool",  None, None),
    "trail_atr_mult":     ("float", 0.0, 20.0),
    "momentum_exit":      ("bool",  None, None),
    "time_stop_bars":     ("int",   0, 500),
    "cooldown_bars":      ("int",   0, 500),
    "short_enabled":      ("bool",  None, None),
    "futures_paper":      ("bool",  None, None),
    "leverage":           ("int",   1, 50),
    # Anti-churn / anti-persecución
    "momentum_grace_bars":   ("int",   0,   500),
    "momentum_min_strength": ("float", 0.0, 100.0),
    "cooldown_loss_mult":    ("float", 1.0, 10.0),
    "max_ext_atr":           ("float", 0.0, 10.0),
    "min_body_ratio":        ("float", 0.0, 1.0),
    "sl_slip_atr":           ("float", 0.0, 10.0),
    # Capa cuantitativa
    "min_signal_score":      ("float", 0.0, 100.0),
    "long_rsi_min":          ("float", 0.0, 100.0),
    "long_rsi_max":          ("float", 0.0, 100.0),
    "short_rsi_min":         ("float", 0.0, 100.0),
    "short_rsi_max":         ("float", 0.0, 100.0),
    # Motor de riesgo formal
    "risk_dd_block_pct":     ("float", 0.0, 50.0),
    "risk_reserve_pct":      ("float", 0.0, 80.0),
    "risk_max_exposure":     ("float", 0.0, 50.0),
    # System 2 (Gemini deep analysis) — toggle en vivo sin reiniciar
    "deep_analysis_enabled": ("bool",  None, None),
    # Comisiones reales Binance (fracciones: 0.001 = 0.1% spot, 0.0005 = 0.05% futuros)
    "fees_enabled":          ("bool",  None, None),
    "spot_pct":              ("float", 0.0, 0.01),
    "futures_pct":           ("float", 0.0, 0.01),
    # Contexto fundamental
    "ctx_filter":            ("bool",  None, None),
    # Targets cuantitativos y ejecucion Maker
    "maker_entry":          ("bool",  None, None),
    "tp_sl_mode":          ("str",   None, None),
    "sl_atr_mult":         ("float", 0.2, 10.0),
    "tp_rr":               ("float", 0.5, 10.0),
    "tp1_rr_frac":         ("float", 0.1, 1.0),
    "min_sl_pct":          ("float", 0.001, 0.1),
    "max_sl_pct":          ("float", 0.005, 0.2),
    "compound_pct":        ("float", 0.0, 100.0),
    "close_pos_filter":    ("bool",  None, None),
}

# Bloque de config.json donde se persiste cada clave editable (None = raíz).
CONFIG_BLOCK = {
    "sl_pct": None, "tp_pct": None, "max_position_usd": None,
    "max_daily_loss_usd": None, "max_trades_open": None,
    "capital_usd": None, "tick_interval": None,
    "ema_fast": "indicators", "ema_slow": "indicators",
    "rsi_period": "indicators", "volume_factor": "indicators",
    "tp1_pct": "strategy", "tp1_size_pct": "strategy", "break_even": "strategy",
    "trail_atr_mult": "strategy", "momentum_exit": "strategy",
    "time_stop_bars": "strategy", "cooldown_bars": "strategy",
    "short_enabled": "strategy", "futures_paper": "strategy", "leverage": "strategy",
    "momentum_grace_bars": "strategy", "momentum_min_strength": "strategy",
    "cooldown_loss_mult": "strategy", "max_ext_atr": "strategy", "min_body_ratio": "strategy",
    "sl_slip_atr": "strategy",
    "min_signal_score": "strategy",
    "long_rsi_min": "strategy", "long_rsi_max": "strategy",
    "short_rsi_min": "strategy", "short_rsi_max": "strategy",
    "risk_dd_block_pct": "strategy",
    "risk_reserve_pct": "strategy",
    "risk_max_exposure": "strategy",
    "fees_enabled": "fees", "spot_pct": "fees", "futures_pct": "fees",
    "ctx_filter": "context",
    "maker_entry": "strategy", "tp_sl_mode": "strategy", "sl_atr_mult": "strategy",
    "tp_rr": "strategy", "tp1_rr_frac": "strategy", "min_sl_pct": "strategy", "max_sl_pct": "strategy",
    "compound_pct": "strategy", "close_pos_filter": "strategy",
}

# Nombre real dentro del dict params compartido (para las claves que difieren).
CONFIG_PARAM = {
    "max_position_usd": "max_pos",
    "max_daily_loss_usd": "max_daily_loss",
    "max_trades_open": "max_open",
    "volume_factor": "vol_factor",
    "capital_usd": "capital0",
    "spot_pct": "fee_spot",
    "futures_pct": "fee_futs",
}


def round_price(p):
    if p is None:
        return None
    p = float(p)
    if p >= 1000:
        return round(p, 2)
    if p >= 1:
        return round(p, 4)
    return round(p, 6)


# ----------------------------------------------------------------------------
# Indicadores
# ----------------------------------------------------------------------------

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
    n = len(closes)
    if n <= period:
        return out
    gains, losses = [], []
    for i in range(1, n):
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


def rolling_mean(values, window):
    out = []
    acc = 0.0
    for i, v in enumerate(values):
        acc += v
        if i >= window:
            acc -= values[i - window]
        out.append(acc / min(i + 1, window))
    return out


def atr_value(candles, period=14):
    """ATR medio (True Range) de las últimas `period` velas; None si no hay datos."""
    n = len(candles)
    if n < period + 1:
        return None
    trs = []
    for i in range(n - period, n):
        c, pc = candles[i], candles[i - 1]
        try:
            tr = max(c["high"] - c["low"],
                     abs(c["high"] - pc["close"]),
                     abs(c["low"] - pc["close"]))
        except Exception:
            return None
        trs.append(tr)
    return sum(trs) / len(trs)


# ----------------------------------------------------------------------------
# Mercado de un par (velas + indicadores + señal, sin cartera)
# ----------------------------------------------------------------------------

class Market:
    def __init__(self, pair, params):
        self.pair = pair
        self.params = params
        self.base = PAIRS[pair][1]
        self.rng = random.Random()
        self.feed = "synthetic"   # 'synthetic' (generado) | 'spot' | 'futures' (velas reales)
        self.candles = []
        self.price = self.base
        self.trend = 0.0
        self.atr = None
        self.indicators = {}
        self.diagnostics = {}
        self.signal = "NONE"
        self.signal_note = "Agente detenido. Pulsa Activar."
        self._ef = []
        self._es = []
        self._rsi = []
        self.seed()
        self.recompute()

    def seed(self):
        tf = self.params["tf_seconds"]
        self.candles = []
        self.price = self.base
        self.trend = 0.0
        t = int(time.time()) - 140 * tf
        price = self.base
        for _ in range(140):
            o = price
            price = self.advance(price)
            c = price
            hi = max(o, c) * (1.0 + self.rng.uniform(0.0002, 0.0012))
            lo = min(o, c) * (1.0 - self.rng.uniform(0.0002, 0.0012))
            self.candles.append({
                "time": t, "open": round_price(o), "high": round_price(hi),
                "low": round_price(lo), "close": round_price(c),
                "volume": round(self.rng.uniform(90, 300), 2),
            })
            t += tf
        self.price = self.candles[-1]["close"]

    def advance(self, prev, vol=1.0):
        """Un paso del mercado. `vol` escala la volatilidad (usado al
        regenerar el historial tras cambiar de temporalidad)."""
        r = self.rng.random()
        if r < 0.04:
            self.trend = self.rng.choice([-1, 1]) * self.rng.uniform(0.0010, 0.0020) * vol
        elif r < 0.08:
            self.trend = 0.0
        drift = self.trend
        noise = self.rng.gauss(0, 0.0009 * vol)
        jump = 0.0
        if self.rng.random() < 0.012:
            jump = self.rng.choice([-1, 1]) * self.rng.uniform(0.0025, 0.006) * vol
        return max(prev * 0.01, prev * (1.0 + drift + noise + jump))

    def recompute(self):
        closes = [c["close"] for c in self.candles]
        vols = [c["volume"] for c in self.candles]
        if not closes:
            self.indicators = {}
            return
        self._ef = ema_series(closes, self.params["ema_fast"])
        self._es = ema_series(closes, self.params["ema_slow"])
        self._rsi = rsi_series(closes, self.params["rsi_period"])
        vm = rolling_mean(vols, 20)
        last = len(closes) - 1
        atr = atr_value(self.candles, self.params["rsi_period"])
        # --- Cuantitativo: serie ATR lima (True Range, media simple) para
        #     medir el RÉGIMEN de volatilidad: percentil del ATR actual dentro
        #     de las últimas 200 barras. Detección de cinta muerta (mecha plana
        #     donde solo se pagan comisiones) y de expansión de volatilidad.
        self._atr_series = []
        for i in range(1, len(self.candles)):
            c, pc = self.candles[i], self.candles[i - 1]
            try:
                tr = max(float(c["high"]) - float(c["low"]),
                         abs(float(c["high"]) - float(pc["close"])),
                         abs(float(c["low"]) - float(pc["close"])))
            except Exception:
                continue
            self._atr_series.append(tr)
        self._atr_series = rolling_mean(self._atr_series, self.params["rsi_period"])
        self._atr_rank = None
        s_atr = [x for x in self._atr_series[-200:] if x]
        cur_atr = self._atr_series[-1] if self._atr_series else None
        if s_atr and cur_atr and cur_atr > 0:
            self._atr_rank = sum(1 for x in s_atr if x <= cur_atr) / len(s_atr)
        self.atr = atr
        self.indicators = {
            "ema_fast": round_price(self._ef[last]),
            "ema_slow": round_price(self._es[last]),
            "rsi": None if self._rsi[last] is None else round(self._rsi[last], 2),
            "volume": round(vols[last], 2),
            "volume_mean": round(vm[last], 2),
            "volume_ratio": round(vols[last] / (vm[last] + 1e-9), 3),
            "atr": round(atr, 4) if atr is not None else None,
        }

    def evaluate(self):
        ind = self.indicators
        if len(self.candles) < 2:
            self.diagnostics = {}
            return "NONE"
        ema_f = ind.get("ema_fast", 0)
        ema_s = ind.get("ema_slow", 0)
        rsi = ind.get("rsi")
        vr = ind.get("volume_ratio", 0)
        last = self.candles[-1]["close"]
        prev = self.candles[-2]["close"]
        l_ema = ema_f > ema_s
        # Rango de RSI por dirección (afinado en vivo: LONG 52-64 evita comprar
        # el techo 68-69; SHORT 36-48 evita vender el suelo 31-33 pre-rebote).
        l_min = float(self.params.get("long_rsi_min", STRATEGY_DEFAULTS["long_rsi_min"]) or 0.0)
        l_max = float(self.params.get("long_rsi_max", STRATEGY_DEFAULTS["long_rsi_max"]) or 100.0)
        s_min = float(self.params.get("short_rsi_min", STRATEGY_DEFAULTS["short_rsi_min"]) or 0.0)
        s_max = float(self.params.get("short_rsi_max", STRATEGY_DEFAULTS["short_rsi_max"]) or 100.0)
        l_rsi = rsi is not None and l_min < rsi < l_max
        l_vol = vr > self.params["vol_factor"]
        l_candle = last > prev
        s_ema = ema_f < ema_s
        s_rsi = rsi is not None and s_min < rsi < s_max
        s_vol = vr > self.params["vol_factor"]
        s_candle = last < prev
        # --- Anti-persecución: no perseguir movimientos extendidos ni entradas
        # sobre velas de mecha (sin cuerpo real). Solo bloquean si están
        # configurados (>0); por defecto quedan neutros (ext_ok/body_ok True).
        atr = self.atr
        max_ext = float(self.params.get("max_ext_atr", STRATEGY_DEFAULTS["max_ext_atr"]) or 0.0)
        body_min = float(self.params.get("min_body_ratio", STRATEGY_DEFAULTS["min_body_ratio"]) or 0.0)
        ext_ratio = None
        body_ratio = None
        ext_l = ext_s = body_l = body_s = False
        if max_ext > 0 and atr and atr > 0:
            ext_ratio = (float(last) - float(ema_s)) / float(atr)
            ext_l = ext_ratio > max_ext        # LONG tras celo: demasiado lejos ARRIBA de la EMA21
            ext_s = (-ext_ratio) > max_ext     # SHORT en caída: demasiado lejos ABAJO
        if body_min > 0:
            c0 = self.candles[-1]
            rng = float(c0.get("high", 0.0) or 0.0) - float(c0.get("low", 0.0) or 0.0)
            if rng > 0:
                body_ratio = abs(float(c0.get("close", 0.0) or 0.0) - float(c0.get("open", 0.0) or 0.0)) / rng
                if body_ratio < body_min:
                    body_l = body_s = True     # vela de mecha/doji: señal de baja calidad en ambos lados

        # --- Filtro anti-trampas de cierre de vela (rejection location) ---
        # Si close_position_filter está activo (o en high_alpha):
        # Para LONG el cierre debe estar en el 55% superior del rango de la vela (fuerza compradora).
        # Para SHORT el cierre debe estar en el 55% inferior del rango (fuerza vendedora).
        close_loc_l = close_loc_s = False
        if bool(self.params.get("close_pos_filter", True)):
            c0 = self.candles[-1]
            rng = float(c0.get("high", 0.0) or 0.0) - float(c0.get("low", 0.0) or 0.0)
            if rng > 0:
                rel_pos = (float(c0.get("close", 0.0) or 0.0) - float(c0.get("low", 0.0) or 0.0)) / rng
                if rel_pos < 0.45:
                    close_loc_l = True  # LONG rechazado: cerró en la mitad inferior
                if rel_pos > 0.55:
                    close_loc_s = True  # SHORT rechazado: cerró en la mitad superior

        self.diagnostics = {
            "volume_factor": self.params["vol_factor"],
            "rsi": rsi,
            "volume_ratio": vr,
            "ext_atr": max_ext,
            "body_min": body_min,
            "ext_ratio": None if ext_ratio is None else round(ext_ratio, 2),
            "body_ratio": None if body_ratio is None else round(body_ratio, 2),
            "long": {"ema_ok": l_ema, "rsi_ok": l_rsi, "vol_ok": l_vol, "candle_ok": l_candle,
                     "ext_ok": not ext_l, "body_ok": not body_l, "close_ok": not close_loc_l},
            "short": {"ema_ok": s_ema, "rsi_ok": s_rsi, "vol_ok": s_vol, "candle_ok": s_candle,
                      "ext_ok": not ext_s, "body_ok": not body_s, "close_ok": not close_loc_s},
        }
        # Fuerza de la señal: fracción de condiciones duras cumplidas por
        # dirección (EMA, RSI, vela). El volumen NO entra: en testnet su ratio
        # es crónicamente bajo y la fuerza quedaría clavada en ≤75%.
        self.diagnostics["strength"] = {
            "long": round(sum([l_ema, l_rsi, l_candle]) / 3.0, 2),
            "short": round(sum([s_ema, s_rsi, s_candle]) / 3.0, 2),
        }

        # --- Score cuantitativo (0-100) por dirección ------------------------
        # Componentes: tendencia (pendiente EMA9-EMA21 en ATR), momentum del
        # precio sobre la EMA rápida, RSI centrado en su zona óptima (60/40),
        # volumen relativo y régimen de volatilidad (percentil ATR). La señal
        # solo se admite si su score >= min_signal_score: esto descarta las
        # entradas de cinta muerta que terminaban pagando comisión en el
        # stop de tiempo (diagnóstico de la efectividad de la flota).
        def _cl(x):
            return max(0.0, min(1.0, x))

        score_l = score_s = None
        if atr and atr > 0:
            slope = (ema_f - ema_s) / atr          # >0 tendencia alcista
            momo = (last - ema_f) / atr            # >0 impulso sobre la EMA rápida
            r50 = float(rsi) if rsi is not None else 50.0
            reg = 0.15 + 0.85 * (self._atr_rank if self._atr_rank is not None else 0.5)
            reg = _cl(reg)
            volq = _cl((vr - 1.0) / 1.2)
            # En feeds reales de TESTNET el libro es artificial y volume_ratio
            # queda crónicamente bajo (≈0.3): el volumen no es informativo, así
            # que su componente pasa a neutral (0.5) y NUNCA bloquea señales.
            if self.feed in ("spot", "futures"):
                volq = 0.5

            def _raw(long_dir):
                return (25.0 * _cl(slope / 1.0 if long_dir else -slope / 1.0)
                        + 20.0 * _cl(momo / 0.8 if long_dir else -momo / 0.8)
                        + 15.0 * _cl(1.0 - abs(r50 - (60.0 if long_dir else 40.0)) / 12.0)
                        + 20.0 * volq
                        + 20.0 * reg)

            # Normalizado al máximo alcanzable EN ESTA vela (volumen y régimen
            # fijos; tendencia/momentum/RSI son la parte libre): el score se
            # lee como "fracción de calidad disponible" 0-100, de modo que el
            # umbral min_signal_score no se vuelve inalcanzable en horas de
            # baja volatilidad (causa original del bloqueo de la flota).
            cap = 60.0 + 20.0 * volq + 20.0 * reg
            if cap < 40.0:
                cap = 40.0
            score_l = round(_raw(True) / cap * 100.0, 1)
            score_s = round(_raw(False) / cap * 100.0, 1)
        self.diagnostics["quant"] = {
            "long": score_l, "short": score_s,
            "atr_rank": None if self._atr_rank is None else round(self._atr_rank, 3),
            "atr_pct": (None if not atr else round(atr / last * 100.0, 4)) if last else None,
        }

        min_sc = float(self.params.get("min_signal_score",
                                       STRATEGY_DEFAULTS["min_signal_score"]) or 0.0)
        ok_l = (score_l is None or score_l >= min_sc)
        ok_s = (score_s is None or score_s >= min_sc)
        # El volumen ya NO es gate duro (en testnet volume_ratio es crónico bajo
        # y bloqueaba TODO): ahora aporta solo dentro del score; el bloqueo
        # duro lo componen tendencia + RSI + vela + extensión/cuerpo/cierre + umbral.
        if l_ema and l_rsi and l_candle and not ext_l and not body_l and not close_loc_l and ok_l:
            return "LONG"
        if s_ema and s_rsi and s_candle and not ext_s and not body_s and not close_loc_s and ok_s:
            return "SHORT"
        return "NONE"

    def tick(self):
        if self.feed != "synthetic":
            # Con feed real (spot o futuros) las velas las aporta el poller de
            # testnet; no generamos velas sinteticas aqui.
            return
        tf = self.params["tf_seconds"]
        last_t = self.candles[-1]["time"] if self.candles else int(time.time())
        t = last_t + tf
        o = self.price
        c = self.advance(self.price)
        hi = max(o, c) * (1.0 + self.rng.uniform(0.0001, 0.0008))
        lo = min(o, c) * (1.0 - self.rng.uniform(0.0001, 0.0008))
        vol = self.rng.uniform(70, 320) * self.rng.uniform(0.7, 1.5)
        self.candles.append({
            "time": t, "open": round_price(o), "high": round_price(hi),
            "low": round_price(lo), "close": round_price(c), "volume": round(vol, 2),
        })
        if len(self.candles) > MAX_CANDLES:
            self.candles = self.candles[-MAX_CANDLES:]
        self.price = self.candles[-1]["close"]
        self.recompute()
        self.signal = self.evaluate()
        self.signal_note = ("Sin señal válida (Laya System 1)." if self.signal == "NONE"
                            else "Señal " + self.signal + " detectada.")

    def retimestamp(self):
        tf = self.params["tf_seconds"]
        n = len(self.candles)
        if n == 0:
            return
        now = int(time.time())
        for i, c in enumerate(self.candles):
            c["time"] = now - (n - 1 - i) * tf

    def regenerate_tf(self):
        """Regenera el historial sintético al cambiar de temporalidad.
        Ancla la última vela al precio actual y escala la volatilidad con
        sqrt(tf/300), de modo que cada periodo se ve distinto (como en
        TradingView) en vez de solo reetiquetar los timestamps."""
        if self.feed != "synthetic" or not self.candles:
            return
        tf = self.params["tf_seconds"]
        vol = (tf / 300.0) ** 0.5          # sqrt(tf) respecto al periodo base 5m
        n = len(self.candles)
        anchor = self.price if self.price else self.base
        now = int(time.time())
        # Camino sintético nuevo con la volatilidad del periodo...
        self.trend = 0.0
        price = anchor
        raw = []
        for i in range(n):
            o = price
            price = self.advance(price, vol)
            c = price
            hi = max(o, c) * (1.0 + self.rng.uniform(0.0002, 0.0012) * vol)
            lo = min(o, c) * (1.0 - self.rng.uniform(0.0002, 0.0012) * vol)
            raw.append((o, hi, lo, c))
        # ...y lo re-anclamos: el cierre de la última vela vuelve al precio actual.
        k = anchor / (raw[-1][3] or anchor)
        self.candles = []
        for i, (o, hi, lo, c) in enumerate(raw):
            self.candles.append({
                "time": now - (n - 1 - i) * tf,
                "open": round_price(o * k), "high": round_price(hi * k),
                "low": round_price(lo * k), "close": round_price(c * k),
                "volume": round(self.rng.uniform(90, 300), 2),
            })
        self.price = self.candles[-1]["close"]
        self.recompute()
        self.signal = self.evaluate()
        self.signal_note = ("Sin señal válida (Laya System 1)." if self.signal == "NONE"
                            else "Señal " + self.signal + " detectada.")

    def apply_real_candles(self, new_candles):
        """Incorpora velas reales: agrega las nuevas y actualiza la vela
        parcial (sin cerrar) del feed real con cada poll. Devuelve True si cambió."""
        if not new_candles:
            return False
        changed = False
        for c in new_candles:
            if not self.candles or c["time"] > self.candles[-1]["time"]:
                self.candles.append(c)
                changed = True
            elif self.candles[-1]["time"] == c["time"] and self.candles[-1] != c:
                self.candles[-1] = c
                changed = True
        if len(self.candles) > MAX_CANDLES:
            self.candles = self.candles[-MAX_CANDLES:]
        self.price = self.candles[-1]["close"]
        self.recompute()
        self.signal = self.evaluate()
        self.signal_note = ("Sin señal válida (Laya System 1)." if self.signal == "NONE"
                            else "Señal " + self.signal + " detectada.")
        return changed

    def set_real_feed(self, candles, kind):
        """Reemplaza el historial por velas reales y activa el feed del kind."""
        if not candles or kind not in ("spot", "futures"):
            return False
        self.feed = kind
        self.candles = candles[-MAX_CANDLES:]
        self.price = self.candles[-1]["close"]
        self.recompute()
        self.signal = self.evaluate()
        self.signal_note = ("Sin señal válida (Laya System 1)." if self.signal == "NONE"
                            else "Señal " + self.signal + " detectada.")
        return True

    def seed_from_testnet(self, candles, kind="spot"):
        """Compatibilidad: siembra velas reales con el kind indicado."""
        return self.set_real_feed(candles, kind)

    def series(self):
        candles = self.candles[-MAX_CANDLES:]
        n = len(candles)
        ef = self._ef[-n:] if self._ef else []
        es = self._es[-n:] if self._es else []
        rsi = self._rsi[-n:] if self._rsi else []
        emaF, emaS, rsiS, volS = [], [], [], []
        for i, c in enumerate(candles):
            if i < len(ef):
                emaF.append({"time": c["time"], "value": round_price(ef[i])})
            if i < len(es):
                emaS.append({"time": c["time"], "value": round_price(es[i])})
            if i < len(rsi) and rsi[i] is not None:
                rsiS.append({"time": c["time"], "value": round(rsi[i], 2)})
            volS.append({"time": c["time"], "value": c["volume"],
                         "color": "#26a69a" if c["close"] >= c["open"] else "#ef5350"})
        return candles, emaF, emaS, rsiS, volS


# ----------------------------------------------------------------------------
# Simulador (cartera propia + mercados de sus pares)
# ----------------------------------------------------------------------------

class Simulator:
    def __init__(self, sid, name, capital, pairs, params):
        self.id = sid
        self.name = name
        # Copia por simulador: los overrides de preset (_apply_*_params) solo
        # afectan a este sim. Antes todos compartían el mismo dict y el último
        # preset aplicado contaminaba a toda la flota.
        self.params = dict(params)
        self._own = set()        # claves fijadas por preset (update_config live no las toca)
        self.capital = float(capital)
        self.cash = float(capital)
        self.realized = 0.0
        self.daily = 0.0
        self.daily_date = None   # fecha ISO del "PnL de hoy"; reset a medianoche
        self.status = "stopped"
        self.started_at = None
        self.cycle = 0
        self.pairs = list(pairs)
        self.mode = "multi" if len(pairs) > 1 else "single"
        self.active_pair = self.pairs[0]
        self.markets = {p: Market(p, self.params) for p in self.pairs}
        self.positions = {}
        self.history = []
        self.tasks = []
        self.cooldown = {}       # par -> barras restantes antes de re-entrar (display)
        self.cooldown_till = {}  # par -> time de vela hasta la que está bloqueado
        self._sig_note = {}      # anti-spam de avisos: 1 por barra y tipo
        self.equity_curve = []   # Intelligence Analytics: [(iso, equity)] por tick
        self.fees_paid = 0.0     # comisiones reales acumuladas (Binance VIP0)
        self.context = {}        # capa de contexto (llena por el Manager por referencia)
        self._ai_sl_tp_adjust = {} # ajustes dinamicos sugeridos por System 2 (IA)
        # --- Decision journal (pipeline SDO) ---
        # Registro de cada intento de señal (ejecutado o rechazado) con su
        # snapshot de features; el outcome (pnl/barras/motivo) se retroalimenta
        # al cerrar la posición, dándole datos al drift detection.
        self.journal = []
        self._journal_keys = set()   # dedupe de rechazos: (par|barra|fase|lado)
        self._drift_closes = 0       # trades ejecutados cerrados desde el último check

    @property
    def price(self):
        """Precio actual del par activo."""
        m = self.markets.get(self.active_pair)
        return m.price if m else 0.0

    @property
    def signal(self):
        """Señal actual del par activo."""
        m = self.markets.get(self.active_pair)
        return m.signal if m else "NONE"

    @property
    def market(self):
        """Mercado del par activo."""
        return self.markets.get(self.active_pair)

    def add_task(self, kind, msg, data=None):
        entry = {"time": now_iso(), "kind": kind, "message": msg, "data": data}
        self.tasks.insert(0, entry)
        if len(self.tasks) > MAX_TASKS:
            self.tasks = self.tasks[:MAX_TASKS]

    # -- helpers de estrategia --------------------------------------------
    def _st(self, key):
        return self.params.get(key, STRATEGY_DEFAULTS.get(key))

    def _futures_on(self):
        """SHORT con mecánica de futuros activo: feed real de futuros
        (data_source='futures') O paper-futuros (strategy.futures_paper)."""
        if self.params.get("futures_enabled"):
            return True
        return bool(self._st("futures_paper"))

    def _note_once(self, pair, key, kind, msg, gap=None):
        """Aviso limitado en tiempo real: como máximo 1 mensaje cada `gap`
        segundos por (par, tipo). El mercado sintético cambia de barra en cada
        tick, así que anclar el límite a la barra sería spam; se ancla al reloj."""
        gap = self.params.get("note_gap", 60.0) if gap is None else gap
        st_key = (pair, key)
        now = time.monotonic()
        last = self._sig_note.get(st_key)
        if last is not None and (now - last) < gap:
            return
        self._sig_note[st_key] = now
        self.add_task(kind, msg)

    # --- Decision journal + drift (pipeline SDO) --------------------------
    def _journal_summary(self):
        """Resumen ligero del journal para el snapshot/dashboard."""
        n_exec = n_rej = n_open = 0
        closed = wr_n = 0
        exp = 0.0
        for e in self.journal:
            if e.get("phase") == "executed":
                n_exec += 1
                if e.get("why") is None:
                    n_open += 1
                else:
                    closed += 1
                    exp += (e.get("pnl") or 0)
                    if (e.get("pnl") or 0) > 0:
                        wr_n += 1
            else:
                n_rej += 1
        return {
            "total": len(self.journal),
            "executed": n_exec, "rejected": n_rej, "open": n_open,
            "use": closed,
            "trades_en_drift": self._drift_closes,
            "use_until_check": max(0, DRIFT_EVERY_N - self._drift_closes),
            "w_rate": round(wr_n / closed * 100, 1) if closed else 0,
            "expectancy": round(exp / closed, 4) if closed else 0,
            "rejected_flat": n_rej,
        }

    def _journal(self, phase, pair, side, price=None, extra=None, dedupe=True):
        """Registra un intento de decisión en el journal. phase: 'executed'
        (posición abierta) o '<fase>' del rechazo (risk:*, cooldown, ctx,
        deep, maxopen). Con dedupe, 1 entrada por (par, vela, fase, lado).
        Devuelve el dict insertado (o None si el par no tiene mercado)."""
        m = self.markets.get(pair)
        if m is None:
            return None
        bar = (m.candles[-1]["time"] if m.candles else int(time.time()))
        key = pair + "|" + str(bar) + "|" + phase + "|" + str(side)
        if dedupe:
            if key in self._journal_keys:
                return None
            self._journal_keys.add(key)
            if len(self._journal_keys) > 4 * JOURNAL_MAX:
                self._journal_keys.clear()   # reset barato: solo perdería 1 dedupe
        try:
            ind = m.indicators or {}
            q = (m.diagnostics or {}).get("quant") or {}
            feat = {
                "ema_f": ind.get("ema_fast"), "ema_s": ind.get("ema_slow"),
                "rsi": ind.get("rsi"),
                "atr": (None if not ind.get("atr") else round(float(ind["atr"]), 6)),
                "atr_pct": q.get("atr_pct"),
                "vol_r": ind.get("volume_ratio"),
                "ext_r": (m.diagnostics or {}).get("ext_ratio"),
                "body": (m.diagnostics or {}).get("body_ratio"),
                "score": q.get((side or "long").lower()),
                "threshold": self._st("min_signal_score"),
            }
        except Exception:
            feat = {}
        ctx = None
        try:
            fng = ((self.context or {}).get("fng") or {})
            mtf = (((self.context or {}).get("mtf") or {}).get(pair) or {}).get("trend")
            ctx = {"fng": fng.get("value"), "mtf": mtf}
        except Exception:
            ctx = None
        rec = {
            "ts": now_iso(), "key": key, "pair": pair, "side": side,
            "phase": phase, "price": (round_price(price) if price else None),
            "feat": feat, "ctx": ctx, "block": extra,
            "pnl": None, "bars": None, "why": None,
        }
        self._journal_prune()
        self.journal.append(rec)
        return rec

    def _journal_prune(self):
        """Al pasar el tope, descarta lo viejo PERO nunca una entrada
        ejecutada que aún no tuvo outcome (su posición sigue abierta)."""
        if len(self.journal) <= JOURNAL_MAX:
            return
        refd = set(id(pos.get("journal_ref")) for pos in self.positions.values()
                   if pos.get("journal_ref") is not None)
        # Solo podemos soltar entradas mientras NO sean la referencia viva
        # de una posición abierta: al llegar a una se detiene el corte.
        keep_from = len(self.journal) - JOURNAL_MAX
        while keep_from < len(self.journal) and id(self.journal[keep_from]) not in refd:
            keep_from += 1
        self.journal = self.journal[keep_from:]

    @staticmethod
    def _journal_fill(rec, pnl, bars, why):
        try:
            if rec is None:
                return
            prev = rec.get("pnl")
            rec["pnl"] = (prev or 0.0) + float(pnl or 0.0)
            rec["bars"] = int(bars or 0)
            rec["why"] = why
        except Exception:
            pass

    def drift_check(self):
        """Detección de drift (tercer paso del pipeline SDO): cada N trades
        cerrados evalúa la ventana reciente y auto-ajusta el umbral de señal
        (min_signal_score, +5, tope 85) si la cartera entra en degradación.
        Nunca reduce el umbral automaticamente."""
        if self._drift_closes < DRIFT_EVERY_N:
            return
        self._drift_closes = 0
        try:
            done = [e for e in self.journal
                    if e.get("phase") == "executed" and e.get("why")]
            if len(done) < DRIFT_EVERY_N:
                return
            win = done[-DRIFT_EVERY_N:]
            n = len(win)
            wr = sum(1 for e in win if (e.get("pnl") or 0) > 0) / n
            exp = sum((e.get("pnl") or 0) for e in win) / max(n, 1)
            base = "Drift check (%d trades): WR %.0f%% · expectancy $%.2f"
            if wr <= 0.35 and exp <= 0.0:
                thr = float(self._st("min_signal_score") or 0)
                new = min(thr + 5.0, 85.0)
                if new > thr:
                    self.params["min_signal_score"] = new
                    self._own.add("min_signal_score")
                    self.add_task("risk", base % (n, wr * 100.0, exp)
                                  + " → calidad en degradación: subo el umbral de señal "
                                  + str(thr) + " → " + str(new) + ".")
                else:
                    self.add_task("risk", base % (n, wr * 100.0, exp)
                                  + " → señal degradada y umbral en el tope: revisa el preset.")
            else:
                # Calibración informativa: ¿el score separa bien?
                thr2 = float(self._st("min_signal_score") or 0)
                high = [e for e in win if (e["feat"].get("score") or 0) >= thr2 + 10]
                if len(high) >= 3:
                    wr_hi = sum(1 for e in high if (e.get("pnl") or 0) > 0) / len(high)
                    if wr_hi < wr - 0.15:
                        self.add_task("risk", "Calibración: el score NO separa "
                                      "(trades score>=%.0f con WR %.0f%% vs %.0f%% general)."
                                      % (thr2 + 10, wr_hi * 100.0, wr * 100.0))
        except Exception as e:
            self.add_task("system", "Drift check: error " + str(e)[:90])

    # --- Motor de riesgo formal (fail-fast, pre-trade) ---------------------
    def _risk_gate(self, pair, side, price, size):
        """Checks pre-trade en orden. Devuelve None si pasa o (tag, resumen)
        del PRIMER check que falla. Los %/asumptions vienen de params
        (strategy.risk_*), editables en vivo desde la configuración."""
        # 1) Kill switch diario
        if self.daily <= -float(self.params["max_daily_loss"]):
            return ("killswitch", "Kill switch: límite de pérdida diaria tocado "
                    "(-$" + f"{abs(self.daily):.2f}" + " >= -$"
                    + f"{self.params['max_daily_loss']:.2f}" + "). Entrada bloqueada.")
        # 2) Drawdown de la curva vs capital
        thr = float(self._st("risk_dd_block_pct") or 0) / 100.0
        if thr > 0:
            try:
                peak = self.capital
                for _ts, v in self.equity_curve[-400:]:
                    fv = float(v)
                    if fv > peak:
                        peak = fv
                eq = self.equity()[0]
                dd = (peak - eq) / peak if peak > 1e-9 else 0.0
                if dd >= thr:
                    return ("dd", "Gate de drawdown: la curva cae "
                            + f"{dd * 100:.2f}" + "% sobre su máximo (bloqueo a "
                            + f"{self._st('risk_dd_block_pct'):.0f}" + "%). "
                            + "Esperando recuperación antes de nuevas entradas.")
            except Exception:
                pass
        # 3) Fat finger: precio de entrada fuera del rango de la vela actual
        m = self.markets.get(pair)
        if m and m.candles and price:
            c0 = m.candles[-1]
            if price > float(c0["high"]) * 1.05 or price < float(c0["low"]) * 0.95:
                return ("fatfinger", "Fat finger: entrada $" + f"{price:g}"
                        + " fuera del rango de la vela actual ["
                        + f"{float(c0['low']):g}" + " - " + f"{float(c0['high']):g}" + "].")
        # 4) Colchón de cash
        res = float(self._st("risk_reserve_pct") or 0) / 100.0
        if res > 0 and (self.cash - size) < self.capital * res:
            return ("reserva", "Colchón insuficiente: abrir dejaría el efectivo bajo el "
                    + f"{res * 100:.0f}" + "% del capital (efectivo $"
                    + f"{self.cash:.2f}" + " / capital $" + f"{self.capital:.2f}" + ").")
        # 5) Exposición nocional total
        cap = float(self._st("risk_max_exposure") or 0)
        if cap > 0:
            lev = int(self._st("leverage")) if self._futures_on() else 1
            notional = sum(float(p.get("notional") or 0) for p in self.positions.values())
            if notional + size * max(lev, 1) > self.capital * cap:
                return ("exposure", "Exposición máxima: nocional total "
                        + f"${notional + size * max(lev, 1):.2f}" + " > "
                        + f"{cap:.1f}" + "x capital $"
                        + f"{self.capital:.2f}" + " (nocional abierta $"
                        + f"{notional:.2f}" + ").")
        return None

    @staticmethod
    def _strength(m, direction):
        try:
            return float((m.diagnostics.get("strength") or {}).get(direction.lower(), 0.0))
        except Exception:
            return 0.0

    def _stop_fill(self, m, price, stop, is_long):
        """Precio de relleno paper para SL/trailing: el nivel del stop más un
        deslizamiento máximo de sl_slip_atr × ATR. Con cap 0 rellena exacto en
        el nivel del stop (como una orden stop colocada en el libro) — un hueco
        violento del feed no convierte un SL de -2.5% en una pérdida enorme."""
        try:
            cap = float(self._st("sl_slip_atr")) or 0.0
        except Exception:
            cap = 0.0
        if cap <= 0 or not stop:
            return round_price(stop)
        atr = float(getattr(m, "atr", None) or 0.0)
        slip = min(abs(float(price) - float(stop)), cap * atr) if atr > 0 else 0.0
        return round_price((float(stop) - slip) if is_long else (float(stop) + slip))

    def _fee_rate(self, side, leverage, maker=False):
        """Comisión REAL de Binance (fracción) para esta pierna. La pierna de
        salida por TP se ejecuta como orden LÍMITE en el libro (maker), igual
        que en real: en USDT-M la distancia maker/taker es grande
        (maker 0.02% vs taker 0.05%) y reduce mucho el coste por ciclo.
        SL/trailing/momento/stop de tiempo son de mercado → taker.
        fees_enabled=False -> 0.0 (sin coste simulado)."""
        if not self.params.get("fees_enabled", True):
            return 0.0
        try:
            lev = int(leverage or 1)
        except Exception:
            lev = 1
        if side == "SHORT" or lev > 1:
            key = ("fee_maker_futs" if maker else "fee_futs")
            rate = self.params.get(key, 0.0002 if maker else 0.0005)
        else:
            key = ("fee_maker_spot" if maker else "fee_spot")
            # Spot VIP0: maker y taker cuestan lo mismo (0.1% base; BNB -25%).
            rate = self.params.get(key, 0.00075 if maker else 0.001)
        try:
            return max(0.0, min(float(rate), 0.01))
        except Exception:
            return 0.0

    # -- System 2: Deep Analysis ----------------------------------------
    def build_deep_price_info(self, pair, signal):
        """Arma el price_info que DeepAnalysisEngine.analyze consume (mismo
        snapshot que en la señal en vivo; extraído como método para poder
        reusarlo desde el run manual del dashboard)."""
        m = self.markets.get(pair)
        if m is None:
            return None
        ind = m.indicators
        ctx = self.context or {}
        preset = self.params.get("preset", "standard")
        preset_info = AGENTIC_PRESETS.get(preset, AGENTIC_PRESETS.get("standard", {}))
        price_info = {
            "pair": pair,
            "price": m.price,
            "ema_fast": ind.get("ema_fast"),
            "ema_slow": ind.get("ema_slow"),
            "rsi": ind.get("rsi"),
            "volume_ratio": ind.get("volume_ratio"),
            "candle_direction": "alcista" if m.candles[-1]["close"] > m.candles[-1]["open"] else "bajista",
            "timeframe": self.params.get("tf_seconds", 300),
            "capital": self.capital,
            "feed": getattr(m, "feed", "synthetic"),
            "atr": ind.get("atr"),
            "body_ratio": (abs(m.candles[-1]["close"] - m.candles[-1]["open"])
                           / max(m.candles[-1]["high"] - m.candles[-1]["low"], 1e-9)),
            "max_ext_atr": self._st("max_ext_atr"),
            "ext_ratio": m.diagnostics.get("ext_ratio"),
            "min_body": self._st("min_body_ratio"),
            "signal": signal,
            "strength_long": m.diagnostics.get("strength", {}).get("long", 0),
            "strength_short": m.diagnostics.get("strength", {}).get("short", 0),
            "context": ctx,
            "strategy": {
                "sl_pct": self.params.get("sl_pct", 0.025),
                "tp_pct": self.params.get("tp_pct", 0.05),
                "ctx_filter": self._st("ctx_filter"),
                "tp_sl_mode": self._st("tp_sl_mode"),
                "sl_atr_mult": self._st("sl_atr_mult"),
                "tp_rr": self._st("tp_rr"),
            },
            "preset": preset,
            "preset_name": preset_info.get("name", "Estándar"),
            "preset_desc": preset_info.get("description", ""),
            "max_position_usd": self.params.get("max_pos", 10),
            "max_daily_loss_usd": self.params.get("max_daily_loss", 8),
            "_deep_latency_ms": 0,
        }
        return price_info

    def _evaluate_system2_verdict(self, pair, signal, result):
        """Evalúa si el veredicto cualitativo de System 2 aprueba la señal
        según el perfil agéntico activo (Conservador, Estándar, Agresivo)."""
        rec = str(result.get("recomendacion_laya", "WAIT") or "WAIT").upper()
        try:
            conf = float(result.get("confianza", 0.0) or 0.0)
        except Exception:
            conf = 0.0
        reason = str(result.get("razon_corta", "") or "")
        preset = str(self.params.get("preset", "standard")).lower()

        if preset == "conservative":
            min_conf = 0.65
            approved = (rec == signal and conf >= min_conf)
            if approved and result.get("filtro_extra", {}).get("esperar_confirmacion_mtf"):
                mtf_trend = str((((self.context or {}).get("mtf") or {}).get(pair) or {}).get("trend") or "")
                if (signal == "LONG" and "BAJISTA" in mtf_trend) or (signal == "SHORT" and "ALCISTA" in mtf_trend):
                    approved = False
                    reason += f" (Bloqueado por sesgo MTF {mtf_trend} en modo Conservador)"
        elif preset == "high_alpha":
            # Modo High-Alpha: máxima confluencia para proteger los $50 USD
            min_conf = 0.55
            # Si Gemini recomienda la dirección contraria, bloqueo directo
            op_dir = "SHORT" if signal == "LONG" else "LONG"
            if rec == op_dir:
                approved = False
                reason += f" (Gemini advierte contratendencia {rec} al trade {signal})"
            elif rec == signal and conf >= min_conf:
                approved = True
            elif rec == "WAIT":
                # Si es WAIT pero el Quant score es excelente (>=70) y MTF alineado, permite entrar
                m = self.markets.get(pair)
                m_q = (m.diagnostics.get("quant", {}).get(signal.lower()) if m else None) if m else None
                approved = (m_q is not None and m_q >= 70.0)
                if approved:
                    reason += f" (Veredicto WAIT superado por Quant Score élite {m_q:.0f})"
                else:
                    approved = False
                    reason += " (Esperando setup con mayor claridad técnica)"
            else:
                approved = False
        elif preset == "aggressive":
            min_conf = 0.40
            if rec == signal and conf >= min_conf:
                approved = True
            elif rec == "WAIT":
                # En modo agresivo, si el Quant score es alto (>=65), Gemini WAIT no bloquea
                m = self.markets.get(pair)
                m_q = (m.diagnostics.get("quant", {}).get(signal.lower()) if m else None) if m else None
                approved = (m_q is not None and m_q >= 65.0)
                if approved:
                    reason += f" (Aprobado por Quant Score {m_q:.0f} en modo Agresivo)"
            else:
                approved = False
        else:  # standard
            min_conf = 0.50
            approved = (rec == signal and conf >= min_conf)

        return approved, min_conf, rec, conf, reason

    def _deep_analysis_check(self, pair, signal):
        """Consulta System 2 (LLM) para confirmar/filtrar la señal.
        Retorna True si System 2 aprueba la entrada, False si la rechaza.
        Respeta rate limits: 10 RPM global, 2 min cooldown por par, 1000 RPD."""
        deep_engine = self.params.get("deep_engine")
        deep_rate = self.params.get("deep_rate")
        deep_lock = self.params.get("deep_lock")
        if not deep_engine or not deep_engine.enabled or not deep_rate or not deep_lock:
            return True  # Sin System 2 -> no bloquea

        # Solo consultar si use_for_filter está activo
        if not getattr(deep_engine, "use_filter", True):
            return True

        m = self.markets.get(pair)
        if not m:
            return True

        now = time.time()
        day_window = int(now // 86400) * 86400

        # --- Rate limit check (con lock compartido) ---
        with deep_lock:
            # Reset contadores si cambió ventana
            if now - deep_rate["minute_window"] >= 60:
                deep_rate["calls_minute"] = 0
                deep_rate["minute_window"] = now
            if day_window != deep_rate["day_window"]:
                deep_rate["calls_day"] = 0
                deep_rate["day_window"] = day_window

            # Verificar límites globales
            if deep_rate["calls_minute"] >= deep_rate["max_per_minute"]:
                return True  # Rate limit -> fallback a System 1 (no bloquea)
            if deep_rate["calls_day"] >= deep_rate["max_per_day"]:
                return True  # Daily limit -> fallback

            # Verificar cooldown por par
            last_call = deep_rate["pair_cooldown"].get(pair, 0)
            if now - last_call < deep_rate["cooldown_sec"]:
                # Verificar cache (válido por cooldown_sec)
                cached = deep_rate["pair_cache"].get(pair)
                if cached and now - cached[2] < deep_rate["cooldown_sec"]:
                    cached_signal, cached_result, _ = cached
                    if cached_signal != signal:
                        # Veredicto vigente es para la dirección contraria: esperar
                        # a que expire el cooldown y se analice la nueva dirección.
                        return False
                    approved, min_conf, rec, conf, reason = self._evaluate_system2_verdict(pair, signal, cached_result)
                    if not approved:
                        # Un WAIT/ABSTAIN real (conf > 0) bloquea durante todo el
                        # cooldown; antes se perdía y el siguiente tick abría igual.
                        self._note_once(pair, "deep_cache", "deep",
                                        f"System 2 (cache) BLOQUEA {signal} en {pair}: {rec} (conf {conf:.0%}, req {min_conf:.0%})")
                    elif getattr(deep_engine, "use_adjust", False) and cached_result.get("ajuste_sl_tp"):
                        if not hasattr(self, "_ai_sl_tp_adjust"):
                            self._ai_sl_tp_adjust = {}
                        self._ai_sl_tp_adjust[pair] = cached_result["ajuste_sl_tp"]
                    return approved
                # Sin veredicto real en cooldown (error del LLM) -> fallback a System 1
                return True

            # Todo OK: incrementar contadores y proceder
            deep_rate["calls_minute"] += 1
            deep_rate["calls_day"] += 1
            deep_rate["pair_cooldown"][pair] = now

        price_info = self.build_deep_price_info(pair, signal)
        try:
            start = time.time()
            result = deep_engine.analyze(price_info)
            elapsed = (time.time() - start) * 1000
            if result:
                approved, min_conf, rec, conf, reason = self._evaluate_system2_verdict(pair, signal, result)
                preset = str(self.params.get("preset", "standard")).capitalize()
                self.add_task("deep", f"System 2 [{pair}] ({preset}): {rec} (conf {conf:.0%}) — {reason}")
                if not approved:
                    self.add_task("deep", f"System 2 BLOQUEA entrada {signal} en {pair}: {rec} (conf {conf:.0%}, req {min_conf:.0%})")
                else:
                    if getattr(deep_engine, "use_adjust", False) and result.get("ajuste_sl_tp"):
                        if not hasattr(self, "_ai_sl_tp_adjust"):
                            self._ai_sl_tp_adjust = {}
                        self._ai_sl_tp_adjust[pair] = result["ajuste_sl_tp"]

                # Cachear todo veredicto real (conf > 0), incluido WAIT/ABSTAIN.
                # conf == 0 es el fallback por error del LLM: no se cachea.
                if conf > 0.0:
                    with deep_lock:
                        deep_rate["pair_cache"][pair] = (signal, result, now)
                else:
                    return True  # fallback por error del LLM: no bloquea
                return approved
        except Exception as e:
            self.add_task("deep", f"System 2 error en {pair}: {e}")
        return True  # En caso de error, no bloquear (fallback a System 1)

    # -- ciclo -----------------------------------------------------------
    def tick(self):
        # "PnL de hoy": se resetea al cambiar el día (semántica real de diario;
        # el límite de pérdida diaria mira este valor).
        today = now_iso()[:10]
        if self.daily_date != today:
            self.daily_date = today
            self.daily = 0.0
        self.cycle += 1
        for p in self.pairs:
            self.markets[p].tick()
            self.manage(p)
        # Cooldowns de re-entrada — medidos en VELAS reales del feed: expiran
        # cuando el reloj de velas avanza, no por tick. En feed real de 5m,
        # 6 barras = 30 min; en sintético, 1 tick = 1 vela (comportamiento igual
        # al anterior). Evita el churn del modo real (antes: 6 ticks = 12 s).
        tf_c = self.params.get("tf_seconds", 300)
        for p in list(self.cooldown_till.keys()):
            m_c = self.markets.get(p)
            bar_c = m_c.candles[-1]["time"] if m_c and m_c.candles else int(time.time())
            if bar_c >= self.cooldown_till[p]:
                del self.cooldown_till[p]
                self.cooldown.pop(p, None)
                self.add_task("system", "Cooldown completado: " + p + " puede re-entrar.")
            else:
                self.cooldown[p] = max(0, -(-(self.cooldown_till[p] - bar_c) // max(1, tf_c)))
        # legacy: estados restaurados con cooldown en barras pero sin 'till'
        for p in list(self.cooldown.keys()):
            if p not in self.cooldown_till:
                del self.cooldown[p]
        futures_on = self._futures_on()
        max_open = max(int(self.params["max_open"]), 1)
        for p in self.pairs:
            m = self.markets[p]
            if p in self.positions:
                continue
            if len(self.positions) >= max_open:
                # Solo informar si hay señal real que quede en espera: con
                # señal NONE no hay nada que "esperar hueco" (evita spam).
                if m.signal != "NONE":
                    unidades = "posición" if max_open == 1 else "posiciones"
                    self._note_once(p, "maxopen", "risk",
                                    "Máximo de " + str(max_open) + " " + unidades + " abierta"
                                    + ("s" if max_open != 1 else "") + ": señal "
                                    + m.signal + " en " + p + " esperando hueco.")
                    self._journal("maxopen", p, m.signal, m.price,
                                  extra="Máximo de posiciones abiertas (" + str(max_open) + ").")
                continue
            cd_till = self.cooldown_till.get(p)
            if cd_till is not None:
                bar_now = m.candles[-1]["time"] if m.candles else int(time.time())
                if bar_now < cd_till:
                    if m.signal != "NONE":
                        rest = max(0, -(-(cd_till - bar_now) // max(1, self.params.get("tf_seconds", 300))))
                        self._note_once(p, "cool", "signal",
                                        "Cooldown activo en " + p + " (" + str(rest) + " barras): "
                                        "señal " + m.signal + " en espera.")
                    continue
            # --- Motor de riesgo formal (checks fail-fast antes de tocar la cartera) ---
            if m.signal in ("LONG", "SHORT"):
                comp_pct = float(self._st("compound_pct") or 0.0)
                if comp_pct > 0.0:
                    size_r = min(self.cash * (comp_pct / 100.0), self.cash)
                else:
                    size_r = min(self.params["max_pos"], self.cash)
                rk = self._risk_gate(p, m.signal, m.price, size_r)
                if rk:
                    self._note_once(p, "risk_" + rk[0], "risk", rk[1])
                    self._journal("risk:" + rk[0], p, m.signal, m.price, extra=rk[1])
                    continue
            # Puerta de contexto y MTF (params.ctx_filter):
            # 1) Bloquea entradas que van en contra de la tendencia MTF (1h) confirmada
            # 2) Con Fear & Greed EXTREMO (>=80 o <=10) exige confluencia absoluta
            if self._st("ctx_filter") and m.signal in ("LONG", "SHORT"):
                trend = str((((self.context or {}).get("mtf") or {}).get(p) or {}).get("trend") or "").upper()
                if ("BAJISTA" in trend and m.signal == "LONG") or ("ALCISTA" in trend and m.signal == "SHORT"):
                    self._note_once(p, "ctx_mtf", "signal",
                                    "Filtro MTF (1h): señal " + m.signal + " bloqueada por contratendencia "
                                    + trend + " en " + p + ".")
                    self._journal("ctx", p, m.signal, m.price,
                                  extra="Bloqueado por contratendencia MTF (1h: " + trend + ").")
                    continue

                ctx_v = None
                try:
                    ctx_v = int(((self.context or {}).get("fng") or {}).get("value"))
                except Exception:
                    ctx_v = None
                if ctx_v is not None and (ctx_v >= 80 or ctx_v <= 10):
                    ok_ctx = ("ALCISTA" in trend) if m.signal == "LONG" else ("BAJISTA" in trend)
                    if not ok_ctx:
                        self._note_once(p, "ctx", "signal",
                                        "Contexto extremo (F&G " + str(ctx_v) + "): señal "
                                        + m.signal + " en " + p + " en espera de confluencia MTF ("
                                        + (trend or "sin sesgo") + ").")
                        self._journal("ctx", p, m.signal, m.price,
                                      extra="F&G extremo (" + str(ctx_v) + ") sin confluencia MTF "
                                            + (trend or "sin sesgo") + ".")
                        continue
            # --- System 2: Deep Analysis filter ---
            if m.signal in ("LONG", "SHORT"):
                if not self._deep_analysis_check(p, m.signal):
                    self._journal("deep", p, m.signal, m.price,
                                  extra="System 2 (Gemini) bloqueó la señal.")
                    continue
            if m.signal == "LONG":
                self.open(p, "LONG")
            elif m.signal == "SHORT":
                if futures_on and self._st("short_enabled"):
                    self.open(p, "SHORT")
                else:
                    self._note_once(p, "shortspot", "signal",
                                    "SHORT detectado (" + p + "): ignorado en spot paper - "
                                    "activa el origen 'Futuros Testnet' (strategy.short_enabled).")
        self._curve_push()
        return self.daily <= -self.params["max_daily_loss"]

    def _curve_push(self):
        """Curva de equity para Intelligence Analytics: 1 punto por tick
        (antes de cada retorno). Al pasar el tope se decima la parte vieja
        ([::2]) para que la memoria no crezca y la curva cubra más tiempo."""
        eq = self.equity()[0]
        self.equity_curve.append([now_iso(), round(eq, 4)])
        if len(self.equity_curve) > 900:
            tail = self.equity_curve[-1]
            self.equity_curve = self.equity_curve[::2]
            self.equity_curve.append(tail)

    def open(self, pair, side="LONG"):
        m = self.markets[pair]
        side = "SHORT" if side == "SHORT" else "LONG"
        if side == "SHORT":
            if not self._futures_on() or not self._st("short_enabled"):
                return
        price = m.price
        # Auto-compounding (ej: 40% del capital/cash disponible si compound_pct está configurado)
        compound_pct = float(self._st("compound_pct") or 0.0)
        if compound_pct > 0.0:
            target_size = self.cash * (compound_pct / 100.0)
            size = min(target_size, self.cash)
        else:
            size = min(self.params["max_pos"], self.cash)
        size = round(size, 2)
        if size < 1.0 or price <= 0:
            if m.signal != "NONE":
                self._note_once(pair, "nocash", "risk",
                                "Señal " + side + " (" + pair + ") : saldo insuficiente para abrir.")
            return
        lev_long = int(self._st("leverage")) if self.params.get("futures_enabled") else 1
        if side == "SHORT":
            lev = int(self._st("leverage")) if self._futures_on() else 1
        else:
            # Paper-futuros: el LONG va a x1; con feed real de futuros, xN (como antes).
            lev = lev_long
        margin = size
        notional = margin * lev
        qty = notional / price
        # Comisión de APERTURA real de Binance (taker) sobre el nocional. Como
        # en backtesting: se ANOTA en la posición y el cash no se toca; el coste
        # total (apertura + salida) se descuenta del PnL en el cierre.
        is_maker_entry = bool(self._st("maker_entry"))
        fee_open = round(max(0.0, notional * self._fee_rate(side, lev, maker=is_maker_entry)), 8)
        # --- SL/TP/TP1: modo "atr" (por defecto) escala las distancias con la
        #     volatilidad REAL del momento: SL = sl_atr_mult*ATR acotado a
        #     [min_sl_pct, max_sl_pct] del precio, TP = SL x tp_rr y TP1 a la
        #     fracción tp1_rr_frac del TP. Con % fijos, en barras de 5m los
        #     objetivos quedaban a decenas de ATR (stop de tiempo asegurado).
        base_sl_dist = price * self.params["sl_pct"]
        tp_sl_mode = str(self._st("tp_sl_mode") or "atr")
        if tp_sl_mode == "atr" and m.atr and m.atr > 0:
            try:
                sl_dist = m.atr * float(self._st("sl_atr_mult"))
                lo_d = price * float(self._st("min_sl_pct"))
                hi_d = price * float(self._st("max_sl_pct"))
                sl_dist = min(max(sl_dist, lo_d), hi_d)
            except Exception:
                sl_dist = base_sl_dist
            try:
                tp_dist = sl_dist * float(self._st("tp_rr"))
            except Exception:
                tp_dist = base_sl_dist * 2.0
            try:
                tp1_dist = tp_dist * float(self._st("tp1_rr_frac"))
            except Exception:
                tp1_dist = tp_dist * 0.45
        else:
            sl_dist = base_sl_dist
            tp_dist = price * float(self.params.get("tp_pct", 0.05))
            tp1_dist = price * float(self._st("tp1_pct") or 0.025)

        # Aplicar ajuste dinamico de System 2 (IA) si esta disponible
        ai_adj = getattr(self, "_ai_sl_tp_adjust", {}).pop(pair, None)
        if ai_adj:
            try:
                sl_f = float(ai_adj.get("sl_factor", 1.0) or 1.0)
                tp_f = float(ai_adj.get("tp_factor", 1.0) or 1.0)
                sl_f = max(0.75, min(1.25, sl_f))
                tp_f = max(0.75, min(1.25, tp_f))
                if sl_f != 1.0 or tp_f != 1.0:
                    sl_dist *= sl_f
                    tp_dist *= tp_f
                    tp1_dist *= tp_f
                    self.add_task("deep", f"System 2 aplico ajuste dinamico a {pair}: SL x{sl_f:.2f}, TP x{tp_f:.2f}")
            except Exception:
                pass

        if side == "LONG":
            sl = price - sl_dist
            tp = price + tp_dist
            tp1 = price + tp1_dist
            liq = price * (1 - 1.0 / lev) if lev > 1 else None
        else:
            sl = price + sl_dist
            tp = price - tp_dist
            tp1 = price - tp1_dist
            liq = price * (1 + 1.0 / lev) if lev > 1 else None
        self.cash -= margin
        self.positions[pair] = {
            "pair": pair, "side": side, "entry": round_price(price),
            "qty": round(qty, 10), "size_usd": round(margin, 4),
            "notional": round(notional, 4), "leverage": lev,
            "liq": round_price(liq) if liq else None,
            "sl": round_price(sl), "tp": round_price(tp),
            "tp1_price": round_price(tp1), "tp1_done": False,
            "trail": None, "peak": round_price(price),
            "bars_held": 0,
            "fee_open": fee_open, "fee_open_left": fee_open,
            "atr_entry": (round(m.atr, 6) if m.atr else None),
            "opened_at": now_iso(),
            "entry_bar": (m.candles[-1]["time"] if m.candles else None),
            "entry_time": (m.candles[-1]["time"] if m.candles else None),
        }
        tam = ("margen $" + f"{margin:.2f}" + " · x" + str(lev) + " · nocional $"
               + f"{notional:.2f}") if lev > 1 else ("tamaño $" + f"{margin:.2f}")
        if is_maker_entry:
            tam += " · [Maker]"
        if fee_open > 0:
            tam += " · fee apertura $" + f"{fee_open:.4f}"
        self.add_task("trade", "SEÑAL " + side + " (" + pair + ") → posición abierta @ $"
                      + str(round_price(price)) + " (" + tam + ").")
        # Decision journal: registro de la ejecución; la referencia vive en la
        # posición para que el cierre retroalimente el outcome al registro.
        try:
            rec = self._journal("executed", pair, side, price, dedupe=False)
            if rec is not None:
                self.positions[pair]["journal_ref"] = rec
        except Exception:
            pass

    def manage(self, pair):
        pos = self.positions.get(pair)
        if not pos:
            return
        m = self.markets[pair]
        price = m.price
        if price <= 0:
            return
        side = pos.get("side", "LONG")
        is_long = side == "LONG"

        # Barras en posición (para el stop de tiempo)
        bar = (m.candles[-1]["time"] if m.candles else None)
        if pos.get("entry_bar") and bar is not None and bar > pos["entry_bar"]:
            held = int((bar - pos["entry_bar"]) / self.params["tf_seconds"])
            pos["bars_held"] = max(int(pos.get("bars_held", 0)), held)
        pos["price"] = round_price(price)

        def hit_favor(target):
            return (price >= target) if is_long else (price <= target)

        # pico más favorable desde la entrada (para trailing)
        if (is_long and price > float(pos.get("peak") or 0)) or \
           (not is_long and price < float(pos.get("peak") or 1e18)):
            pos["peak"] = round_price(price)

        # 1) Stop Loss (fijo o trailing móvil). Si el precio sobrepasa el SL,
        #    se ejecuta de inmediato para evitar que un salto de velas o slippage
        #    simulado alcance la liquidación artificialmente.
        sl_now = pos.get("trail") or pos["sl"]
        if (is_long and price <= sl_now) or (not is_long and price >= sl_now):
            reason = "Trailing Stop (ATR)" if pos.get("trail") is not None else "SL alcanzado"
            self.close(pair, self._stop_fill(m, price, sl_now, is_long), reason)
            return

        # 2) Liquidación (solo futuros apalancados cuando el precio supera el nivel de quiebra)
        liq = pos.get("liq")
        if liq and ((is_long and price <= liq) or (not is_long and price >= liq)):
            self.close(pair, liq, "Liquidación (futuros)")
            return

        # 2) TP final (posición completa)
        if hit_favor(pos["tp"]):
            self.close(pair, pos["tp"], "TP alcanzado")
            return

        # 3) TP1 parcial: toma de ganancias + break-even + activar trailing
        if not pos.get("tp1_done") and hit_favor(pos["tp1_price"]):
            frac = max(0.1, min(0.9, float(self._st("tp1_size_pct"))))
            tp1_pct_real = abs(float(pos["tp1_price"]) / float(pos["entry"]) - 1.0) * 100.0
            self.close(pair, price, "TP1 parcial (+" + f"{tp1_pct_real:.1f}" + "%)", qty=pos["qty"] * frac)
            pos = self.positions.get(pair)
            if not pos:
                return
            pos["tp1_done"] = True
            if self._st("break_even") and pos["sl"] != pos["entry"]:
                pos["sl"] = pos["entry"]
                self.add_task("trade", "Break-even: SL de " + pair + " movido a la entrada $"
                              + str(pos["entry"]) + " (riesgo residual 0).")
            atr = m.atr or pos.get("atr_entry") or (price * 0.01)
            mult = float(self._st("trail_atr_mult"))
            if mult > 0:
                lvl = (price - atr * mult) if is_long else (price + atr * mult)
                lvl = max(lvl, pos["entry"]) if is_long else min(lvl, pos["entry"])
                pos["trail"] = round_price(lvl)
                self.add_task("trade", "Trailing ATR activado en " + pair + ": nivel $"
                              + str(pos["trail"]) + " (" + f"{mult:.1f}" + "xATR).")
            return

        # 4) Trailing por ATR sobre la parte restante
        trail = pos.get("trail")
        if trail is not None and pos.get("tp1_done"):
            atr = m.atr or pos.get("atr_entry")
            mult = float(self._st("trail_atr_mult"))
            if atr and mult > 0:
                lvl = (pos["peak"] - atr * mult) if is_long else (pos["peak"] + atr * mult)
                if self._st("break_even"):
                    lvl = max(lvl, pos["entry"]) if is_long else min(lvl, pos["entry"])
                if (is_long and lvl > trail) or (not is_long and lvl < trail):
                    pos["trail"] = round_price(lvl)
                    trail = pos["trail"]
            if (is_long and price <= trail) or (not is_long and price >= trail):
                self.close(pair, trail, "Trailing Stop (ATR)")
                return

        # 5) Salida por momentum contrario (señal antagónica fuerte).
        #    Gracia: ignora rebotes de contragolpe en las primeras N barras de la
        #    posición (anti-churn). Umbral: fuerza mínima configurable (%) de la
        #    señal contraria; 0 mantiene la base histórica de 75%.
        if self._st("momentum_exit"):
            anti = "SHORT" if is_long else "LONG"
            if m.signal == anti:
                minst = float(self._st("momentum_min_strength")) / 100.0
                if self._strength(m, anti) >= max(0.75, minst):
                    grace = int(self._st("momentum_grace_bars"))
                    held = int(pos.get("bars_held", 0))
                    if held < grace:
                        self._note_once(pair, "grace", "trade",
                                        "Rebote " + anti + " ignorado en " + pair + ": gracia de "
                                        + str(grace) + " barras tras la entrada (barra "
                                        + str(held) + ").")
                    else:
                        self.close(pair, price, "Salida por momentum " + anti + " adverso")
                        return

        # 6) Stop de tiempo (barras sin alcanzar SL/TP)
        ts = int(self._st("time_stop_bars"))
        if ts > 0 and int(pos.get("bars_held", 0)) >= ts:
            self.close(pair, price, "Stop de tiempo (" + str(ts) + " barras sin objetivo)")

    def close(self, pair, exit_price, reason, qty=None):
        """Cierra toda la posición (qty=None) o una parte (TP1 parcial).
        Devuelve {'partial': bool, 'pnl': float} o None si no había posición."""
        pos = self.positions.get(pair)
        if not pos:
            return None
        side = pos.get("side", "LONG")
        is_long = side == "LONG"
        entry = float(pos["entry"])
        pos_qty = float(pos["qty"])
        margin = float(pos.get("size_usd") or 0.0)

        if qty is not None and float(qty) > 0 and float(qty) < pos_qty - 1e-12:
            # ---- cierre parcial (TP1) ----
            closed_qty = float(qty)
            closed_margin = margin * closed_qty / pos_qty
            # Fee real de la parte cerrada: proporción del fee de apertura
            # pendiente + fee taker de la pierna de salida.
            fee_left = float(pos.get("fee_open_left", 0.0) or 0.0)
            fee_share = fee_left * closed_qty / pos_qty
            maker_exit = reason.startswith("TP1") or reason.startswith("TP ")
            fee_exit = float(exit_price) * closed_qty * self._fee_rate(
                side, pos.get("leverage", 1), maker=maker_exit)
            fee = fee_share + fee_exit
            pnl = ((exit_price - entry) if is_long else (entry - exit_price)) * closed_qty
            pnl = max(pnl, -closed_margin)
            # Reserva neta al cerrar: este tramo paga su parte del fee de
            # apertura + el fee de salida; la pérdida neta tampoco supera el
            # margen del tramo.
            pnl = max(pnl - fee, -closed_margin)
            self.fees_paid += fee
            pos["fee_open_left"] = max(0.0, fee_left - fee_share)
            self.cash += closed_margin + pnl
            self.realized += pnl
            self.daily += pnl
            pos["qty"] = round(pos_qty - closed_qty, 10)
            pos["size_usd"] = round(margin - closed_margin, 4)
            self._record_trade(pair, side, entry, exit_price, closed_qty,
                               closed_margin, pnl, reason, pos, partial=True, fee=fee)
            # Outcome parcial al journal (acumulable hasta el cierre total)
            try:
                self._journal_fill(pos.get("journal_ref"), pnl, pos.get("bars_held"), None)
            except Exception:
                pass
            self.add_task("trade", "TP1 parcial en " + pair + ": cerrado "
                          + f"{closed_qty:.6f}" + " → PnL neto "
                          + ("+" if pnl >= 0 else "-") + f"${abs(pnl):.2f}"
                          + " · resto gestionado con trailing.")
            return {"partial": True, "pnl": pnl}

        # ---- cierre total ----
        if reason.startswith("Liquidación"):
            pnl = -margin          # margen íntegro perdido en la liquidación
            fee = 0.0              # la pérdida ya es el margen completo: no se añaden comisiones
        else:
            maker_exit = reason.startswith("TP1") or reason.startswith("TP ")
            fee_exit = float(exit_price) * pos_qty * self._fee_rate(
                side, pos.get("leverage", 1), maker=maker_exit)
            fee = float(pos.get("fee_open_left", 0.0) or 0.0) + fee_exit
            pnl = ((exit_price - entry) if is_long else (entry - exit_price)) * pos_qty
            pnl = max(pnl, -margin)   # la pérdida bruta nunca supera el margen (paper)
            pnl = max(pnl - fee, -margin)  # ...y el fee tampoco la lleva más allá
        self.fees_paid += fee
        self.cash += margin + pnl
        self.realized += pnl
        self.daily += pnl
        self._record_trade(pair, side, entry, exit_price, pos_qty, margin, pnl, reason, pos, fee=fee)
        # Outcome completo al journal + contador para el drift check
        try:
            self._journal_fill(pos.get("journal_ref"), pnl, pos.get("bars_held"), reason)
            if pos.get("journal_ref") is not None:
                self._drift_closes += 1
                self.drift_check()
        except Exception:
            pass
        del self.positions[pair]
        cd = int(self._st("cooldown_bars"))
        if cd > 0:
            if pnl < 0:
                mult = float(self._st("cooldown_loss_mult"))
                if mult > 1.0:
                    cd = int(round(cd * mult))
                # Disyuntor de rachas adversas: si hay 2 o más pérdidas consecutivas, extender enfriamiento
                recent_losses = 0
                for tr in reversed(self.history[-4:]):
                    if (tr.get("pnl") or 0) < 0:
                        recent_losses += 1
                    else:
                        break
                if recent_losses >= 2:
                    cd = max(cd, 6)  # Al menos 6 velas de enfriamiento tras 2 pérdidas seguidas
                    self.add_task("risk", f"Disyuntor de racha adversa en {pair} ({recent_losses} pérdidas consecutivas): enfriamiento extendido a {cd} barras.")
            self.cooldown[pair] = cd
            m_c = self.markets.get(pair)
            bar_c = m_c.candles[-1]["time"] if m_c and m_c.candles else int(time.time())
            # Bloqueo por vela real del feed (cooldown_till), no por tick.
            self.cooldown_till[pair] = bar_c + cd * self.params.get("tf_seconds", 300)
        self.add_task("trade", "Posición " + side + " " + pair + " cerrada: " + reason
                      + " → PnL " + ("+" if pnl >= 0 else "-") + f"${abs(pnl):.2f}"
                      + (" (comisión $" + f"{fee:.2f}" + ")" if fee > 0 else "") + ".")
        return {"partial": False, "pnl": pnl}

    def _record_trade(self, pair, side, entry, exit_price, qty, size_usd, pnl, reason, pos, partial=False, fee=0.0):
        m = self.markets.get(pair)
        trade = {
            "id": len(self.history) + 1, "pair": pair, "side": side,
            "entry": round_price(entry), "exit": round_price(exit_price),
            "qty": round(qty, 10), "size_usd": round(size_usd, 4),
            "leverage": pos.get("leverage", 1),
            "pnl": round(pnl, 4),
            "pnl_pct": round(pnl / size_usd * 100, 2) if size_usd else 0,
            "fee": round(max(0.0, float(fee or 0.0)), 6),
            "reason": reason, "partial": bool(partial),
            "opened_at": pos.get("opened_at"),
            "entry_time": pos.get("entry_time"),
            "bars_held": int(pos.get("bars_held", 0)),
            "closed_at": now_iso(),
            "exit_time": (m.candles[-1]["time"] if m and m.candles else None),
        }
        self.history.append(trade)
        if len(self.history) > MAX_HISTORY:
            self.history = self.history[-MAX_HISTORY:]
        return trade

    def equity(self):
        eq = self.cash
        pos_val = 0.0
        unreal = 0.0
        for pair, pos in self.positions.items():
            price = self.markets[pair].price
            is_long = pos.get("side", "LONG") == "LONG"
            if is_long:
                pv = pos["qty"] * price
                pos_val += pv
                un = pv - pos["size_usd"]
            else:
                # SHORT (futuros): PnL = (entrada - actual) * qty; el margen sigue reservado
                un = (float(pos["entry"]) - price) * pos["qty"]
                un = max(un, -pos["size_usd"])   # clamp: liquidación no rompe el margen
                pos_val += pos["size_usd"]
            eq += pos["size_usd"] + un
            unreal += un
        return eq, unreal, pos_val

    # -- vistas ----------------------------------------------------------
    # Los datos de una operación "lógica": el TP1 parcial y el cierre final
    # comparten par + opened_at → se agrupan (parcial + final = 1 trade).
    @staticmethod
    def _reason_bucket(reason):
        if not reason:
            return "Otro"
        for k, lbl in (("Liquidación", "Liquidación"), ("TP1", "TP1 parcial"),
                       ("Trailing", "Trailing"), ("SL", "Stop Loss"),
                       ("momentum", "Salida momentum"), ("tiempo", "Stop de tiempo"),
                       ("TP", "Take Profit"), ("Cierre", "Manual"), ("Manual", "Manual")):
            if k in reason or k in reason.lower():
                return lbl
        return reason if len(reason) <= 24 else "Otro"

    def _analytics(self):
        """Estadística completa del cerebro: equity, drawdown, expectancy,
        profit factor, rachas y desgloses por lado/par/razón."""
        from collections import OrderedDict

        cap = self.capital or 20.0
        groups = OrderedDict()
        order = []
        for t in self.history:
            key = (t["pair"], t["opened_at"], t["entry_time"])
            if key not in groups:
                g = {"pair": t["pair"], "side": t["side"], "entry": t["entry"],
                     "opened_at": t["opened_at"], "entry_time": t["entry_time"],
                     "pnl": 0.0, "size": 0.0, "partials": 0, "closed": False,
                     "bars_held": int(t.get("bars_held", 0)), "reason": None}
                groups[key] = g
            g = groups[key]
            g["pnl"] += t["pnl"]
            g["size"] = max(g["size"], t["size_usd"])
            g["bars_held"] = int(t.get("bars_held", 0))
            if t.get("partial"):
                g["partials"] += 1
            else:
                g["closed"] = True
                g["reason"] = t["reason"]
        logical = list(groups.values())

        closes = [g for g in logical if g["closed"]]
        n_trades = len(logical)
        wins = [g for g in closes if g["pnl"] >= 0]
        losses = [g for g in closes if g["pnl"] < 0]
        total_pnl = round(sum(g["pnl"] for g in logical), 4)
        gross_win = sum(g["pnl"] for g in wins)
        gross_loss = -sum(g["pnl"] for g in losses)
        if gross_loss > 1e-9:
            pfac = round(gross_win / gross_loss, 2)
        elif gross_win > 1e-9:
            pfac = None            # sin pérdidas: profit factor infinito
        else:
            pfac = 0.0
        avg_win = round(gross_win / len(wins), 4) if wins else 0.0
        avg_loss = round(-gross_loss / len(losses), 4) if losses else 0.0
        rets = [g["pnl"] / cap for g in closes]
        mean_r = sum(rets) / len(rets) if rets else 0.0
        if len(rets) > 1:
            var = sum((r - mean_r) ** 2 for r in rets) / (len(rets) - 1)
            sharpe = round(mean_r / (var ** 0.5), 3) if var > 1e-12 else None
        else:
            sharpe = None

        # drawdown sobre la curva de equity
        curve = self.equity_curve
        peak = cap
        max_dd = 0.0
        max_dd_pct = 0.0
        dd_series = []
        for _ts, eq in curve[-800:]:
            if eq > peak:
                peak = eq
            dd = peak - eq
            ddp = (dd / peak * 100.0) if peak > 1e-9 else 0.0
            dd_series.append(round(ddp, 3))
            if ddp > max_dd_pct:
                max_dd_pct = ddp
            if dd > max_dd:
                max_dd = dd
        cur_eq = curve[-1][1] if curve else self.cash
        while dd_series and dd_series[-1] < 1e-9:
            dd_series.pop()
        cur_dd = round(dd_series[-1], 3) if dd_series else 0.0
        
        # rachas (operaciones lógicas cerradas, en orden de cierre)
        best_w = best_l = cur_s = 0
        cur_type = None
        for g in closes:
            if g["pnl"] >= 0:
                if cur_type == "w":
                    cur_s += 1
                else:
                    cur_type, cur_s = "w", 1
            else:
                if cur_type == "l":
                    cur_s += 1
                else:
                    cur_type, cur_s = "l", 1
            if cur_type == "w" and cur_s > best_w:
                best_w = cur_s
            if cur_type == "l" and cur_s > best_l:
                best_l = cur_s
        cur_w = cur_s if cur_type == "w" else 0
        cur_l = cur_s if cur_type == "l" else 0

        by_side = {}
        for g in logical:
            d = by_side.setdefault(g["side"], {"trades": 0, "wins": 0, "pnl": 0.0})
            d["trades"] += 1
            if g["closed"] and g["pnl"] >= 0:
                d["wins"] += 1
            d["pnl"] = round(d["pnl"] + g["pnl"], 4)
        for d in by_side.values():
            d["win_rate"] = round(d["wins"] / d["trades"] * 100, 1) if d["trades"] else 0.0
            d["pnl"] = round(d["pnl"], 4)

        by_reason = {}
        for g in closes:
            b = self._reason_bucket(g["reason"])
            d = by_reason.setdefault(b, {"reason": b, "n": 0, "pnl": 0.0})
            d["n"] += 1
            d["pnl"] = round(d["pnl"] + g["pnl"], 4)
        reasons_out = sorted(by_reason.values(), key=lambda d: -d["n"])

        per_pair = {}
        for g in logical:
            d = per_pair.setdefault(g["pair"], {"pair": g["pair"], "trades": 0, "wins": 0, "pnl": 0.0})
            d["trades"] += 1
            if g["closed"] and g["pnl"] >= 0:
                d["wins"] += 1
            d["pnl"] += g["pnl"]
        pairs_out = sorted(per_pair.values(), key=lambda d: -d["pnl"])
        for d in pairs_out:
            d["win_rate"] = round(d["wins"] / d["trades"] * 100, 1)
            d["pnl"] = round(d["pnl"], 4)

        # score compuesto 0-100 (confianza por tamaño de muestra)
        pf_score = 1.0 if pfac is None else (0.0 if pfac <= 0 else max(0.0, min(1.0, (pfac - 0.5) / 1.5)))
        wr = (len(wins) / len(closes) * 100.0) if closes else 0.0
        wr_score = max(0.0, min(1.0, (wr - 30.0) / 40.0))
        dd_score = max(0.0, min(1.0, 1.0 - max_dd_pct / 20.0))
        exp_pct = (mean_r * 100.0) if rets else 0.0
        exp_score = max(0.0, min(1.0, (exp_pct + 1.0) / 6.0))
        conf = max(0.0, min(1.0, n_trades / 25.0))
        score = round(100.0 * (0.28 * pf_score + 0.24 * wr_score + 0.24 * dd_score + 0.24 * exp_score)
                      * (0.55 + 0.45 * conf))
        score = max(0, min(100, score))
        grade = ("A+" if score >= 85 else "A" if score >= 70 else "B" if score >= 55
                 else "C" if score >= 40 else "D") if n_trades else "-"

        # serie de equity para la gráfica (downsample a ~300 puntos)
        step = max(1, len(curve) // 300)
        eq_out = curve[::step]
        if curve and (not eq_out or eq_out[-1][0] != curve[-1][0]):
            eq_out.append(curve[-1])

        return {
            "score": score, "grade": grade, "trades": n_trades,
            "tiles": {
                "profit_factor": pfac, "expectancy": round(mean_r * cap, 4),
                "expectancy_pct": round(exp_pct, 2), "win_rate": round(wr, 1),
                "max_dd": round(max_dd, 4), "max_dd_pct": round(max_dd_pct, 2),
                "cur_dd_pct": cur_dd, "sharpe": sharpe,
                "total_pnl": total_pnl, "wins": len(wins), "losses": len(losses),
                "avg_win": avg_win, "avg_loss": avg_loss,
                "best": round(max((g["pnl"] for g in logical), default=0.0), 4),
                "worst": round(min((g["pnl"] for g in logical), default=0.0), 4),
                "avg_bars_held": round(sum(g["bars_held"] for g in closes) / len(closes), 1) if closes else 0.0,
            },
            "equity": eq_out,
            "dd": dd_series[-300:] if len(dd_series) > 300 else dd_series,
            "trade_list": [{"pair": g["pair"], "side": g["side"], "pnl": round(g["pnl"], 4),
                            "reason": g["reason"], "partials": g["partials"]} for g in logical[-48:]][::-1],
            "by_side": by_side,
            "by_reason": reasons_out,
            "per_pair": pairs_out,
            "streak": {"w": cur_w, "l": cur_l, "best_w": best_w, "best_l": best_l},
        }

    def _gestion_note(self, pos):
        if not pos:
            return "Sin posición abierta."
        parts = [pos.get("side", "LONG")]
        parts.append("TP1 ✓ · BE" if pos.get("tp1_done") else "TP1 pendiente")
        if pos.get("trail") is not None:
            parts.append("trailing $" + str(pos["trail"]))
        parts.append("SL $" + str(pos["sl"]))
        parts.append("TP $" + str(pos["tp"]))
        if pos.get("leverage", 1) > 1:
            parts.append("x" + str(pos["leverage"]))
        return " · ".join(parts)

    def _pulse(self, m, active_pos):
        """Estado del pulso de Laya: los 5 pasos del ciclo con ok/detalle.
        El dashboard lo renderiza tal cual (pasos Mercado→Gestión)."""
        ind = m.indicators or {}
        strn = (m.diagnostics.get("strength") if isinstance(m.diagnostics, dict) else None) or {}
        sl = float(strn.get("long", 0) or 0)
        ss = float(strn.get("short", 0) or 0)
        strength = max(sl, ss)
        quant = (m.diagnostics.get("quant") if isinstance(m.diagnostics, dict) else None) or {}
        min_sc = int(float(self._st("min_signal_score") or 0))
        cd = self.cooldown.get(self.active_pair, 0)
        futs = self._futures_on()
        rsi_txt = "--" if ind.get("rsi") is None else f"{ind['rsi']:.1f}"
        atr_txt = "--" if not ind.get("atr") else f"{ind['atr']:.6g}"
        dec = m.signal if m.signal != "NONE" else "NONE — sin confluencia"
        if cd > 0:
            dec += " · cooldown " + str(cd) + "b"
        if m.signal == "SHORT" and not futs:
            dec += " · ignora: spot"
        steps = [
            {"k": "Mercado", "ok": bool(m.candles),
             "v": "Precio $" + f"{m.price:,.2f}" + " · velas " + str(len(m.candles)) + " · feed " + m.feed},
            {"k": "Indicadores", "ok": ind.get("rsi") is not None and ind.get("ema_fast") is not None,
             "v": "EMA " + str(ind.get("ema_fast")) + "/" + str(ind.get("ema_slow"))
                  + " · RSI " + rsi_txt + " · ATR " + atr_txt},
            {"k": "Filtro", "ok": strength >= 0.67,
             "v": "L " + str(int(sl * 100)) + "% · S " + str(int(ss * 100)) + "% · umbral 67%"
                  + " · score L " + ("--" if quant.get("long") is None else str(int(quant["long"])))
                  + " / S " + ("--" if quant.get("short") is None else str(int(quant["short"])))
                  + " (admite >= " + str(min_sc) + ")"},
            {"k": "Decisión", "ok": m.signal in ("LONG", "SHORT"), "v": dec},
            {"k": "Gestión", "ok": active_pos is not None, "v": self._gestion_note(active_pos)},
        ]
        return {"cycle": self.cycle, "signal": m.signal, "strength": round(strength, 2),
                "cooldown": cd, "futures": futs, "steps": steps}

    def snapshot_detail(self):
        m = self.markets[self.active_pair]
        eq, unreal, pos_val = self.equity()
        wins = [t for t in self.history if t["pnl"] >= 0]
        candles, emaF, emaS, rsiS, volS = m.series()
        active_pos = self.positions.get(self.active_pair)
        orders = []
        positions_out = []
        for pair, pos in self.positions.items():
            mm = self.markets[pair]
            price = mm.price
            is_long = pos.get("side", "LONG") == "LONG"
            side_lbl = "sell" if is_long else "buy"
            qty = float(pos["qty"])
            margin = float(pos.get("size_usd") or 0.0)
            entry = float(pos["entry"])
            lev = pos.get("leverage", 1) or 1
            if is_long:
                unpnl = qty * price - margin
            else:
                unpnl = (entry - price) * qty
                unpnl = max(unpnl, -margin)
            positions_out.append({
                "pair": pair, "side": pos.get("side", "LONG"), "entry": pos["entry"],
                "qty": pos["qty"], "size_usd": pos["size_usd"], "sl": pos["sl"],
                "tp": pos["tp"], "price": price, "pnl": round(unpnl, 4),
                "notional": pos.get("notional"), "leverage": pos.get("leverage", 1),
                "liq": pos.get("liq"), "trail": pos.get("trail"),
                "tp1_price": pos.get("tp1_price"), "tp1_done": bool(pos.get("tp1_done")),
                "bars_held": int(pos.get("bars_held", 0)),
                "opened_at": pos.get("opened_at"),
            })

            def _ord(ttype, level, q_exec, state):
                """Orden de salida con monto operable y métricas del nivel:
                usd = q_exec * nivel (monto operado si toca), pnl_level =
                PnL $ vs entrada si ese nivel ejecuta, dist_pct = % que el
                precio debe viajar para disparar la orden."""
                o = {"type": ttype, "side": side_lbl, "pair": pair, "state": state,
                     "pside": pos.get("side", "LONG"), "margin": round(margin, 2),
                     "leverage": lev, "pos_pnl": round(unpnl, 4),
                     "pos_qty": round(qty, 8), "qty_exec": round(q_exec, 8),
                     "trail_active": pos.get("trail") is not None,
                     "bars_held": int(pos.get("bars_held", 0))}
                if level:
                    o["price"] = level
                    o["usd"] = round(q_exec * level, 2)
                    if ttype == "Liquidación":
                        o["pnl_level"] = -margin          # la liq consume justo el margen
                    else:
                        o["pnl_level"] = round(((level - entry) if is_long
                                                else (entry - level)) * q_exec, 4)
                    o["dist_pct"] = round((level / price - 1.0) * 100.0, 2) if price else 0.0
                else:
                    o["price"] = None
                    o["usd"] = 0.0
                    o["pnl_level"] = 0.0
                    o["dist_pct"] = 0.0
                return o

            orders.append(_ord("Stop Loss", pos.get("trail") or pos["sl"], qty,
                               "activo (trailing)" if pos.get("trail") is not None else "pendiente"))
            orders.append(_ord("Take Profit", pos["tp"], qty, "pendiente"))
            if not pos.get("tp1_done"):
                q1 = qty * float(self._st("tp1_size_pct"))
                orders.append(_ord("TP1 parcial", pos.get("tp1_price"), q1, "pendiente"))
            if pos.get("liq"):
                orders.append(_ord("Liquidación", pos.get("liq"), qty,
                                   "futuros x" + str(lev)))
        return {
            "id": self.id, "name": self.name, "sim_mode": self.mode, "pairs": self.pairs,
            "pair": m.pair, "pair_name": PAIRS[m.pair][0], "active_pair": self.active_pair,
            "status": self.status, "started_at": self.started_at, "cycle": self.cycle,
            "price": m.price, "signal": m.signal, "signal_note": m.signal_note,
            "indicators": m.indicators, "diagnostics": m.diagnostics,
            "feed": m.feed, "pulse": self._pulse(m, active_pos),
            "analytics": self._analytics(),
            "journal": self._journal_summary(),
            "futures": self._futures_on(),
            "strategy": {
                "tp1_pct": self._st("tp1_pct"), "tp1_size_pct": self._st("tp1_size_pct"),
                "break_even": self._st("break_even"), "trail_atr_mult": self._st("trail_atr_mult"),
                "momentum_exit": self._st("momentum_exit"),
                "time_stop_bars": self._st("time_stop_bars"),
                "cooldown_bars": self._st("cooldown_bars"),
                "short_enabled": self._st("short_enabled"), "leverage": self._st("leverage"),
                "futures_paper": self._st("futures_paper"),
                "momentum_grace_bars": self._st("momentum_grace_bars"),
                "momentum_min_strength": self._st("momentum_min_strength"),
                "cooldown_loss_mult": self._st("cooldown_loss_mult"),
                "max_ext_atr": self._st("max_ext_atr"),
                "min_body_ratio": self._st("min_body_ratio"),
                "sl_slip_atr": self._st("sl_slip_atr"),
                "tp_sl_mode": self._st("tp_sl_mode"),
                "sl_atr_mult": self._st("sl_atr_mult"),
                "tp_rr": self._st("tp_rr"),
                "tp1_rr_frac": self._st("tp1_rr_frac"),
                "min_sl_pct": self._st("min_sl_pct"),
                "max_sl_pct": self._st("max_sl_pct"),
                "min_signal_score": self._st("min_signal_score"),
                "preset": self.params.get("preset", "standard"),
            },
            "portfolio": {
                "initial_capital": self.capital,
                "cash": round(self.cash, 4),
                "equity": round(eq, 4),
                "position_value": round(pos_val, 4),
                "reserved_margin": round(sum(float(pos.get("size_usd") or 0.0)
                                             for pos in self.positions.values()), 4),
                "unrealized_pnl": round(unreal, 4),
                "realized_pnl": round(self.realized, 4),
                "total_pnl": round(self.realized + unreal, 4),
                "daily_realized": round(self.daily, 4),
                "max_daily_loss": self.params.get("max_daily_loss", 0),
                "fees_paid": round(float(getattr(self, "fees_paid", 0.0) or 0.0), 4),
                "trades_count": len(self.history),
                "wins": len(wins),
                "win_rate": round(len(wins) / len(self.history) * 100, 1) if self.history else 0,
            },
            "position": active_pos,
            "positions": positions_out,
            "active_orders": orders,
            "history": list(reversed(self.history[-MAX_HISTORY:])),
            "tasks": self.tasks[:60],
            "candles": candles,
            "ema_fast_series": emaF,
            "ema_slow_series": emaS,
            "rsi_series": rsiS,
            "volume_series": volS,
        }

    def summary(self):
        eq, unreal, pos_val = self.equity()
        wins = sum(1 for t in self.history if t.get("pnl", 0) > 0)
        trades = len(self.history)
        win_rate = (wins / trades * 100) if trades > 0 else 0
        gross_profit = sum(t["pnl"] for t in self.history if t.get("pnl", 0) > 0)
        gross_loss = abs(sum(t["pnl"] for t in self.history if t.get("pnl", 0) < 0))
        profit_factor = (gross_profit / gross_loss) if gross_loss > 0 else (float('inf') if gross_profit > 0 else 0)
        return {
            "id": self.id, "name": self.name, "mode": self.mode, "pairs": self.pairs,
            "active_pair": self.active_pair, "status": self.status,
            "capital": self.capital, "cash": round(self.cash, 2),
            "equity": round(eq, 2), "pnl": round(self.realized + unreal, 2),
            "realized": round(self.realized, 2), "trades": len(self.history),
            "wins": wins,
            "win_rate": round(win_rate, 1),
            "profit_factor": round(profit_factor, 2) if profit_factor != float('inf') else None,
            "positions": len(self.positions), "cycle": self.cycle,
            "futures": self._futures_on(), "cooldown": sum(1 for v in self.cooldown.values() if v > 0),
        }


# ----------------------------------------------------------------------------
# Gestor de simuladores (bucle compartido + control + snapshot)
# ----------------------------------------------------------------------------

class Manager:
    def __init__(self, cfg):
        self.cfg = cfg
        self.mode = cfg.get("mode", "paper")
        self.tf_label = cfg.get("timeframe", "5m")
        if self.tf_label not in TIMEFRAMES:
            self.tf_label = "1m"
        try:
            self.tick_interval = max(0.2, float(cfg.get("tick_interval", TICK_INTERVAL)))
        except Exception:
            self.tick_interval = TICK_INTERVAL
        ind = cfg.get("indicators", {})
        self.params = {
            "capital0": float(cfg.get("capital_usd", 20)),
            "max_pos": float(cfg.get("max_position_usd", 5)),
            "sl_pct": float(cfg.get("sl_pct", 0.025)),
            "tp_pct": float(cfg.get("tp_pct", 0.05)),
            "max_daily_loss": float(cfg.get("max_daily_loss_usd", 2)),
            "max_open": int(cfg.get("max_trades_open", 5)),
            "ema_fast": int(ind.get("ema_fast", 9)),
            "ema_slow": int(ind.get("ema_slow", 21)),
            "rsi_period": int(ind.get("rsi_period", 14)),
            "vol_factor": float(ind.get("volume_factor", 1.2)),
            "tf_seconds": TIMEFRAMES[self.tf_label],
        }
        # Bloque "strategy": pulido del pulso + salidas estratégicas +
        # futuros (SHORT paper). Claves planas en params para todos los sims.
        self.params.update(sanitize_strategy(cfg.get("strategy", {})))
        # Bloque "fees": comisiones REALES de Binance por operación (VIP0:
        # spot taker 0.1% = 0.001, USDT-M futuros taker 0.05% = 0.0005,
        # editables en vivo). Reserva neta al cerrar: al abrir solo se ANOTA
        # el fee (pos.fee_open / fee_open_left) y el cash no se toca.
        fees_cfg = cfg.get("fees") if isinstance(cfg.get("fees"), dict) else {}
        self.params["fees_enabled"] = bool(fees_cfg.get("enabled", fees_cfg.get("fees_enabled", True)))
        try:
            self.params["fee_spot"] = max(0.0, min(float(fees_cfg.get("spot_pct", 0.001)), 0.01))
        except Exception:
            self.params["fee_spot"] = 0.001
        try:
            self.params["fee_futs"] = max(0.0, min(float(fees_cfg.get("futures_pct", 0.0005)), 0.01))
        except Exception:
            self.params["fee_futs"] = 0.0005
        # Tasas MAKER (orden límite: así sale el TP/TP1 en real). En USDT-M la
        # distancia maker/taker es real (0.02% vs 0.05%); en spot VIP0 son
        # iguales pero se configuran por si hay VIP/BNB que la baje.
        try:
            self.params["fee_maker_spot"] = max(0.0, min(float(fees_cfg.get("maker_spot_pct", fees_cfg.get("spot_pct", 0.001))), 0.01))
        except Exception:
            self.params["fee_maker_spot"] = self.params.get("fee_spot", 0.001)
        try:
            self.params["fee_maker_futs"] = max(0.0, min(float(fees_cfg.get("maker_futures_pct", 0.0002)), 0.01))
        except Exception:
            self.params["fee_maker_futs"] = 0.0002
        # Bloque "context": capa fundamental en vivo (F&G, titulares, macro
        # global, sesgo MTF). "filter" activa el bloqueo de entradas con F&G
        # extremo sin confluencia multi-temporal (off = neutro para los tests).
        ctx_cfg = cfg.get("context") if isinstance(cfg.get("context"), dict) else {}
        self.params["ctx_enabled"] = bool(ctx_cfg.get("enabled", ctx_cfg.get("ctx_enabled", True)))
        if "filter" in ctx_cfg or "ctx_filter" in ctx_cfg:
            self.params["ctx_filter"] = bool(ctx_cfg.get("filter", ctx_cfg.get("ctx_filter")))
        # Contexto compartido: TODOS los sims leen este dict por referencia.
        self.context = {"updated_at": None, "fng": {}, "global": {}, "news": {}, "mtf": {}}
        self._ctx_ts = {}      # clave -> timestamp del último refresco
        self._ctx_lock = threading.Lock()
        self.available_pairs = [{"symbol": s, "name": PAIRS[s][0]} for s in PAIRS]
        # Vista de gráfico multi-temporal (solo dashboard /api/chart):
        # (pair, tf) -> {"candles": [...], "src": etiqueta, "ts": último refresco}
        self._chart_hist = {}
        # Ticks de precio para animar la vela en formación (~2 Hz, cache compartida)
        # (kind, pair) -> (timestamp, precio)
        self._ticker_cache = {}
        # precio real de mercado (data-api.binance.vision) para animar el tick
        # cuando el testnet queda congelado; {pair: (ts, price)}
        self._ticker_real = {}

        # --- System 2: Deep Analysis Engine ---
        self.deep_engine = None
        deep_cfg = cfg.get("deep_analysis", {})
        if DEEP_ANALYSIS_AVAILABLE and deep_cfg.get("enabled", False):
            try:
                self.deep_engine = DeepAnalysisEngine(cfg)
                if self.deep_engine.enabled:
                    print(f"[DEEP] System 2 activado: {self.deep_engine.provider}/{self.deep_engine.model}")
                else:
                    self.deep_engine = None
                    print("[DEEP] System 2 desactivado (sin API key o error init)")
            except Exception as e:
                self.deep_engine = None
                print(f"[DEEP] Error inicializando: {e}")
        else:
            print("[DEEP] System 2 no disponible (módulo no cargado o disabled en config)")

        # --- System 2: Rate Limiting (configurable via config.json deep_analysis.rate_limits) ---
        rate_cfg = deep_cfg.get("rate_limits", {})
        self._deep_rate = {
            "calls_minute": 0,
            "minute_window": time.time(),
            "calls_day": 0,
            "day_window": int(time.time() // 86400) * 86400,
            "pair_cooldown": {},      # pair -> last_call_timestamp
            "pair_cache": {},         # pair -> (signal, result, timestamp)
            "cooldown_sec": int(rate_cfg.get("pair_cooldown_sec", 60)),
            "max_per_minute": int(rate_cfg.get("max_per_minute", 15)),
            "max_per_day": int(rate_cfg.get("max_per_day", 1500)),
        }
        self._deep_lock = threading.Lock()
        self._deep_busy = False   # guard: solo un análisis manual a la vez

        # Pasar deep_engine y rate limiter a params para que los simuladores puedan acceder
        self.params["deep_engine"] = self.deep_engine
        self.params["deep_rate"] = self._deep_rate
        self.params["deep_lock"] = self._deep_lock

        # Origen de datos: 'synthetic' (mercado simulado) | 'spot' | 'futures'
        # (velas REALES de testnet; la cartera sigue siendo paper/simulada y
        # nunca se colocan órdenes reales).
        self.data_source = "synthetic"
        self._heal_ts = {}       # autosembrado de feeds: último intento por (kind|par)
        self.params["futures_enabled"] = False   # feed real de futuros (SHORT xN en ambos lados)
        self._tn_symbols = {"spot": None, "futures": None}
        self._tn_fail = {}

        self.simulators = {}
        self.active_id = None
        self.lock = threading.RLock()
        self.cond = threading.Condition(self.lock)
        self.wake = threading.Event()
        self.shutdown = False
        self.version = 0

        # Crear 4 simuladores élite con $400 capital (total $1600)
        # Eliminado ETH Scalp (bajo rendimiento: 55% WR, PF 1.21, fees 78%)
        # Re-parametrizados ETH Trend y BTC Scalp para mayor consistencia
        self.create_sim("sim-1", "ETH Solo", 50, ["ETHUSDT"])         # 62.8% WR, PF 1.85
        self.create_sim("sim-2", "ETH Trend", 50, ["ETHUSDT"])        # Re-param: swing conservador
        self.create_sim("sim-3", "BTC Solo", 50, ["BTCUSDT"])         # 63.4% WR, PF 1.63
        self.create_sim("sim-4", "BTC Scalp", 50, ["BTCUSDT"])        # Re-param: scalp moderado
        self.active_id = "sim-1"
        self.simulators["sim-1"].add_task("system", "Servidor listo. Pulsa Activar.")

    # -- creación ---------------------------------------------------------
    def create_sim(self, sid, name, capital, pairs):
        pairs = [p for p in pairs if p in PAIRS]
        if not pairs:
            pairs = ["BTCUSDT"]
        try:
            capital = float(capital)
        except Exception:
            capital = 20.0
        if capital <= 0:
            capital = 20.0
        sim = Simulator(sid, name, capital, pairs, self.params)
        # Registrar PRIMERO: los presets (_apply_*) buscan el sim por sid.
        # Preset por nombre (Trend / Scalp): cada sim opera con COPIA propia
        # de params y registra sus claves 'propias'.
        self.simulators[sid] = sim
        self._apply_preset(sim)
        # Capa de contexto compartida (por referencia: el daemon del Manager la llena
        # y TODOS los sims leen el mismo dict en su puerta de entrada).
        sim.context = self.context
        sim.add_task("system", "Simulador creado con $" + f"{capital:.2f}" + " en " + ", ".join(pairs) + ".")
        return sim

    def create_sim_auto(self, name, capital, pairs):
        with self.cond:
            n = 1
            while "sim-" + str(n) in self.simulators:
                n += 1
            sid = "sim-" + str(n)
            sim = self.create_sim(sid, name, capital, pairs)
            self.active_id = sid
            self.version += 1
            self.cond.notify_all()
        self.wake.set()
        self._seed_sim_testnet(sim)
        return sim

    # -- control ----------------------------------------------------------
    def control(self, sid, action, pair=""):
        with self.cond:
            sim = self.simulators.get(sid)
            if sim is None:
                return {"ok": False, "error": "Simulador no encontrado"}
            if action == "start":
                if sim.status == "stopped":
                    if sim.started_at is None:
                        sim.started_at = now_iso()
                    sim.add_task("system", "Simulador ACTIVADO.")
                elif sim.status == "paused":
                    sim.add_task("system", "Simulador REANUDADO.")
                sim.status = "running"
            elif action == "pause":
                if sim.status == "running":
                    sim.status = "paused"
                    sim.add_task("system", "Simulador PAUSADO.")
            elif action == "stop":
                if sim.status in ("running", "paused"):
                    for pair in list(sim.positions.keys()):
                        sim.close(pair, sim.markets[pair].price, "Parada manual")
                    sim.status = "stopped"
                    sim.add_task("system", "Simulador PARADO.")
            elif action == "flatten" or action == "close-positions":
                # Cierra todas las posiciones a precio de mercado SIN detener
                # el agente (sigue buscando setups al instante).
                if not sim.positions:
                    return {"ok": False, "error": "Sin posiciones abiertas"}
                for pair_f in list(sim.positions.keys()):
                    sim.close(pair_f, sim.markets[pair_f].price, "Cierre manual")
                sim.add_task("system", "Posiciones cerradas manualmente; el agente sigue operativo.")
            elif action == "remove_pair":
                # Retira un par del universo de trading (sin posición abierta
                # en él). El Market queda como vista de solo lectura.
                if pair not in sim.pairs:
                    return {"ok": False, "error": sim.name + " no opera " + str(pair)}
                if pair in sim.positions:
                    return {"ok": False, "error": "Cierra la posición en " + pair +
                            " (acción flatten) antes de retirar el par"}
                if len(sim.pairs) <= 1:
                    return {"ok": False, "error": sim.name + " necesita al menos un par"}
                sim.pairs.remove(pair)
                sim.mode = "multi" if len(sim.pairs) > 1 else "single"
                sim.cooldown.pop(pair, None)
                sim.cooldown_till.pop(pair, None)
                sim.add_task("system", "Par retirado: " + pair + ". " + sim.name
                             + " ahora opera " + ", ".join(sim.pairs) + ".")
                if sim.active_pair == pair:
                    sim.active_pair = sim.pairs[0]
            elif action == "reset":
                capital, name, pairs = sim.capital, sim.name, sim.pairs
                fresh = Simulator(sid, name, capital, pairs, self.params)
                fresh.add_task("system", "Simulador REINICIADO a valores iniciales.")
                # El journal es memoria de aprendizaje del sim: sobrevive al reset
                # (las estadísticas de drift parten del historial completo).
                fresh.journal = list(sim.journal)
                fresh._journal_keys = set(sim._journal_keys)
                fresh._drift_closes = sim._drift_closes
                self.simulators[sid] = fresh
                sim = fresh
            else:
                return {"ok": False, "error": "Acción desconocida: " + str(action)}
            self.version += 1
            self.cond.notify_all()
        self.wake.set()
        if action == "reset":
            self._seed_sim_testnet(self.simulators.get(sid))
        return {"ok": True, "id": sid, "status": sim.status}

    def set_balance(self, sid, capital):
        with self.cond:
            sim = self.simulators.get(sid)
            if sim is None:
                return {"ok": False, "error": "Simulador no encontrado"}
            if sim.status != "stopped":
                return {"ok": False, "error": "Detén el simulador antes de cambiar el saldo"}
            try:
                cap = float(capital)
            except Exception:
                return {"ok": False, "error": "Saldo inválido"}
            if cap <= 0:
                return {"ok": False, "error": "El saldo debe ser mayor que 0"}
            name, pairs = sim.name, sim.pairs
            fresh = Simulator(sid, name, cap, pairs, self.params)
            fresh.add_task("system", "Saldo cambiado a $" + f"{cap:.2f}" + " (simulador reiniciado).")
            self.simulators[sid] = fresh
            self.version += 1
            self.cond.notify_all()
        self.wake.set()
        self._seed_sim_testnet(self.simulators.get(sid))
        return {"ok": True, "id": sid, "capital": cap}

    def select(self, sid):
        with self.cond:
            if sid not in self.simulators:
                return {"ok": False, "error": "Simulador no encontrado"}
            self.active_id = sid
            self.version += 1
            self.cond.notify_all()
        self.wake.set()
        return {"ok": True, "id": sid}

    def delete(self, sid):
        with self.cond:
            if sid not in self.simulators:
                return {"ok": False, "error": "Simulador no encontrado"}
            if len(self.simulators) <= 1:
                return {"ok": False, "error": "Debe quedar al menos un simulador"}
            del self.simulators[sid]
            if self.active_id == sid:
                self.active_id = next(iter(self.simulators))
            self.version += 1
            self.cond.notify_all()
        self.wake.set()
        return {"ok": True}

    def switch_pair(self, sid, pair):
        """Cambia el par VISIBLE del chart (active_pair). Solo cambia el
        universo de trading si el par ya pertenece a sim.pairs: ver el gráfico
        de otro par NO lo añade a la estrategia (antes, solo con seleccionar
        un par en el selector el simulador empezaba a operarlo, y "ETH Solo"
        terminaba con posiciones en BTCUSDT)."""
        with self.cond:
            sim = self.simulators.get(sid)
            if sim is None:
                return {"ok": False, "error": "Simulador no encontrado"}
            if pair not in PAIRS:
                return {"ok": False, "error": "Par desconocido: " + str(pair)}
            added_market = False
            if pair not in sim.markets:
                # Mercado de solo lectura: velas para el chart, pero NO entra
                # en sim.pairs (el tick solo evalúa pares de sim.pairs).
                sim.markets[pair] = Market(pair, self.params)
                added_market = True
                if pair not in sim.pairs:
                    sim.add_task("system", "Vista cambiada a " + pair + " (solo gráfico: "
                                 + sim.name + " sigue operando " + ", ".join(sim.pairs) + ").")
            sim.active_pair = pair
            self.version += 1
            self.cond.notify_all()
        self.wake.set()
        if added_market:
            self._seed_sim_testnet(sim, pairs=[pair])
        return {"ok": True, "id": sid, "pair": pair, "pairs": list(sim.pairs)}

    def set_timeframe(self, label):
        with self.cond:
            if label not in TIMEFRAMES:
                return {"ok": False, "error": "Periodo no válido: " + str(label)}
            self.tf_label = label
            self.params["tf_seconds"] = TIMEFRAMES[label]
            for sim in self.simulators.values():
                for m in sim.markets.values():
                    if m.feed == "synthetic":
                        m.regenerate_tf()
            self.version += 1
            self.cond.notify_all()
        self.wake.set()
        if self.data_source != "synthetic":
            # Re-sembrar los mercados con velas reales del nuevo intervalo
            self._apply_feed_to_all()
            with self.cond:
                self.version += 1
                self.cond.notify_all()
        return {"ok": True, "timeframe": label}

    # -- configuración en vivo ---------------------------------------------
    def apply_preset_profile(self, name):
        """Aplica un perfil agentico (conservative, standard, aggressive) a toda la flota
        y a los parametros compartidos. Actualiza self.cfg, params y recalcula senales."""
        name = (name or "").strip().lower()
        if name not in AGENTIC_PRESETS:
            return {"ok": False, "error": f"Preset desconocido: {name}. Opciones: {list(AGENTIC_PRESETS.keys())}"}
        
        preset_info = AGENTIC_PRESETS[name]
        params_to_apply = preset_info["params"]
        
        # Guardar seleccion en config
        self.cfg["preset"] = name
        
        # Aplicar mediante update_config a nivel global
        res = self.update_config(params_to_apply)
        res["preset"] = name
        res["preset_name"] = preset_info["name"]
        res["preset_description"] = preset_info["description"]
        
        # Actualizar directamente cada simulador de la flota
        with self.cond:
            for sim in self.simulators.values():
                sim.params["preset"] = name
                for k, v in params_to_apply.items():
                    pname = CONFIG_PARAM.get(k, k)
                    # Mantener el sizing de cartera proporcional en simuladores multi-activo
                    if len(sim.pairs) > 1 and pname in ("max_pos", "max_open"):
                        continue
                    sim.params[pname] = v
                if hasattr(sim, "_ai_sl_tp_adjust"):
                    sim._ai_sl_tp_adjust.clear()
                for m in sim.markets.values():
                    try:
                        m.recompute()
                        m.signal = m.evaluate()
                        m.signal_note = ("Sin senal valida (Laya System 1)."
                                         if m.signal == "NONE"
                                         else "Senal " + m.signal + " detectada.")
                    except Exception:
                        pass
                sim.add_task("system", f"Perfil agentico aplicado: {preset_info['name']}")
            self.version += 1
            self.cond.notify_all()
        self.wake.set()
        return res

    def update_config(self, updates):
        """Aplica parámetros editables en vivo (riesgo, estrategia, indicadores,
        velocidad y capital). Efecto inmediato en TODOS los simuladores porque
        comparten el mismo dict params por referencia. Persiste en config.json
        vía save_config() desde la ruta Flask."""
        if not isinstance(updates, dict):
            return {"ok": False, "error": "Payload debe ser un objeto"}
        applied, errors = {}, []
        with self.cond:
            for key, raw in updates.items():
                if key not in CONFIG_SPEC:
                    errors.append("Clave no editable: " + str(key))
                    continue
                typ, lo, hi = CONFIG_SPEC[key]
                try:
                    if typ == "bool":
                        val = bool(raw)
                    elif typ == "int":
                        val = int(round(float(raw)))
                    elif typ == "str":
                        val = str(raw).strip().lower()
                    else:
                        val = float(raw)
                except (TypeError, ValueError):
                    errors.append("Valor inválido para " + str(key) + ": " + str(raw))
                    continue
                if lo is not None and val < lo:
                    val = lo if typ != "int" else int(lo)
                if hi is not None and val > hi:
                    val = hi if typ != "int" else int(hi)
                applied[key] = val
                # 1) dict params compartido -> efecto inmediato en todos los sims
                if key == "tick_interval":
                    self.tick_interval = val
                elif key == "capital_usd":
                    # capital0 solo aplica a resets/nuevos sims; no toca el cash vivo
                    self.params["capital0"] = val
                    self.cfg["capital_usd"] = val
                elif key == "deep_analysis_enabled":
                    # Se persiste dentro del bloque deep_analysis y se conmuta
                    # el motor System 2 en caliente (ver _set_deep_enabled).
                    self.cfg.setdefault("deep_analysis", {})["enabled"] = val
                    self._set_deep_enabled(val)
                else:
                    self.params[CONFIG_PARAM.get(key, key)] = val
                    # 2) espejo en self.cfg para que sanitize_config lo refleje
                    block = CONFIG_BLOCK.get(key)
                    if block:
                        self.cfg.setdefault(block, {})[key] = val
                    else:
                        self.cfg[key] = val
            # 3) Espejo por simulador: los sims tienen COPIA propia de params,
            #    así que cada cambio en vivo hay que trasladarlo a la copia de
            #    cada uno. FIX (Mejora 3): update_config AHORA SOBRESCRIBE las
            #    claves fijadas por preset (sim._own). Antes `if not in own`
            #    ignoraba en silencio p.ej. momentum_exit / sl_atr_mult /
            #    min_signal_score en sim-1,2,4,6 (sus presets las habían
            #    bloqueado), y los ajustes del dashboard no llegaban a 4 de 6
            #    sims. La intención del preset es solo el arranque: lo que el
            #    operador ajusta en vivo manda sobre toda la flota, y la clave
            #    sale de _own para que el preset deje de bloquearla.
            for sim in self.simulators.values():
                own = getattr(sim, "_own", None)
                for key, val in applied.items():
                    pname = CONFIG_PARAM.get(key, key)
                    if pname in sim.params:
                        sim.params[pname] = val
                        if own is not None and pname in own:
                            own.discard(pname)
            # Recalcular indicadores y señal con los nuevos parámetros
            if applied:
                for sim in self.simulators.values():
                    for m in sim.markets.values():
                        try:
                            m.recompute()
                            m.signal = m.evaluate()
                            m.signal_note = ("Sin señal válida (Laya System 1)."
                                             if m.signal == "NONE"
                                             else "Señal " + m.signal + " detectada.")
                        except Exception:
                            pass
                next(iter(self.simulators.values())).add_task(
                    "system", "Configuración actualizada en vivo: "
                    + ", ".join("%s=%s" % (k, v) for k, v in applied.items()) + ".")
                self.version += 1
                self.cond.notify_all()
        if applied:
            self.wake.set()
        out = {"ok": bool(applied), "applied": applied}
        if errors:
            out["errors"] = errors
        if not applied and errors:
            out["error"] = "; ".join(errors)
        return out

    # -- System 2 (Gemini): activación / desactivación en caliente ----------
    def _set_deep_enabled(self, val):
        """Conmutar System 2 (deep analysis / Gemini) en vivo, sin reiniciar.
        Los sims guardan una COPIA de params, así que:
        - si ya existe un motor, basta mutar `enabled` in-place: todos los sims
          comparten la MISMA referencia del objeto;
        - si no existe (nunca se creó o falló la API key) y val=True, se
          construye uno nuevo con la config vigente y se repliega a todas las
          copias de params de cada sim."""
        eng = self.params.get("deep_engine")
        if not val:
            if eng is not None:
                eng.enabled = False
                self.deep_engine = eng
            for sim in self.simulators.values():
                sim.add_task("deep", "System 2 desactivado desde configuración: "
                                     "solo deciden System 1 + capa cuant.")
            return
        new_eng = None
        if DEEP_ANALYSIS_AVAILABLE:
            try:
                # Asegurar bloque completo de deep_analysis en self.cfg
                da_cfg = self.cfg.setdefault("deep_analysis", {})
                da_cfg["enabled"] = True
                if not da_cfg.get("provider"): da_cfg["provider"] = "google"
                if not da_cfg.get("model"): da_cfg["model"] = "gemini-3.5-flash-lite"
                if not da_cfg.get("api_key_env"): da_cfg["api_key_env"] = "GEMINI_API_KEY"
                new_eng = DeepAnalysisEngine(self.cfg)
                if not new_eng.enabled:
                    new_eng = None   # sin API key o config inválida
            except Exception as e:
                new_eng = None
                print("[DEEP] Error recreando motor en toggle:", e)
        if new_eng is not None:
            self.deep_engine = new_eng
            self.params["deep_engine"] = new_eng
            for sim in self.simulators.values():
                sim.params["deep_engine"] = new_eng
        elif eng is not None:
            # motor existente con key válida: solo reactivarlo
            eng.enabled = True
            self.deep_engine = eng
        else:
            for sim in self.simulators.values():
                sim.add_task("deep", "System 2 no pudo activarse: GEMINI_API_KEY "
                                     "ausente o deep_analysis.provider inválido.")
            return
        for sim in self.simulators.values():
            sim.add_task("deep", "System 2 activado en caliente ("
                         + self.deep_engine.provider + "/" + self.deep_engine.model
                         + "): valida cada señal antes de operar.")

    # -- ticks de precio para el gráfico en tiempo real --------------------
    def ticker_price(self, pair, sub_min=False):
        """Tick de precio para animar la vela en formación del gráfico.
        Fuente primaria: testnet del feed activo (mismo precio que opera el
        sim). Problema conocido: el libro del testnet es artificial y puede
        mantener el precio congelado >= 1.2 s (o minutos). Cuando eso ocurre
        se anima con el precio REAL de mercado (data-api.binance.vision,
        misma fuente que usa el fallback de velas): el gráfico así refresca
        cada segundo. Cache 450 ms por fuente/par: los clientes comparten
        el mismo tick y no machacan la API."""
        kind = self._tn_kind()
        if kind not in ("spot", "futures"):
            kind = "spot"
        # Vista sub-minuto: las velas base son 1s de spot (futuros no las da)
        if sub_min and kind == "futures":
            kind = "spot"
        now = time.time()
        ent = None
        with self.cond:
            raw = self._ticker_cache.get((kind, pair))
            if raw is not None:
                ent = (raw[0], raw[1], raw[2]) if len(raw) == 3 else (raw[0], raw[1], raw[0])
        if (not ent) or now - ent[0] >= 0.45:
            try:
                path = ("/fapi/v1/ticker/price" if kind == "futures"
                        else "/v3/ticker/price")
                d = tn_http_get(kind, path, {"symbol": pair})
                price = float(d["price"])
                # moved_ts: momento en que el testnet cambió de valor por última vez
                moved = ent[2] if (ent and price == ent[1]) else now
                with self.cond:
                    self._ticker_cache[(kind, pair)] = (now, price, moved)
                ent = (now, price, moved)
            except Exception:
                if not ent:
                    return {"ok": False, "error": "sin tick de " + str(pair)}
        out_price, src = ent[1], kind
        # Testnet congelado >= 1.2 s -> animar con precio real de mercado
        if ent is not None and now - ent[2] >= 1.2:
            with self.cond:
                rent = self._ticker_real.get(pair)
            if (not rent) or now - rent[0] >= 0.45:
                try:
                    d = tn_http_get("real", "/v3/ticker/price", {"symbol": pair})
                    with self.cond:
                        self._ticker_real[pair] = (now, float(d["price"]))
                    rent = (now, float(d["price"]))
                except Exception:
                    rent = rent or None
            if rent is not None:
                out_price, src = rent[1], "real"
        return {"ok": True, "pair": pair, "price": out_price, "kind": src,
                "age_ms": int((time.time() - ent[0]) * 1000) if ent else 0}

    # -- origen de datos: simulado / spot testnet / futuros testnet --------
    def _tn_kind(self):
        return self.data_source if self.data_source in ("spot", "futures") else None

    # -- vista de gráfico multi-temporal (solo visualización, sin claves) --
    def chart_data(self, pair, tf, limit=600):
        """Velas REALES de testnet para el gráfico del dashboard con
        temporalidades desde 5s (agregadas de velas 1s nativas de spot /
        binance.vision). Refresco incremental por startTime para que el bin
        en formación se actualice a cada consulta. NO toca la simulación."""
        sec = CHART_TFS.get(tf)
        if sec is None:
            return {"error": "Temporalidad no soportada: " + str(tf)}
        limit = max(50, min(int(limit or 600), 1200))
        interval = CHART_BASE_INTERVAL[tf]
        sub_min = (interval == "")      # requiere velas base 1s + agregación
        base_interval = "1s" if sub_min else interval
        need = sec * limit if sub_min else limit
        need = min(need, 3400)          # tope razonable de velas base en memoria
        # La API de futuros no ofrece velas de 1s (solo desde 1m). Para la
        # VISTA sub-minuto se toman las 1s reales de spot (testnet spot o
        # binance.vision); >= 1m usa la temporalidad nativa de la fuente activa.
        kind = self._tn_kind()
        if sub_min and kind != "spot":
            kind = "spot"
        if kind is None:
            kind = "spot"
        with self.cond:
            entry = self._chart_hist.get((pair, tf))
            entry = dict(entry) if entry else None
            if entry is not None and entry.get("kind") != kind:
                entry = None
        now = time.time()
        base, src = [], ""
        if entry:
            base, src = entry["candles"], entry["src"]
        try:
            if base:
                # refresco incremental desde la última vela base conocida
                got = _chart_fetch_tail(kind, pair, base_interval,
                                        base[-1]["time"] * 1000,
                                        max(2, min(need - len(base) + 10, 1000)))
                if got:
                    seen = {c["time"] for c in base[-need:]}
                    base = (base + [c for c in got if c["time"] not in seen])[-need:]
                    src = TN_KINDS[kind]["label"]
            if len(base) < 30:
                # primera carga (o la fuente se quedó sin cola): fetch paginado
                base, src = fetch_klines_hist(kind, pair, base_interval, need)
        except Exception as e:
            if not base:
                return {"error": "sin velas " + pair + " " + tf + ": " + str(e)[:90]}
        if not base:
            return {"error": "sin velas " + pair + " " + tf}
        out = _chart_aggregate(base, sec) if sub_min else base
        with self.cond:
            self._chart_hist[(pair, tf)] = {"candles": base, "src": src, "ts": now,
                                            "interval": base_interval, "kind": kind}
            if len(self._chart_hist) > 24:
                for k in sorted(self._chart_hist, key=lambda k: self._chart_hist[k]["ts"])[:-16]:
                    self._chart_hist.pop(k, None)
        last_t = out[-1]["time"] if out else 0
        return {"pair": pair, "tf": tf, "source": src, "interval": base_interval,
                "live": bool(last_t and now - last_t < sec + 3),
                "candles": out[-limit:], "updated_at": now_iso()}

    def _tn_symbols_refresh(self, kind, force=False):
        self._tn_symbols[kind] = tn_supported_symbols(kind)

    def set_data_source(self, source):
        if source not in ("synthetic", "spot", "futures"):
            return {"ok": False, "error": "Origen no válido: " + str(source)}
        with self.cond:
            if source == self.data_source:
                return {"ok": True, "source": source}
            self.data_source = source
            # Feed real de futuros: apalancamiento xN también en LONG.
            # (el SHORT paper sin feed lo habilita strategy.futures_paper)
            self.params["futures_enabled"] = (source == "futures")
        notes = self._apply_feed_to_all()
        with self.cond:
            self.version += 1
            self.cond.notify_all()
        self.wake.set()
        return {"ok": True, "source": source, "notes": notes}

    def _apply_feed_to_all(self):
        """Ajusta el feed de los mercados de todos los simuladores y resiembra
        los reales con velas de testnet. Los fetches de red se hacen SIN
        mantener el lock para no bloquear el bucle ni las conexiones SSE."""
        notes = []
        kind = self._tn_kind()
        if kind is None:
            with self.cond:
                for sim in self.simulators.values():
                    for m in sim.markets.values():
                        m.feed = "synthetic"
            notes.append("Mercado simulado reactivado en todos los pares.")
            return notes
        interval = BINANCE_TIMEFRAMES.get(self.tf_label)
        if interval is None:
            with self.cond:
                for sim in self.simulators.values():
                    for m in sim.markets.values():
                        m.feed = "synthetic"
            notes.append("El periodo " + self.tf_label + " no existe en testnet (solo 1m+); "
                         "los mercados siguen simulados.")
            return notes
        try:
            self._tn_symbols_refresh(kind)
            symbols = self._tn_symbols[kind]
        except Exception as e:
            notes.append(TN_KINDS[kind]["label"] + " no accesible (" + str(e)[:70] + "); mercados simulados.")
            return notes
        label = TN_KINDS[kind]["label"]
        with self.cond:
            wanted = sorted({p for sim in self.simulators.values() for p in sim.pairs})
        fetched = {}
        for p in wanted:
            if p not in symbols:
                continue
            try:
                fetched[p] = tn_klines(kind, p, interval, 500)
            except Exception as e:
                notes.append(p + ": fallo klines de " + label + " (" + str(e)[:60] + ") → simulado.")
        with self.cond:
            for sim in self.simulators.values():
                for p in sim.pairs:
                    m = sim.markets[p]
                    if p in fetched and fetched[p]:
                        m.set_real_feed(fetched[p], kind)
                        sim.add_task("system", "Datos REALES de " + label + " en " + p
                                     + " (" + interval + ", " + str(len(m.candles)) + " velas).")
                        if kind == "futures":
                            sim.add_task("system", "Modo FUTUROS: SHORT paper activado (x"
                                         + str(int(self._lev())) + ", cartera simulada, sin órdenes reales).")
                    else:
                        m.feed = "synthetic"
                        if p in symbols:
                            sim.add_task("system", p + ": testnet no devolvió velas → simulado.")
                        else:
                            sim.add_task("system", p + ": no cotiza en " + label + " → simulado.")
        return notes

    def _lev(self):
        try:
            return int(self.params.get("leverage", 5))
        except Exception:
            return 5

    def _seed_sim_testnet(self, sim, pairs=None):
        """Tras crear/reiniciar/cambiar saldo de un simulador, re-sembrar sus
        mercados con velas reales si el origen de datos es testnet."""
        kind = self._tn_kind()
        if sim is None or kind is None:
            return
        interval = BINANCE_TIMEFRAMES.get(self.tf_label)
        if interval is None:
            return
        targets = [p for p in (pairs or sim.pairs) if p in sim.markets]
        fetched = {}
        try:
            self._tn_symbols_refresh(kind)
            symbols = self._tn_symbols[kind]
        except Exception:
            return
        for p in targets:
            if p not in symbols:
                continue
            try:
                fetched[p] = tn_klines(kind, p, interval, 500)
            except Exception:
                fetched[p] = None
        with self.cond:
            for p in targets:
                m = sim.markets.get(p)
                if m is None:
                    continue
                if fetched.get(p) and m.set_real_feed(fetched[p], kind):
                    sim.add_task("system", "Datos REALES de " + TN_KINDS[kind]["label"]
                                 + " en " + p + " (" + interval + ").")
            self.version += 1
            self.cond.notify_all()

    def start_testnet_poller(self):
        threading.Thread(target=self._testnet_poll_loop, daemon=True).start()

    def _testnet_poll_loop(self):
        while not self.shutdown:
            time.sleep(TESTNET_POLL_INTERVAL)
            try:
                if self.data_source != "synthetic":
                    self._poll_testnet_once()
            except Exception as e:
                log("Poller testnet: error " + str(e))

    def _poll_testnet_once(self):
        interval = BINANCE_TIMEFRAMES.get(self.tf_label)
        if not interval:
            return 0
        # --- Autocuración: con origen testnet, ningún mercado debe quedarse
        #     sintético (antes la divergencia de precios: cada sim dibujaba
        #     su propio paseo aleatorio). Si un feed falló al sembrar,
        #     reintentamos con throttle de 90 s por (kind, par).
        heals = {}
        kind_main = self._tn_kind()
        if kind_main is not None:
            now_ts = time.time()
            with self.cond:
                for sim in self.simulators.values():
                    for p, m in sim.markets.items():
                        if m.feed == "synthetic" \
                                and now_ts - self._heal_ts.get(kind_main + p, 0.0) > 90.0:
                            heals.setdefault(kind_main, set()).add(p)
            for kind in list(heals):
                try:
                    self._tn_symbols_refresh(kind)
                    symbols = self._tn_symbols[kind]
                except Exception:
                    continue
                for sym in list(heals[kind]):
                    if sym not in symbols:
                        continue
                    try:
                        ks = tn_klines(kind, sym, interval, 500)
                    except Exception:
                        self._heal_ts[kind + sym] = time.time()
                        continue
                    if not ks:
                        continue
                    with self.cond:
                        for sim in self.simulators.values():
                            m = sim.markets.get(sym)
                            if m is not None and m.feed == "synthetic":
                                if m.set_real_feed(ks, kind):
                                    self._heal_ts[kind + sym] = time.time()
                                    sim.add_task("system", "Autocuración: feed REAL reestablecido en "
                                                 + sym + " (" + str(len(ks)) + " velas).")
                        self.version += 1
                        self.cond.notify_all()
        # Agrupar pares por kind (spot / futures coexisten por mercado)
        want = {"spot": set(), "futures": set()}
        with self.cond:
            for sim in self.simulators.values():
                for p, m in sim.markets.items():
                    if m.feed in want:
                        want[m.feed].add(p)
        got = {"spot": {}, "futures": {}}
        for kind, syms in want.items():
            if not syms:
                continue
            for sym in syms:
                try:
                    got[kind][sym] = tn_klines(kind, sym, interval, 3)
                    if self._tn_fail.pop(kind + sym, None):
                        log("Poller testnet: " + sym + " (" + kind + ") OK de nuevo.")
                except Exception as e:
                    if self._tn_fail.get(kind + sym) is not True:
                        self._tn_fail[kind + sym] = True
                        log("Poller testnet: " + sym + " (" + kind + ") fallo: " + str(e)[:90])
        with self.cond:
            n = 0
            for kind, cens in got.items():
                for sym, ks in cens.items():
                    if not ks:
                        continue
                    for sim in self.simulators.values():
                        m = sim.markets.get(sym)
                        if m is not None and m.feed == kind and m.apply_real_candles(ks):
                            n += 1
            if n:
                self.version += 1
                self.cond.notify_all()
        return n

    # -- snapshot ---------------------------------------------------------
    def _snapshot(self):
        sim = self.simulators.get(self.active_id)
        if sim is None and self.simulators:
            self.active_id = next(iter(self.simulators))
            sim = self.simulators[self.active_id]
        d = sim.snapshot_detail() if sim else {}
        d["version"] = self.version
        d["server_time"] = now_iso()
        d["mode"] = self.mode
        d["timeframe"] = self.tf_label
        d["data_source"] = self.data_source
        d["data_source_label"] = (TN_KINDS[self.data_source]["label"]
                                  if self.data_source in TN_KINDS else "Mercado simulado")
        d["available_pairs"] = self.available_pairs
        d["active_id"] = self.active_id
        d["tick_interval"] = self.tick_interval
        # Estado de System 2 para el pipeline del dashboard
        d["deep_enabled"] = bool(self.cfg.get("deep_analysis", {}).get("enabled", False))
        _de = self.params.get("deep_engine")
        d["deep_model"] = (_de.model if _de is not None
                           else self.cfg.get("deep_analysis", {}).get("model", ""))
        d["simulators"] = [s.summary() for s in self.simulators.values()]
        # Config saneada para poblar el panel de configuración en vivo
        try:
            d["config"] = sanitize_config(self.cfg)
        except Exception:
            d["config"] = {}
        # Context fundamental (F&G, news, macro, MTF) para el dashboard
        try:
            d["context"] = self.context_snapshot()
            print(f"[SNAPSHOT] Context included: {list(d['context'].keys())}")
        except Exception as e:
            print(f"[SNAPSHOT] Error getting context: {e}")
            d["context"] = {}
        return d

    def get_snapshot(self):
        with self.cond:
            return self._snapshot()

    # -- System 2 manual (botón del dashboard) -----------------------------
    def deep_run(self, pair="", signal=""):
        """Corre el análisis de System 2 (LLM) EN DEMANDA sobre el par activo
        y devuelve la nueva calificación (recomendación, confianza, razón).
        By-pasa el cooldown por-par (el usuario lo pide explícitamente) pero
        respeta los límites globales de tasa. No se retiene self.cond durante
        la llamada al LLM para no congelar el bucle de ticks; el veredicto
        queda cacheado por par igual que un análisis en vivo."""
        with self.cond:
            sim = self.simulators.get(self.active_id)
            if sim is None:
                return {"ok": False, "error": "no hay simulador activo"}
            engine = self.params.get("deep_engine") or getattr(self, "deep_engine", None)
            if not engine or not getattr(engine, "enabled", False):
                if DEEP_ANALYSIS_AVAILABLE:
                    try:
                        self._set_deep_enabled(True)
                        engine = self.params.get("deep_engine") or getattr(self, "deep_engine", None)
                    except Exception:
                        pass
            if not engine or not getattr(engine, "enabled", False):
                return {"ok": False, "error": "System 2 no está activado (Configuración → deep_analysis.enabled)"}
            pair = str(pair or sim.active_pair or "").upper()
            if pair not in sim.markets:
                # Si el par solicitado no está en este sim, buscar si otro sim lo tiene
                found_sim = None
                for other_s in self.simulators.values():
                    if pair in other_s.markets:
                        found_sim = other_s
                        break
                if found_sim is not None:
                    sim = found_sim
                else:
                    # Crear mercado al vuelo para este par para permitir el análisis
                    sim.markets[pair] = Market(pair, self.params)
                    self._seed_sim_testnet(sim, pairs=[pair])
            m = sim.markets[pair]
            q = (m.diagnostics or {}).get("quant") or {}
            if str(signal).upper() in ("LONG", "SHORT"):
                signal = str(signal).upper()
            else:
                ql = float(q.get("long") or 0)
                qs = float(q.get("short") or 0)
                signal = "SHORT" if qs > ql else "LONG"
            price_info = sim.build_deep_price_info(pair, signal)
            if price_info is None:
                return {"ok": False, "error": "sin datos de mercado para " + pair}
            # cupo: contadores bajo el lock de rate; el cooldown por-par se
            # refresca para que el filtro en vivo use este veredicto fresco
            rate = self._deep_rate
            now = time.time()
            with self._deep_lock:
                if getattr(self, "_deep_busy", False):
                    return {"ok": False, "error": "ya hay un análisis en curso"}
                if now - rate["minute_window"] >= 60:
                    rate["calls_minute"] = 0
                    rate["minute_window"] = now
                day_window = int(now // 86400) * 86400
                if day_window != rate["day_window"]:
                    rate["calls_day"] = 0
                    rate["day_window"] = day_window
                if rate["calls_minute"] >= rate["max_per_minute"]:
                    return {"ok": False, "error": "límite de tasa por minuto alcanzado (" + str(rate["max_per_minute"]) + "); espera un momento"}
                if rate["calls_day"] >= rate["max_per_day"]:
                    return {"ok": False, "error": "límite diario de System 2 alcanzado"}
                rate["calls_minute"] += 1
                rate["calls_day"] += 1
                rate["pair_cooldown"][pair] = now
                self._deep_busy = True
        try:
            import time as _t
            t0 = _t.time()
            result = engine.analyze(price_info)
            elapsed = int((_t.time() - t0) * 1000)
        except Exception as e:
            self._deep_busy = False
            sim.add_task("deep", "System 2 (manual) error: " + str(e)[:120])
            return {"ok": False, "error": "System 2 falló: " + str(e)[:200]}
        self._deep_busy = False
        if not result:
            err = getattr(engine, "last_error", "") or ("sin resultado para " + signal + " en " + pair)
            if "API key not valid" in err or "400" in err or "401" in err or "403" in err:
                msg = "API Key no válida o expirada. Configura GEMINI_API_KEY o desactiva System 2 en Config."
            else:
                msg = "System 2: " + err
            sim.add_task("deep", msg)
            return {"ok": False, "error": msg}
        rec = result.get("recomendacion_laya", "WAIT")
        conf = float(result.get("confianza", 0.0) or 0.0)
        reason = result.get("razon_corta", "")
        approved = (rec == signal and conf >= 0.5)
        msg = ("System 2 [manual] %s: %s (conf %.0f%%) — %s • señal %s %s"
               % (pair, rec, conf * 100.0, reason[:80], signal,
                  "✓ coherente" if approved else "✗ discrepa/bloquea"))
        sim.add_task("deep", msg)
        # cache para que el filtro en vivo reutilice este veredicto recién hecho
        if conf > 0.0 and rec != "WAIT":
            with self._deep_lock:
                self._deep_rate["pair_cache"][pair] = (signal, result, time.time())
        return {"ok": True, "pair": pair, "signal": signal,
                "result": result, "elapsed_ms": elapsed, "approved": approved}

    # -- bucle ------------------------------------------------------------
    def _loop(self):
        while not self.shutdown:
            self.wake.wait(timeout=self.tick_interval)
            self.wake.clear()
            with self.cond:
                any_running = False
                for sim in self.simulators.values():
                    if sim.status == "running":
                        any_running = True
                        try:
                            loss = sim.tick()
                        except Exception as e:
                            sim.add_task("system", "Error en ciclo: " + str(e))
                            loss = False
                        if loss:
                            sim.status = "paused"
                            sim.add_task("risk", "Límite de pérdida diaria alcanzado. Simulador pausado.")
                if any_running:
                    self.version += 1
                    self.cond.notify_all()

    def start_loop(self):
        threading.Thread(target=self._loop, daemon=True).start()

    # -- capa de contexto (fundamental en tiempo real, sin claves ni pagos) --
    def context_snapshot(self):
        with self.cond:
            return dict(self.context)

    def start_context_layer(self):
        if self.params.get("ctx_enabled", True):
            threading.Thread(target=self._context_loop, daemon=True,
                             name="laya-context").start()

    def _context_loop(self):
        """Daemon del contexto: F&G cada 10 min, macro global cada 10 min,
        titulares RSS cada 5 min y sesgo multi-temporal por par cada 5 min.
        Todas las fuentes son públicas (sin keys) y toleran caídas."""
        while not self.shutdown:
            try:
                self.context_refresh()
            except Exception:
                pass
            for _ in range(20):
                if self.shutdown:
                    return
                time.sleep(1)

    def context_refresh(self):
        """Refresca solo lo caducado (TTL por fuente). La red se hace FUERA del
        lock para no bloquear el bucle ni las conexiones SSE; una fuente caída
        conserva su último dato marcado como stale."""
        now = time.time()
        work = []
        for k, ttl in (("fng", FNG_TTL), ("global", GLOBAL_TTL), ("news", NEWS_TTL)):
            if not self.context.get(k) or now - self._ctx_ts.get(k, 0.0) >= ttl:
                work.append(k)
        if not self.context.get("mtf") or now - self._ctx_ts.get("mtf", 0.0) >= MTF_TTL:
            work.append("mtf")
        if not work:
            self.context["updated_at"] = now_iso()
            return
        with self.cond:
            pairs = sorted({p for sim in self.simulators.values() for p in sim.pairs})[:4]

        fetched = {}
        if "fng" in work:
            try:
                fetched["fng"] = self._ctx_fetch_fng()
            except Exception as e:
                fetched["fng"] = (dict(self.context.get("fng") or {}, stale=True)
                                  if self.context.get("fng") else {"error": str(e)[:70]})
        if "global" in work:
            try:
                fetched["global"] = self._ctx_fetch_global()
            except Exception as e:
                fetched["global"] = (dict(self.context.get("global") or {}, stale=True)
                                     if self.context.get("global") else {"error": str(e)[:70]})
        if "news" in work:
            try:
                fetched["news"] = self._ctx_fetch_news()
            except Exception as e:
                fetched["news"] = (dict(self.context.get("news") or {}, stale=True)
                                   if self.context.get("news") else {"error": str(e)[:70]})
        mtf_out = {}
        if "mtf" in work:
            kind = self._tn_kind() or "spot"
            for p in pairs:
                old = (self.context.get("mtf") or {}).get(p)
                try:
                    cl, _src = fetch_klines_hist(kind, p, "1h", 160)
                    d = mtf_trend_from_candles(cl)
                    d["timeframe"] = "1h"
                    d["updated"] = now_iso()
                    mtf_out[p] = d
                except Exception as e:
                    mtf_out[p] = (dict(old, stale=True) if old
                                  else {"trend": "SIN DATOS", "error": str(e)[:55]})
        with self.cond:
            for k, v in fetched.items():
                self.context[k] = v
                self._ctx_ts[k] = time.time()
            if mtf_out:
                mm = dict(self.context.get("mtf") or {})
                mm.update(mtf_out)
                self.context["mtf"] = mm
                self._ctx_ts["mtf"] = time.time()
            self.context["updated_at"] = now_iso()

    def _ctx_fetch_fng(self):
        """Fear & Greed Index (crypto total) - alternative.me, sin claves."""
        r = _tn_session.get("https://api.alternative.me/fng/", params={"limit": 1}, timeout=8)
        if r.status_code != 200:
            raise RuntimeError("HTTP " + str(r.status_code))
        arr = (r.json() or {}).get("data") or []
        if not arr:
            raise RuntimeError("sin datos")
        v = int(arr[0]["value"])
        return {"value": v, "label": fng_label_es(v),
                "label_src": str(arr[0].get("value_classification") or "")}

    def _ctx_fetch_global(self):
        """Macro global del mercado crypto - CoinGecko /global (gratuito)."""
        r = _tn_session.get("https://api.coingecko.com/api/v3/global", timeout=8)
        if r.status_code != 200:
            raise RuntimeError("HTTP " + str(r.status_code))
        d = (r.json() or {}).get("data") or {}
        mc = float(d.get("total_market_cap", {}).get("usd", 0) or 0)
        unit = "T" if mc >= 1e12 else ("B" if mc >= 1e9 else "M")
        scale = 1e12 if unit == "T" else (1e9 if unit == "B" else 1e6)
        pct = d.get("market_cap_percentage") or {}
        return {"mcap": "$" + "{:,.2f}".format(mc / scale) + unit,
                "change_24h": round(float(d.get("market_cap_change_percentage_24h_usd", 0) or 0), 2),
                "btc_dom": round(float(pct.get("btc", 0) or 0), 1),
                "eth_dom": round(float(pct.get("eth", 0) or 0), 1)}

    def _ctx_fetch_news(self):
        """Titulares RSS (CoinDesk + Cointelegraph) -> sentimiento léxico."""
        titles = []
        for url in ("https://www.coindesk.com/arc/outboundfeeds/rss/",
                    "https://cointelegraph.com/rss"):
            try:
                r = _tn_session.get(url, timeout=8)
                if r.status_code != 200:
                    continue
                root = ET.fromstring(r.content)
                for it in root.iter("item"):
                    t = " ".join(str(it.findtext("title") or "").split())
                    if t:
                        titles.append(t)
            except Exception:
                continue
            if len(titles) >= 20:
                break
        if not titles:
            raise RuntimeError("sin titulares accesibles")
        score = score_news_titles(titles)
        return {"sentiment": score, "label": sentiment_class(score),
                "count": len(titles), "samples": titles[:5]}

    # -- scalper params override ---------------------------------------------
    # -- preset de params por simulador (por nombre) -------------------------
    def _apply_preset(self, sim):
        """Elige el preset según el nombre del sim. Cada sim trabaja con COPIA
        propia de params (fix: antes todos compartían el dict y el último
        preset contaminaba a toda la flota)."""
        n = (sim.name or "").lower()
        if len(sim.pairs) > 1 or "multi" in n:
            self._apply_multi_crypto_params(sim.id)
        elif "trend" in n:
            self._apply_trend_params(sim.id)
        elif "scalp" in n:
            self._apply_scalper_moderate_params(sim.id)
        # los "Solo" usan la copia de los params globales afinados (sin override)

    def _apply_multi_crypto_params(self, sid):
        """Aplica parámetros de cartera multi-activo para operar varias monedas
        simultáneamente dividiendo el riesgo de forma equilibrada."""
        sim = self.simulators.get(sid)
        if not sim:
            return
        n_pairs = max(len(sim.pairs), 2)
        # Calcular tamaño de posición proporcional al capital y número de monedas
        # Para $50 y 4 pares: $10.0 por trade, hasta 4 trades simultáneos
        pos_usd = round(max(5.0, min(sim.capital / n_pairs, 15.0)), 2)
        self._ov(sim, {
            "tp_sl_mode": "atr",
            "leverage": 1,
            "max_pos": pos_usd,
            "max_open": n_pairs,
            "max_daily_loss": round(sim.capital * 0.15, 2),  # 15% drawdown max
            "sl_atr_mult": 1.6,
            "tp_rr": 2.5,
            "tp1_rr_frac": 0.5,
            "tp1_size_pct": 0.35,
            "break_even": True,
            "trail_atr_mult": 2.0,
            "momentum_exit": False,
            "momentum_grace_bars": 7,
            "momentum_min_strength": 75,
            "cooldown_bars": 4,
            "cooldown_loss_mult": 2.5,
            "max_ext_atr": 1.25,
            "min_body_ratio": 0.25,
            "maker_entry": True,
            "min_signal_score": 70,
            "ctx_filter": True,
        })
        sim.add_task("system", f"Parámetros MULTI-CRYPTO ({n_pairs} pares simultáneos, ${pos_usd}/pos, 1x) aplicados a {sid}.")

    def _ov(self, sim, overrides):
        """Aplica un preset a la COPIA de params del sim y marca las claves
        como 'propias': los ajustes en vivo del dashboard no las pisan."""
        sim.params.update(overrides)
        sim._own.update(overrides.keys())
        # Recalcular indicadores y señal con los parámetros del preset
        for m in sim.markets.values():
            m.recompute()
            m.signal = m.evaluate()
            m.signal_note = ("Sin señal válida (Laya System 1)."
                             if m.signal == "NONE"
                             else "Señal " + m.signal + " detectada.")
        sim.add_task("system", "Preset de estrategia aplicado a " + sim.name + ".")

    def _apply_scalper_params(self, sid):
        """Aplica parámetros de scalping selectivo a un simulador específico.
        Scalping BTC/BNB: SL/TP equilibrados, trailing ceñido, filtro MTF y score mínimo 62."""
        sim = self.simulators.get(sid)
        if not sim:
            return
        # Sobreescribir params del simulador (no afecta a self.params global)
        self._ov(sim, {
            # Riesgo ultra-ajustado
            "sl_pct": 0.012,        # fallback en modo "fixed" (1.2% SL)
            "tp_pct": 0.025,        # fallback modo "fixed" (R:R ~2:1)
            # Capa cuant: targets por ATR (reemplaza a los % fijos)
            "tp_sl_mode": "atr",
            "leverage": 1,
            "sl_atr_mult": 1.6,
            "tp_rr": 2.5,
            "tp1_rr_frac": 0.45,
            "min_sl_pct": 0.003,
            "max_sl_pct": 0.02,
            "min_signal_score": 70,
            "max_pos": 10.0,        # $10 por trade
            "max_daily_loss": 5.0, # 5% límite diario
            "max_open": 1,
            # Trailing selectivo
            "trail_atr_mult": 1.4,  # Menos propenso a whip
            "trail_activation_pct": 0.008,  # Activa a +0.8%
            "trail_step_atr": 0.4,  # Step pequeño
            "trail_max_pct": 0.04,
            # Salidas rápidas
            "tp1_pct": 0.012,       # fallback modo "fixed"
            "tp1_size_pct": 0.35,   # 35% en TP1 (antes 50%: asimetría negativa post-TP1)
            "break_even": True,
            "momentum_exit": False,
            "momentum_grace_bars": 7,   # Gracia suficiente contra falsos rebotes
            "momentum_min_strength": 85,
            "time_stop_bars": 24,   # 2h para dejar desarrollar
            "cooldown_bars": 4,
            "cooldown_loss_mult": 2.5,
            # Filtros activos para mejorar WR
            "max_ext_atr": 1.25,
            "min_body_ratio": 0.25,
            "sl_slip_atr": 0.15,
            "ctx_filter": True,    # Filtro MTF activo
            "btc_corr_filter": False,
            "vol_confirm_factor": 0.7,
            "vol_factor": 1.05,
        })
        sim.add_task("system", "Parámetros SCALPER aplicados a " + sid + " (BTCUSDT)")

    # -- trend params override (ETH Trend: swing conservador) ---------------
    def _apply_trend_params(self, sid):
        """Aplica parámetros conservadores de swing trading a ETH Trend.
        Objetivo: mayor WR, menos trades, mejor PF. Filtros estrictos."""
        sim = self.simulators.get(sid)
        if not sim:
            return
        self._ov(sim, {
            # Riesgo moderado
            "sl_pct": 0.025,        # fallback modo "fixed" (2.5% SL)
            "tp_pct": 0.06,         # fallback modo "fixed" (R:R 2.4:1)
            # Capa cuant: swing paciente, score de calidad alto
            "tp_sl_mode": "atr",
            "leverage": 1,
            "sl_atr_mult": 1.6,
            "tp_rr": 2.4,
            "tp1_rr_frac": 0.45,
            "min_sl_pct": 0.004,
            "max_sl_pct": 0.03,
            "min_signal_score": 68,
            "max_pos": 10.0,        # $10 por trade
            "max_daily_loss": 5.0, # 5% límite diario
            "max_open": 1,
            # Trailing más amplio para dejar correr ganancias
            "trail_atr_mult": 2.5,
            "trail_activation_pct": 0.015,
            "trail_step_atr": 0.7,
            "trail_max_pct": 0.10,
            # TP1 conservador
            "tp1_pct": 0.03,        # fallback modo "fixed"
            "tp1_size_pct": 0.35,
            "break_even": True,
            "momentum_exit": False,
            "momentum_grace_bars": 12,  # Paciencia en swing
            "momentum_min_strength": 75,
            "time_stop_bars": 48,       # 4h: el swing necesita margen para desarrollar
            "cooldown_bars": 6,         # Cooldown mayor
            "cooldown_loss_mult": 3.0,
            # Filtros estrictos
            "max_ext_atr": 1.25,        # No perseguir extensiones (fresh trend, no tardía)
            "min_body_ratio": 0.4,      # Velas con cuerpo
            "sl_slip_atr": 0.25,
            "ctx_filter": True,         # Filtro contexto activo
            "btc_corr_filter": True,
            "vol_confirm_factor": 0.8,  # Confirmación volumen estricta
            "vol_factor": 1.2,
        })
        sim.add_task("system", "Parámetros TREND conservadores aplicados a " + sid + " (ETHUSDT)")

    # -- scalper moderate params override (BTC Scalp: scalp moderado) -------
    def _apply_scalper_moderate_params(self, sid):
        """Aplica parámetros de scalping moderado a BTC Scalp.
        Objetivo: equilibrio velocidad/consistencia. Menos agresivo que ultra."""
        sim = self.simulators.get(sid)
        if not sim:
            return
        self._ov(sim, {
            # Riesgo ajustado pero no extremo
            "sl_pct": 0.015,        # fallback modo "fixed" (1.5% SL)
            "tp_pct": 0.03,         # fallback modo "fixed" (R:R 2:1)
            # Capa cuant: scalp moderado sobre ATR
            "tp_sl_mode": "atr",
            "leverage": 1,
            "sl_atr_mult": 1.35,
            "tp_rr": 2.0,
            "tp1_rr_frac": 0.45,
            "min_sl_pct": 0.003,
            "max_sl_pct": 0.02,
            "min_signal_score": 64,
            "max_pos": 10.0,        # $10 por trade
            "max_daily_loss": 5.0, # 5% límite diario
            "max_open": 1,
            # Trailing moderado
            "trail_atr_mult": 1.6,
            "trail_activation_pct": 0.010,
            "trail_step_atr": 0.5,
            "trail_max_pct": 0.05,
            # TP1 rápido
            "tp1_pct": 0.015,       # fallback modo "fixed"
            "tp1_size_pct": 0.35,
            "break_even": True,
            "momentum_exit": False,
            "momentum_grace_bars": 7,
            "momentum_min_strength": 75,
            "time_stop_bars": 32,   # 32 velas 5m (~2.6h)
            "cooldown_bars": 4,
            "cooldown_loss_mult": 2.5,
            # Filtros equilibrados
            "max_ext_atr": 1.25,
            "min_body_ratio": 0.3,
            "sl_slip_atr": 0.2,
            "ctx_filter": True,    # Filtro MTF activo
            "btc_corr_filter": False,
            "vol_confirm_factor": 0.75,
            "vol_factor": 1.1,
        })
        sim.add_task("system", "Parámetros SCALPER MODERADO aplicados a " + sid + " (BTCUSDT)")

# ----------------------------------------------------------------------------
# Backtesting paper: velas reales históricas por el MISMO circuito en vivo
# ----------------------------------------------------------------------------

class Backtester:
    """Backtest de Laya System 1 con dinero ficticio y velas reales.
    replay() es PURA (offline, permite velas sinteticas para tests); run()
    descarga el histórico sin claves (fetch_klines_hist) y llama a replay.
    El backtest usa su propio Simulator fuera del Manager: no toca el estado
    de los agentes vivos ni sim_state.json, y aplica las comisiones reales."""

    @staticmethod
    def replay(candles, pair="BTCUSDT", timeframe="5m", kind="spot",
               params=None, capital=20.0, label="local"):
        if len(candles) < 30:
            return {"ok": False, "error": "pocas velas para el backtest"}
        pars = dict(params or {})
        pars["tf_seconds"] = TIMEFRAMES.get(timeframe, 300)
        try:
            capital = float(capital)
        except Exception:
            capital = 20.0
        sim = Simulator("backtest", "Backtest", capital, [pair], pars)
        sim.status = "running"
        sim.context = {}          # en offline no hay F&G: la puerta ctx queda inerte
        m = sim.markets[pair] if pair in sim.markets else next(iter(sim.markets.values()))
        pair = m.pair
        m.feed = kind             # velas inyectadas a mano; Market.tick no-op
        n = len(candles)
        seed_n = max(2, min(60, n // 8))
        m.candles = [dict(c) for c in candles[:seed_n]]
        m.price = m.candles[-1]["close"]
        m.recompute()
        m.signal = m.evaluate()
        daily_stops = 0
        for c in candles[seed_n:]:
            m.candles.append(dict(c))
            if len(m.candles) > MAX_CANDLES:
                m.candles = m.candles[-MAX_CANDLES:]
            m.price = c["close"]
            m.recompute()
            m.signal = m.evaluate()
            if sim.tick():
                daily_stops += 1   # anota que el límite diario habría pausado al vivo
        if pair in sim.positions:
            sim.close(pair, m.price, "Cierre de fin de backtest")
        an = sim._analytics()
        tiles = an.get("tiles", {})
        eq_final = sim.equity()[0]
        return {
            "ok": True, "pair": pair, "timeframe": timeframe, "kind": kind,
            "source": label, "candles": n,
            "capital": round(capital, 2),
            "equity_final": round(eq_final, 4),
            "pnl": tiles.get("total_pnl", 0.0),
            "trades": an.get("trades", 0),
            "wins": tiles.get("wins", 0), "losses": tiles.get("losses", 0),
            "win_rate": tiles.get("win_rate", 0.0),
            "profit_factor": tiles.get("profit_factor"),
            "expectancy_pct": tiles.get("expectancy_pct", 0.0),
            "max_dd_pct": tiles.get("max_dd_pct", 0.0),
            "sharpe": tiles.get("sharpe"),
            "fees": round(float(getattr(sim, "fees_paid", 0.0)), 4),
            "open_positions": len(sim.positions),
            "daily_stops": daily_stops,
            "score": an.get("score", 0), "grade": an.get("grade", "-"),
            "by_reason": an.get("by_reason", []),
            "trade_list": (an.get("trade_list") or [])[:10],
            "equity": an.get("equity", []),
            "streak": an.get("streak", {}),
        }

    @staticmethod
    def run(manager, pair, timeframe, n_candles, kind):
        tf = TIMEFRAMES.get(timeframe)
        interval = BINANCE_TIMEFRAMES.get(timeframe)
        if tf is None or interval is None:
            return {"ok": False, "error": "Periodo no soportado por testnet: " + str(timeframe)}
        try:
            n_candles = max(300, min(int(n_candles), BACKTEST_MAX_VELAS))
        except Exception:
            n_candles = 1000
        with manager.cond:
            params = dict(manager.params)
        try:
            candles, source = fetch_klines_hist(kind, pair, interval, n_candles)
        except Exception as e:
            return {"ok": False, "error": "No se pudieron descargar velas: " + str(e)[:130]}
        if len(candles) < 100:
            return {"ok": False,
                    "error": "Histórico insuficiente en " + str(source) + " ("
                             + str(len(candles)) + " velas)"}
        return Backtester.replay(candles, pair, timeframe, kind,
                                 params=params, capital=params.get("capital0", 20.0),
                                 label=source)


# ----------------------------------------------------------------------------
# Flask
# ----------------------------------------------------------------------------

cfg = load_config()
print(f"[STARTUP] Server version: 2026-11-06-improvements - MTF fresh-trend filters (ctx_filter+RSI range+max_ext 1.25), TP1 35%, SL 1.6xATR, update_config overrides presets, post-SL cooldown 4x2.5")
manager = Manager(cfg)
app = Flask(__name__, static_folder=None)

# Simple CORS support for headless browser
def add_cors_headers(response):
    response.headers['Access-Control-Allow-Origin'] = '*'
    response.headers['Access-Control-Allow-Methods'] = 'GET, POST, OPTIONS'
    response.headers['Access-Control-Allow-Headers'] = 'Content-Type'
    return response

app.after_request(add_cors_headers)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))


@app.route("/")
def index():
    return send_from_directory(BASE_DIR, "dashboard.html")


@app.route("/vendor/<path:filename>")
def vendor_assets(filename):
    return send_from_directory(os.path.join(BASE_DIR, "vendor"), filename)


@app.route("/favicon.ico")
def favicon_ico():
    return ("", 204)


@app.route("/api/state")
def api_state():
    return jsonify(manager.get_snapshot())


@app.route("/api/debug/snapshot")
def api_debug_snapshot():
    """Debug endpoint to test snapshot with context."""
    snap = manager.get_snapshot()
    return jsonify({
        "has_context": "context" in snap,
        "context_keys": list(snap.get("context", {}).keys()) if "context" in snap else [],
        "manager_context_keys": list(manager.context.keys()),
        "snapshot_keys": list(snap.keys()),
    })


@app.route("/api/preset", methods=["GET", "POST"])
def api_preset():
    global cfg
    if request.method == "POST":
        data = request.get_json(silent=True) or {}
        name = data.get("preset", "standard")
        result = manager.apply_preset_profile(name)
        if result.get("ok"):
            cfg = manager.cfg
            cfg["preset"] = name
            save_config(cfg)
            save_persisted()
        return jsonify(result), (200 if result.get("ok") else 400)
    return jsonify({
        "current": cfg.get("preset", "standard"),
        "presets": {k: {"name": v["name"], "description": v["description"], "params": v["params"]} for k, v in AGENTIC_PRESETS.items()}
    })


@app.route("/api/config", methods=["GET", "POST"])
def api_config():
    global cfg
    if request.method == "POST":
        data = request.get_json(silent=True) or {}
        result = manager.update_config(data)
        if result.get("ok"):
            # Persistir en config.json (sobrevive a reinicios)
            cfg = manager.cfg
            save_config(cfg)
            save_persisted()
        return jsonify(result), (200 if result.get("ok") else 400)
    return jsonify(sanitize_config(cfg))


@app.route("/api/control", methods=["POST"])
def api_control():
    data = request.get_json(silent=True) or {}
    result = manager.control(manager.active_id, data.get("action", ""))
    if result.get("ok"):
        save_persisted()
    return jsonify(result), (200 if result.get("ok") else 400)


@app.route("/api/pair", methods=["POST"])
def api_pair():
    data = request.get_json(silent=True) or {}
    result = manager.switch_pair(manager.active_id, data.get("pair", ""))
    if result.get("ok"):
        save_persisted()
    return jsonify(result), (200 if result.get("ok") else 400)


@app.route("/api/timeframe", methods=["POST"])
def api_timeframe():
    data = request.get_json(silent=True) or {}
    result = manager.set_timeframe(data.get("timeframe", ""))
    if result.get("ok"):
        save_persisted()
    return jsonify(result), (200 if result.get("ok") else 400)


@app.route("/api/data-source", methods=["POST"])
def api_data_source():
    data = request.get_json(silent=True) or {}
    result = manager.set_data_source(data.get("source", ""))
    if result.get("ok"):
        save_persisted()
    return jsonify(result), (200 if result.get("ok") else 400)


@app.route("/api/chart")
def api_chart():
    """Gráfico de visualización con temporalidades cortas (5s..1D).
    Velas REALES del testnet activo (o binance.vision). Solo lectura:
    no afecta la temporalidad con la que el agente opera."""
    pair = str(request.args.get("pair") or "").upper()
    tf = str(request.args.get("tf") or "5m")
    try:
        limit = int(request.args.get("limit") or 600)
    except Exception:
        limit = 600
    with manager.cond:
        active = manager.simulators.get(manager.active_id)
        if not pair and active:
            pair = active.active_pair
    if not pair:
        pair = manager.cfg.get("pair", "BTCUSDT")
    try:
        result = manager.chart_data(pair, tf, limit)
    except Exception as e:
        return jsonify({"ok": False, "error": "chart: " + str(e)[:120]}), 500
    if result.get("error"):
        return jsonify({"ok": False, "error": result["error"]}), 502
    result["ok"] = True
    return jsonify(result)


@app.route("/api/ticker")
def api_ticker():
    """Tick de precio ultraligero (cache 450 ms) para animar la vela en
    formación del gráfico sin recargar velas. `tf` en segundos hace que,
    en vista sub-minuto con fuente futuros, se use el precio spot (misma
    fuente que las velas 1s del gráfico)."""
    pair = str(request.args.get("pair") or "").upper()
    tf = str(request.args.get("tf") or "")
    sub = tf.endswith("s")
    with manager.cond:
        active = manager.simulators.get(manager.active_id)
        if not pair and active:
            pair = active.active_pair
    if not pair:
        pair = manager.cfg.get("pair", "BTCUSDT")
    return jsonify(manager.ticker_price(pair, sub_min=sub))


@app.route("/api/backtest", methods=["POST"])
def api_backtest():
    """Backtest paper con velas REALES históricas (sin claves, sin órdenes).
    {pair, timeframe, velas, kind: spot|futures} -> estadística completa."""
    data = request.get_json(silent=True) or {}
    tf = str(data.get("timeframe") or "")
    kind = str(data.get("kind") or (manager.data_source
                                    if manager.data_source in ("spot", "futures") else "spot"))
    if kind not in ("spot", "futures"):
        kind = "spot"
    pair = str(data.get("pair") or "").upper()
    if not pair:
        with manager.cond:
            active = manager.simulators.get(manager.active_id)
            pair = active.active_pair if active else "BTCUSDT"
    if pair not in PAIRS:
        return jsonify({"ok": False, "error": "Par no soportado: " + pair}), 400
    if tf not in BINANCE_TIMEFRAMES:
        return jsonify({"ok": False,
                        "error": "Periodo no soportado por testnet (usa 1m, 5m, 15m, 1h, 4h o 1D): "
                                 + (tf or "(vacío)")}), 400
    try:
        velas = int(data.get("velas") or 1000)
    except Exception:
        velas = 1000
    velas = max(300, min(velas, BACKTEST_MAX_VELAS))
    try:
        result = Backtester.run(manager, pair, tf, velas, kind)
    except Exception as e:
        result = {"ok": False, "error": "Error de backtest: " + str(e)[:120]}
    if result.get("ok"):
        with manager.cond:
            active = manager.simulators.get(manager.active_id)
            if active:
                active.add_task("system", "Backtest " + pair + " " + tf + ": "
                                + str(result.get("candles", 0)) + " velas (" + str(result.get("source"))
                                + ") → trades " + str(result.get("trades", 0))
                                + ", PnL $" + f"{result.get('pnl', 0):.2f}"
                                + ", comisiones $" + f"{result.get('fees', 0):.3f}"
                                + ", score " + str(result.get("score")) + ".")
        return jsonify(result)
    return jsonify(result), 400


@app.route("/api/context")
def api_context():
    """Contexto del agente: Fear & Greed, macro global, sentimiento de
    noticias y sesgo multi-temporal por par (fuentes públicas, sin claves)."""
    return jsonify(manager.context_snapshot())


@app.route("/api/sim/select", methods=["POST"])
def api_sim_select():
    data = request.get_json(silent=True) or {}
    result = manager.select(data.get("id", ""))
    return jsonify(result), (200 if result.get("ok") else 400)


@app.route("/api/sim/create", methods=["POST"])
def api_sim_create():
    data = request.get_json(silent=True) or {}
    name = data.get("name") or "Simulador"
    capital = data.get("capital", 20)
    pairs = data.get("pairs") or ["BTCUSDT"]
    if not isinstance(pairs, list):
        pairs = [pairs]
    sim = manager.create_sim_auto(name, capital, pairs)
    save_persisted()
    return jsonify({"ok": True, "id": sim.id, "name": sim.name})


@app.route("/api/sim/delete", methods=["POST"])
def api_sim_delete():
    data = request.get_json(silent=True) or {}
    result = manager.delete(data.get("id", ""))
    if result.get("ok"):
        save_persisted()
    return jsonify(result), (200 if result.get("ok") else 400)


@app.route("/api/sim/control", methods=["POST"])
def api_sim_control():
    data = request.get_json(silent=True) or {}
    result = manager.control(data.get("id", ""), data.get("action", ""),
                             pair=data.get("pair", ""))
    if result.get("ok"):
        save_persisted()
    return jsonify(result), (200 if result.get("ok") else 400)


@app.route("/api/sim/balance", methods=["POST"])
def api_sim_balance():
    data = request.get_json(silent=True) or {}
    result = manager.set_balance(data.get("id", ""), data.get("capital", 0))
    if result.get("ok"):
        save_persisted()
    return jsonify(result), (200 if result.get("ok") else 400)


@app.route("/api/deep/run", methods=["POST"])
def api_deep_run():
    """System 2 manual: corre el análisis LLM del sim activo y devuelve la
    calificación fresca (recomendación, confianza, razón)."""
    data = request.get_json(silent=True) or {}
    result = manager.deep_run(data.get("pair", ""), data.get("signal", ""))
    return jsonify(result), (200 if result.get("ok") else 400)


@app.route("/api/fleet")
def api_fleet():
    """Vista general de la flota: por sim (resumen + posiciones detalladas +
    analytics + journal) y consolidado (capital/equity/pnl, WR/PF, curva de
    equity sumada por índice alineada por la cola, y cierres recientes)."""
    with manager.cond:
        sims = list(manager.simulators.values())
        sims_out = []
        curves = []
        closes_recent = []
        reasons_agg = {}
        # Consolidado
        tot = {"capital": 0.0, "equity": 0.0, "cash": 0.0, "unrealized": 0.0,
               "realized": 0.0, "pnl": 0.0, "fees": 0.0, "trades": 0, "wins": 0,
               "positions": 0, "running": 0, "gross_win": 0.0, "gross_loss": 0.0}
        for sim in sims:
            s = sim.summary()
            eq, unreal, pos_val = sim.equity()
            an = sim._analytics()
            tiles = an.get("tiles", {})
            js = sim._journal_summary()
            # Posiciones detalladas (mismo cálculo que snapshot_detail)
            positions_out = []
            for pair, pos in sim.positions.items():
                m = sim.markets.get(pair)
                price = float(m.price) if m is not None else float(pos.get("entry") or 0.0)
                is_long = pos.get("side", "LONG") == "LONG"
                qty = float(pos.get("qty") or 0.0)
                margin = float(pos.get("size_usd") or 0.0)
                entry = float(pos.get("entry") or 0.0)
                if is_long:
                    unpnl = qty * price - margin
                else:
                    unpnl = max((entry - price) * qty, -margin)
                positions_out.append({
                    "pair": pair, "side": pos.get("side", "LONG"),
                    "entry": entry, "qty": qty, "size_usd": margin,
                    "sl": pos.get("sl"), "tp": pos.get("tp"),
                    "price": round(price, 6), "pnl": round(unpnl, 4),
                    "notional": pos.get("notional"), "leverage": pos.get("leverage", 1),
                    "tp1_done": bool(pos.get("tp1_done")),
                    "bars_held": int(pos.get("bars_held", 0)),
                    "opened_at": pos.get("opened_at"),
                    "sim_id": sim.id, "sim_name": sim.name,
                })
            # Últimos cierres lógicos (no-parciales) de este sim
            groups = {}
            order = []
            for t in sim.history:
                key = (t["pair"], t.get("opened_at"), t.get("entry_time"))
                g = groups.get(key)
                if g is None:
                    g = {"pair": t["pair"], "side": t.get("side", "LONG"),
                         "entry": t.get("entry"), "opened_at": t.get("opened_at"),
                         "pnl": 0.0, "closed": False, "reason": None, "fees": 0.0}
                    groups[key] = g
                    order.append(key)
                g["pnl"] += float(t.get("pnl") or 0.0)
                g["fees"] += float(t.get("fee") or 0.0)
                if not t.get("partial"):
                    g["closed"] = True
                    g["reason"] = t.get("reason")
            closed = [groups[k] for k in order if groups[k]["closed"]]
            for g in closed[-10:][::-1]:
                by = sim._reason_bucket(g["reason"])
                reasons_agg.setdefault(by, {"reason": by, "n": 0, "pnl": 0.0})
                reasons_agg[by]["n"] += 1
                reasons_agg[by]["pnl"] = round(reasons_agg[by]["pnl"] + g["pnl"], 4)
                closes_recent.append({
                    "sim_id": sim.id, "sim_name": sim.name, "pair": g["pair"],
                    "side": g["side"], "entry": g["entry"],
                    "pnl": round(g["pnl"], 4), "reason": g["reason"],
                    "opened_at": g["opened_at"],
                })
            sims_out.append({
                "id": s["id"], "name": s["name"], "status": s["status"],
                "pairs": s.get("pairs", []), "active_pair": s.get("active_pair"),
                "capital": round(float(sim.capital), 2),
                "cash": s.get("cash"), "equity": s.get("equity"),
                "pnl": s.get("pnl"), "realized": s.get("realized"),
                "unrealized": round(unreal, 2),
                "trades": s.get("trades"), "wins": s.get("wins"),
                "win_rate": s.get("win_rate"),
                "profit_factor": s.get("profit_factor"),
                "positions_open": len(sim.positions),
                "fees_paid": round(float(getattr(sim, "fees_paid", 0.0) or 0.0), 4),
                "daily": round(float(sim.daily), 2),
                "max_daily_loss": sim.params.get("max_daily_loss", 0),
                "analytics_score": an.get("score"), "grade": an.get("grade"),
                "tiles": {k: tiles.get(k) for k in
                          ("profit_factor", "win_rate", "expectancy", "expectancy_pct",
                           "max_dd_pct", "cur_dd_pct", "sharpe", "total_pnl",
                           "best", "worst", "avg_bars_held")},
                "journal": js,
                "positions": positions_out,
            })
            # Suma del consolidado
            tot["capital"] += sim.capital
            tot["equity"] += eq
            tot["cash"] += sim.cash
            tot["unrealized"] += unreal
            tot["realized"] += sim.realized
            tot["pnl"] += sim.realized + unreal
            tot["fees"] += float(getattr(sim, "fees_paid", 0.0) or 0.0)
            tot["trades"] += len(sim.history)
            tot["wins"] += sum(1 for t in sim.history if t.get("pnl", 0) > 0)
            tot["positions"] += len(sim.positions)
            if sim.status == "running":
                tot["running"] += 1
            tot["gross_win"] += sum(g["pnl"] for g in closed if g["pnl"] >= 0)
            tot["gross_loss"] += -sum(g["pnl"] for g in closed if g["pnl"] < 0)
            curves.append(list(sim.equity_curve[-500:]))
        # Curva consolidada: alineada por la cola (mín. len común, tope 500)
        consolidated_curve = []
        if sims and curves and all(len(c) >= 2 for c in curves):
            n = min(len(c) for c in curves)
            for i in range(n):
                ts = curves[-1][n - 1 - i][0]
                val = sum(c[n - 1 - i][1] for c in curves)
                consolidated_curve.append([ts, round(val, 4)])
            consolidated_curve = consolidated_curve[::-1]
        # Consolidado final
        wr = round(tot["wins"] / tot["trades"] * 100.0, 1) if tot["trades"] else 0.0
        pf = None
        if tot["gross_loss"] > 1e-9:
            pf = round(tot["gross_win"] / tot["gross_loss"], 2)
        elif tot["gross_win"] > 1e-9:
            pf = None   # sin pérdidas: infinito
        else:
            pf = 0.0
        peak = tot.get("capital", 0.0)
        max_dd_pct = 0.0
        for _ts, v in consolidated_curve:
            if v > peak:
                peak = v
            if peak > 1e-9:
                ddp = (peak - v) / peak * 100.0
                if ddp > max_dd_pct:
                    max_dd_pct = ddp
        consolidated = {
            "sims": len(sims), "running": tot["running"],
            "capital": round(tot["capital"], 2), "equity": round(tot["equity"], 2),
            "cash": round(tot["cash"], 2),
            "pnl": round(tot["pnl"], 2), "realized": round(tot["realized"], 2),
            "unrealized": round(tot["unrealized"], 2),
            "fees": round(tot["fees"], 4),
            "trades": tot["trades"], "wins": tot["wins"], "win_rate": wr,
            "profit_factor": pf,
            "positions_open": tot["positions"],
            "max_dd_pct": round(max_dd_pct, 2),
        }
    return jsonify({
        "ok": True,
        "server_time": now_iso(),
        "consolidated": consolidated,
        "curve": consolidated_curve[-500:],
        "by_reason": sorted(reasons_agg.values(), key=lambda d: -d["n"]),
        "last_closes": closes_recent[:12],
        "fleet": sims_out,
    })


@app.route("/api/stream")
def api_stream():
    def gen():
        last_version = -1
        first = True
        while True:
            with manager.cond:
                if first:
                    snap = manager._snapshot()
                    first = False
                else:
                    if manager.version == last_version:
                        manager.cond.wait(timeout=5.0)
                    snap = None if manager.version == last_version else manager._snapshot()
                if snap is not None:
                    last_version = snap["version"]
            if snap is None:
                yield ": heartbeat\n\n"
            else:
                yield "data: " + json.dumps(snap) + "\n\n"

    resp = Response(stream_with_context(gen()), mimetype="text/event-stream")
    resp.headers["Cache-Control"] = "no-cache"
    resp.headers["X-Accel-Buffering"] = "no"
    resp.headers["Connection"] = "keep-alive"
    return resp


# ----------------------------------------------------------------------------
# Persistencia del estado (sobrevive recargas del servidor y de la página)
# ----------------------------------------------------------------------------

# Directorio de datos y persistencia (soporte para volumen Railway en /storage/data o DATA_DIR)
DATA_DIR = os.environ.get("DATA_DIR") or os.environ.get("RAILWAY_VOLUME_MOUNT_PATH")
if not DATA_DIR and os.path.isdir("/storage/data"):
    DATA_DIR = "/storage/data"

_base_dir = DATA_DIR if DATA_DIR else os.path.dirname(os.path.abspath(__file__))

PERSIST_FILE = os.environ.get("LAYA_STATE_FILE") or os.path.join(_base_dir, "sim_state.json")
PERSIST_INTERVAL = 10.0

JOURNAL_FILE = os.environ.get("LAYA_JOURNAL_FILE") or os.path.join(_base_dir, "sim_journal.json")
JOURNAL_MAX = 400
DRIFT_EVERY_N = 12

def _ensure_initial_persistence():
    """Si se usa un volumen externo nuevo y los archivos de estado aun no existen,
    copia el estado empaquetado inicial para no arrancar vacio."""
    try:
        app_dir = os.path.dirname(os.path.abspath(__file__))
        if DATA_DIR and os.path.abspath(DATA_DIR) != os.path.abspath(app_dir):
            os.makedirs(DATA_DIR, exist_ok=True)
            if not os.path.exists(PERSIST_FILE):
                src_state = os.path.join(app_dir, "sim_state.json")
                if os.path.exists(src_state):
                    import shutil
                    shutil.copy2(src_state, PERSIST_FILE)
                    log(f"Estado inicial copiado a volumen persistente: {PERSIST_FILE}")
            if not os.path.exists(JOURNAL_FILE):
                src_journal = os.path.join(app_dir, "sim_journal.json")
                if os.path.exists(src_journal):
                    import shutil
                    shutil.copy2(src_journal, JOURNAL_FILE)
                    log(f"Journal inicial copiado a volumen persistente: {JOURNAL_FILE}")
    except Exception as e:
        print("Advertencia al inicializar volumen persistente:", e)

_ensure_initial_persistence()
JOURNAL_MAX = 400          # tope de entradas por simulador
DRIFT_EVERY_N = 12         # trades cerrados entre evaluaciones de drift


def save_journal(mgr=None):
    """Persiste el decision journal de cada simulador (se reescribe el
    archivo con la ventana recortada por sim; json.tmp + rename atomico)."""
    mgr = mgr or globals()["manager"]
    try:
        with mgr.cond:
            data = {sid: st.journal for sid, st in mgr.simulators.items()}
        tmp = JOURNAL_FILE + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False)
        os.replace(tmp, JOURNAL_FILE)
    except Exception as e:
        print("Advertencia: no se pudo persistir el journal:", e)


def load_journal(mgr=None):
    """Restaura el journal por sid si existe (llamar DESPUÉS de
    load_persisted: los sims deben estar ya creados/restaurados)."""
    mgr = mgr or globals()["manager"]
    if not os.path.exists(JOURNAL_FILE):
        return
    try:
        with open(JOURNAL_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        with mgr.cond:
            for sid, st in mgr.simulators.items():
                js = data.get(sid) if isinstance(data.get(sid), list) else []
                kept = []
                for e in js:
                    if not (isinstance(e, dict) and e.get("phase")):
                        continue
                    if e.get("phase") == "executed" and e.get("why") is None:
                        continue   # ejecutadas sin outcome válido: posición fantasma
                    if len(kept) >= JOURNAL_MAX:
                        break
                    kept.append(e)
                st.journal = kept[-JOURNAL_MAX:]
                st._journal_keys = {e.get("key") for e in kept
                                    if e.get("key") and e.get("phase") != "executed"}
        print("Decision journal restaurado: " + JOURNAL_FILE)
    except Exception as e:
        print("Sin decision journal previo (" + str(e)[:70] + ").")


def _relink_journal_refs(st):
    """Tras restaurar posiciones desde el disco, re-vincula journal_ref con
    la entrada 'executed' del journal restaurado (la copia serializada del
    position apunta a otro objeto y los outcomes posteriores no sumarían
    en el registro correcto)."""
    try:
        for pc in (st.positions or {}).values():
            if not isinstance(pc, dict):
                continue
            pc.pop("journal_ref", None)
        if not st.journal:
            return
        jmap = {e.get("key"): e for e in st.journal if e.get("phase") == "executed"}
        for pair, pc in st.positions.items():
            if not isinstance(pc, dict):
                continue
            side = pc.get("side", "LONG")
            bar = pc.get("entry_bar") or pc.get("entry_time")
            if bar is None:
                continue
            e = jmap.get(pair + "|" + str(bar) + "|executed|" + side)
            if e is not None:
                pc["journal_ref"] = e
    except Exception:
        pass


def _clean_candles(candles):
    out = []
    if not isinstance(candles, list):
        return out
    for c in candles:
        if not isinstance(c, dict):
            continue
        try:
            out.append({
                "time": int(c["time"]),
                "open": float(c["open"]),
                "high": float(c["high"]),
                "low": float(c["low"]),
                "close": float(c["close"]),
                "volume": float(c.get("volume") or 0),
            })
        except Exception:
            continue
    return out


def save_persisted(mgr=None):
    mgr = mgr or globals()["manager"]
    try:
        with mgr.cond:
            sims = {}
            for sid, st in mgr.simulators.items():
                sims[sid] = {
                    "name": st.name,
                    "capital": st.capital,
                    "pairs": list(st.pairs),
                    "active_pair": st.active_pair,
                    "status": st.status,
                    "started_at": st.started_at,
                    "cycle": st.cycle,
                    "cash": st.cash,
                    "realized_pnl": st.realized,
                    "daily": st.daily,
                    "fees_paid": float(getattr(st, "fees_paid", 0.0) or 0.0),
                    "positions": copy.deepcopy(st.positions),
                    "history": copy.deepcopy(st.history),
                    "tasks": copy.deepcopy(st.tasks[:60]),
                    "cooldown": copy.deepcopy(st.cooldown),
                    "cooldown_till": copy.deepcopy(getattr(st, "cooldown_till", {})),
                    "equity_curve": (copy.deepcopy(st.equity_curve[-900:])
                                     if isinstance(st.equity_curve, list) else []),
                    "daily_date": st.daily_date,
                    # Guardar el mercado de TODOS los pares del simulador
                    "markets": {p: {"feed": m.feed, "candles": copy.deepcopy(m.candles[-MAX_CANDLES:])}
                                for p, m in st.markets.items()},
                }
            data = {"saved_at": now_iso(), "active_id": mgr.active_id,
                    "data_source": mgr.data_source, "sims": sims}
        tmp = PERSIST_FILE + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False)
        os.replace(tmp, PERSIST_FILE)
        save_journal(mgr)
    except Exception as e:
        print("Advertencia: no se pudo persistir:", e)


def load_persisted(mgr=None):
    mgr = mgr or globals()["manager"]
    if not os.path.exists(PERSIST_FILE):
        log("Sin estado previo en " + PERSIST_FILE + " (primera ejecucion).")
        return
    try:
        with open(PERSIST_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        with mgr.cond:
            sims_data = data.get("sims") if isinstance(data.get("sims"), dict) else {}
            if not sims_data:
                # Compatibilidad con el esquema antiguo (claves sim-N al raiz)
                sims_data = {k: v for k, v in data.items()
                             if k.startswith("sim-") and isinstance(v, dict)}
            for sid, st in list(mgr.simulators.items()):
                saved = sims_data.get(sid)
                if not isinstance(saved, dict):
                    continue
                pairs = [p for p in (saved.get("pairs") or st.pairs) if p in PAIRS] or st.pairs
                st.pairs = pairs
                st.mode = "multi" if len(pairs) > 1 else "single"
                st.name = saved.get("name", st.name)
                try:
                    st.capital = float(saved.get("capital", st.capital))
                except Exception:
                    pass
                status = saved.get("status", "stopped")
                st.status = status if status in ("running", "paused", "stopped") else "stopped"
                st.started_at = saved.get("started_at")
                try:
                    st.cycle = int(saved.get("cycle", st.cycle))
                except Exception:
                    pass
                st.cash = float(saved.get("cash", st.cash))
                st.realized = float(saved.get("realized_pnl", st.realized))
                st.daily = float(saved.get("daily", st.daily))
                try:
                    st.fees_paid = float(saved.get("fees_paid", 0.0) or 0.0)
                except Exception:
                    st.fees_paid = 0.0
                st.positions = saved.get("positions") if isinstance(saved.get("positions"), dict) else {}
                st.history = saved.get("history") if isinstance(saved.get("history"), list) else []
                st.tasks = saved.get("tasks") if isinstance(saved.get("tasks"), list) else []
                ec_saved = saved.get("equity_curve")
                st.equity_curve = []
                if isinstance(ec_saved, list):
                    for rss_pt in ec_saved[-900:]:
                        if (isinstance(rss_pt, (list, tuple)) and len(rss_pt) == 2
                                and isinstance(rss_pt[0], str) and isinstance(rss_pt[1], (int, float))):
                            st.equity_curve.append([rss_pt[0], float(rss_pt[1])])
                dd_saved = saved.get("daily_date")
                st.daily_date = dd_saved if isinstance(dd_saved, str) else None
                cd_saved = saved.get("cooldown") if isinstance(saved.get("cooldown"), dict) else {}
                st.cooldown = {}
                for cp, v in cd_saved.items():
                    try:
                        iv = int(v)
                        if iv > 0 and cp in st.pairs:
                            st.cooldown[cp] = iv
                    except Exception:
                        pass
                ct_saved = saved.get("cooldown_till") if isinstance(saved.get("cooldown_till"), dict) else {}
                st.cooldown_till = {}
                for cp, tv in ct_saved.items():
                    try:
                        st.cooldown_till[cp] = int(tv)
                    except Exception:
                        pass
                # Legacy: cooldown en barras sin 'till' — puente por vela actual
                for cp, iv in list(st.cooldown.items()):
                    if cp not in st.cooldown_till:
                        touch = st.markets.get(cp) if hasattr(st, "markets") else None
                        bar_l = touch.candles[-1]["time"] if touch and touch.candles else int(time.time())
                        st.cooldown_till[cp] = bar_l + iv * st.params.get("tf_seconds", 300)
                # Defensivo: posiciones restauradas con los nuevos campos si faltan
                safe_pos = {}
                for cp, pc in st.positions.items():
                    if not isinstance(pc, dict):
                        continue
                    pc.setdefault("side", "LONG")
                    pc.setdefault("leverage", 1)
                    pc.setdefault("notional", pc.get("size_usd"))
                    pc.setdefault("liq", None)
                    pc.setdefault("tp1_done", False)
                    pc.setdefault("trail", None)
                    pc.setdefault("peak", pc.get("entry"))
                    pc.setdefault("bars_held", 0)
                    if cp in st.pairs:
                        safe_pos[cp] = pc
                st.positions = safe_pos

                active_pair = saved.get("active_pair", saved.get("pair",
                             st.pairs[0] if st.pairs else "BTCUSDT"))
                if active_pair not in PAIRS or active_pair not in st.pairs:
                    active_pair = st.pairs[0] if st.pairs else "BTCUSDT"
                st.active_pair = active_pair

                # Mercados guardados (todos los pares); esquema antiguo: solo el activo
                mk_saved = saved.get("markets") if isinstance(saved.get("markets"), dict) else {}
                single = mk_saved.get(active_pair) or ({"candles": saved.get("candles")}
                                                       if saved.get("candles") else None)
                for p in pairs:
                    m = st.markets.get(p)
                    if m is None:
                        m = Market(p, mgr.params)
                        st.markets[p] = m
                    entry = mk_saved.get(p) if isinstance(mk_saved.get(p), dict) else None
                    candles = _clean_candles((entry or {}).get("candles")
                                             if entry else (single or {}).get("candles"))
                    if candles:
                        m.candles = candles[-MAX_CANDLES:]
                        m.price = m.candles[-1]["close"]
                        m.recompute()
                        m.evaluate()
                    # Si no hay velas guardadas, el Market ya viene seeded de su constructor
                    feed_saved = (entry or {}).get("feed")
                    # compatibilidad: el antiguo feed "testnet" era el spot actual
                    if feed_saved in ("spot", "futures", "testnet") and candles:
                        m.feed = "spot" if feed_saved == "testnet" else feed_saved
                        log("  " + sid + "/" + p + ": feed " + m.feed + " restaurado ("
                            + str(len(m.candles)) + " velas reales)")
                    else:
                        m.feed = "synthetic"
                        m.retimestamp()   # solo en mercados simulados: re-anclar a la hora actual


            # Seguridad: soltar posiciones de pares que ya no existen
                for p in list(st.positions.keys()):
                    if p not in st.pairs:
                        st.positions.pop(p, None)
                _relink_journal_refs(st)

                if st.status == "running":
                    st.add_task("system", "Estado restaurado del disco: la simulacion continua activa.")
                elif st.status == "paused":
                    st.add_task("system", "Estado restaurado del disco (pausado). Pulsa Activar para continuar.")

            # Crear simuladores nuevos guardados que faltan en memoria
            for sid, saved in sims_data.items():
                if sid not in mgr.simulators:
                    if isinstance(saved, dict):
                        name = saved.get("name", sid)
                        try:
                            capital = float(saved.get("capital", 20))
                        except Exception:
                            capital = 20.0
                        pairs_raw = saved.get("pairs", ["BTCUSDT"])
                        pairs_list = pairs_raw if isinstance(pairs_raw, list) else [pairs_raw]
                        pairs = [p for p in pairs_list if p in PAIRS] or ["BTCUSDT"]
                        new_sim = mgr.create_sim(sid, name, capital, pairs)
                        new_sim.context = mgr.context
                        new_sim.status = saved.get("status", "stopped") if saved.get("status", "stopped") in ("running", "paused", "stopped") else "stopped"
                        new_sim.cash = float(saved.get("cash", capital))
                        new_sim.capital = capital
                        new_sim.realized = float(saved.get("realized_pnl", 0))
                        new_sim.daily = float(saved.get("daily", 0))
                        new_sim.started_at = saved.get("started_at")
                        try:
                            new_sim.cycle = int(saved.get("cycle", 0))
                        except Exception:
                            pass
                        new_sim.history = saved.get("history", []) if isinstance(saved.get("history"), list) else []
                        new_sim.positions = saved.get("positions") if isinstance(saved.get("positions"), dict) else {}
                        _relink_journal_refs(new_sim)
                        new_sim.tasks = saved.get("tasks", [])[:60] if isinstance(saved.get("tasks"), list) else []
                        new_sim.cooldown = saved.get("cooldown", {}) if isinstance(saved.get("cooldown"), dict) else {}
                        # Mercados mínimos
                        for p in pairs:
                            m = new_sim.markets.get(p)
                            if m is None:
                                m = Market(p, mgr.params)
                                new_sim.markets[p] = m
                        # Restaurar feed
                        feed_saved = saved.get("feed") or saved.get("markets", {}).get(pairs[0] or "BTCUSDT", {}).get("feed")
                        if feed_saved in ("spot", "futures", "testnet"):
                            kind = "spot" if feed_saved == "testnet" else feed_saved
                            for p in pairs:
                                m = new_sim.markets.get(p)
                                if m:
                                    m.feed = kind
                        log("  " + sid + ": creado y restaurado (capital $" + f"{capital:.2f}" + ", " + str(len(pairs)) + " pares).")

            active = data.get("active_id")
            if active in mgr.simulators:
                mgr.active_id = active
            elif mgr.simulators:
                mgr.active_id = next(iter(mgr.simulators))
            ds = data.get("data_source")
            if ds == "testnet":        # esquema antiguo
                ds = "spot"
            if ds in ("synthetic", "spot", "futures"):
                mgr.data_source = ds
            mgr.params["futures_enabled"] = (mgr.data_source == "futures")
            mgr.version += 1
            mgr.cond.notify_all()
        log("Estado restaurado desde " + PERSIST_FILE
            + " (guardado en " + str(data.get("saved_at")) + ").")
        for sid, st in mgr.simulators.items():
            log("  " + sid + ": " + st.name + " -> " + st.status
                + " (ciclo " + str(st.cycle) + ", " + str(len(st.positions)) + " posiciones)")
    except Exception as e:
        print("Advertencia: no se pudo restaurar el estado:", e)


def _persist_loop():
    while not globals()["manager"].shutdown:
        time.sleep(PERSIST_INTERVAL)
        save_persisted()


def log(msg):
    ts = now_iso()
    print(f"[{ts}] {msg}")

if __name__ == "__main__":
    manager.start_loop()
    load_persisted(manager)
    load_journal(manager)
    # Con origen testnet restaurado del disco, re-sembrar TODOS los mercados
    # con velas reales AHORA (antes los markets restaban synthetic y cada sim
    # derivaba su propia serie: precios divergentes entre sims del mismo par).
    if manager.data_source != "synthetic":
        try:
            notes = manager._apply_feed_to_all()
            for n in notes:
                log("Feed al arrancar: " + n)
        except Exception as e:
            log("No se pudo aplicar el feed real al arrancar: " + str(e))
    threading.Thread(target=_persist_loop, daemon=True).start()
    manager.start_testnet_poller()
    # Capa de contexto fundamental (F&G, noticias, macro, MTF): daemon propio,
    # solo en el servidor real (importar server.py no lanza ningun hilo).
    manager.start_context_layer()

    # Guardar el estado al salir (Ctrl+C / cierre normal) para que una
    # simulacion iniciada siga iniciada tras reiniciar el servidor.
    def _save_on_exit():
        try:
            save_persisted(manager)
            log("Estado guardado al salir en " + PERSIST_FILE)
        except Exception:
            pass
    atexit.register(_save_on_exit)

    print("=" * 66)
    print(" LAYA — Agente de trading con IA (System 1) · modo paper")
    print(f" Consola: http://127.0.0.1:{PORT}")
    print(" Agentes iniciales: 8 simuladores con $200 cada uno (BTC/ETH optimizado).")
    print(" Para salir: Ctrl+C")
    print(" Estado persistente en: " + PERSIST_FILE)
    print(" (si un agente estaba activo, al reiniciar el servidor sigue activo)")
    print("=" * 66)
    try:
        HOST = os.environ.get("HOST", "0.0.0.0")
        app.run(host=HOST, port=PORT, threaded=True, debug=False, use_reloader=False)
    except KeyboardInterrupt:
        _save_on_exit()
        log("Servidor detenido.")

