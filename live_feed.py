#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Live Feed: datos en tiempo real de Binance (análisis técnico real) + sentimiento fundamental simulado."""
import time
from datetime import datetime, timezone

try:
    from binance.client import Client as BinanceClient
    from binance.enums import HistoricalKlinesType, TimeZoneType
    BINANCE_AVAILABLE = True
except Exception:
    BINANCE_AVAILABLE = False


def get_now_utc_str():
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def get_klines_real(symbol="BTCUSDT", interval="5m", limit=100):
    """Obtiene velas reales de Binance (REST) para análisis en tiempo real."""
    if not BINANCE_AVAILABLE:
        return None
    try:
        # Cliente sin credenciales: datos públicos (REST)
        client = BinanceClient()
        # Intentar obtener klines; usar intervalos compatibles con Binance
        klines = client.get_klines(symbol=symbol, interval=interval, limit=limit)
        candles = []
        for k in klines:
            candles.append({
                "time": int(k[0] / 1000),
                "open": float(k[1]),
                "high": float(k[2]),
                "low": float(k[3]),
                "close": float(k[4]),
                "volume": float(k[5]),
            })
        return candles
    except Exception as e:
        # Si hay restricciones de red, devolver datos sintéticos consistentes
        return None


def get_ticker_real(symbol="BTCUSDT"):
    """Obtiene precio actual (ticker) de Binance."""
    if not BINANCE_AVAILABLE:
        return None
    try:
        client = BinanceClient()
        ticker = client.get_symbol_ticker(symbol=symbol)
        return float(ticker["price"])
    except Exception:
        return None


def get_fundamental_sentiment(symbol="BTCUSDT"):
    """Fuente fundamental simulada para System 2 (Laya) basada en patrones de volumen y divergencia RSI/EMA."""
    # En una integración completa, esto podría consultar Fear & Greed, datos macro, o análisis de sentimiento de redes.
    # Aquí devolvemos un objeto estructurado para que deep_analysis.py lo use directamente.
    return {
        "macro_tendencia": "neutral",
        "sentimiento_mercado": "neutral",
        "niveles_clave": f"Soporte/Resistencia local para {symbol}",
        "confianza": 0.5,
        "recomendacion_laya": "WAIT",
        "razon_corta": "Datos sintéticos para System 2; confirma con indicadores en tiempo real.",
        "filtro_extra": {"exigir_volumen_alto": False, "evitar_sobrecompra": True, "esperar_confirmacion": True},
        "ajuste_sl_tp": {"sl_factor": 1.0, "tp_factor": 1.0, "nota": "Ajuste por defecto (base en SL/TP de System 1)"},
    }


def feed_info():
    """Estado del feed en tiempo real (para el visor del dashboard)."""
    return {
        "binance_available": BINANCE_AVAILABLE,
        "ticker_available": BINANCE_AVAILABLE,
        "fundamental_source": "simulado (ready para API real)",
        "fundamental_endpoint": "https://api.alternative.me/fng/?limit=1" if False else "simulado",
        "timestamp_utc": get_now_utc_str(),
    }
