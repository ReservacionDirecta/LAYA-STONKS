#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Deep Analysis (System 2) — LLM para análisis profundo que alimenta Laya System 1.

Integración con NVIDIA API (stepfun-ai/step-5-preview) para análisis técnico/fundamental.

Fuentes en tiempo real integradas:
- Técnico: EMA fast/slow, RSI, volumen relativo, cuerpo de vela (body ratio),
  ATR, tendencia de velas, feed (simulado / spot testnet / futuros testnet), MTF (1h).
- Fundamental: Fear & Greed (alternative.me), macro global CoinGecko (mcap, btc_dom),
  sentimiento de titulares RSS (CoinDesk + Cointelegraph), sesgo multi-temporal (MTF).

Uso: el usuario (o server.py) construye price_info con datos técnicos + contexto;
este motor arma un prompt estructurado JSON y aplica un filtro sobre Laya System 1.
"""
import os
import json
import requests
import time
import sys
from datetime import datetime, timezone
from typing import Optional, Dict, Any


class DeepAnalysisEngine:
    """Llama a un LLM (NVIDIA API) para análisis profundo con fuentes técnicas y fundamentales."""

    def __init__(self, cfg):
        self.cfg = cfg.get("deep_analysis", {})
        self.enabled = self.cfg.get("enabled", False)
        if not self.enabled:
            return

        # Configuración NVIDIA API
        self.provider = self.cfg.get("provider", "nvidia")
        self.model = self.cfg.get("model", "stepfun-ai/step-5-preview")
        self.api_key_env = self.cfg.get("api_key_env", "NVIDIA_API_KEY")
        self.endpoint = self.cfg.get("endpoint", "https://integrate.api.nvidia.com/v1/chat/completions")
        self.temperature = float(self.cfg.get("temperature", 0.2))
        self.timeout = int(self.cfg.get("timeout_seconds", 20))
        self.max_retries = int(self.cfg.get("max_retries", 2))
        self.retry_delay = float(self.cfg.get("retry_delay_seconds", 2.0))
        self.use_filter = self.cfg.get("use_for_filter", True)
        self.use_adjust = self.cfg.get("use_for_sl_tp_adjust", False)

        # 1) Leer de config.json explícita si existe, 2) os.environ, 3) Registro de Windows (User Environment)
        self.api_key = self.cfg.get("api_key", "").strip() or os.environ.get(self.api_key_env, "").strip()
        if not self.api_key and os.name == 'nt':
            try:
                import winreg
                with winreg.OpenKey(winreg.HKEY_CURRENT_USER, r"Environment") as key:
                    val, _ = winreg.QueryValueEx(key, self.api_key_env)
                    if val:
                        self.api_key = str(val).strip()
            except Exception:
                pass
        self.auth_failed = False
        if not self.api_key:
            print(f"[DEEP] ADVERTENCIA: {self.api_key_env} no está configurada. "
                  "Análisis profundo desactivado. El sistema usará solo System 1.")
            self.enabled = False
            return

        print(f"[DEEP] Inicializado: provider={self.provider}, model={self.model}, endpoint={self.endpoint}")

    def build_prompt(self, price_info: Dict[str, Any]) -> str:
        pair = price_info.get("pair", "BTCUSDT")
        price = price_info.get("price", "N/A")
        ema_f = price_info.get("ema_fast", "N/A")
        ema_s = price_info.get("ema_slow", "N/A")
        rsi = price_info.get("rsi", "N/A")
        vol_ratio = price_info.get("volume_ratio", "N/A")
        candle_dir = price_info.get("candle_direction", "N/A")
        tf = price_info.get("timeframe", "5m")
        capital = price_info.get("capital", 20)
        feed = price_info.get("feed", "synthetic")

        atr = price_info.get("atr", "N/A")
        body_ratio = price_info.get("body_ratio", "N/A")
        max_ext_atr = price_info.get("max_ext_atr", 0)
        ext_ratio = price_info.get("ext_ratio", "N/A")
        min_body = price_info.get("min_body", 0)
        signal_str = price_info.get("signal", "NONE")
        strength_l = price_info.get("strength_long", 0)
        strength_s = price_info.get("strength_short", 0)

        ctx = price_info.get("context") or {}
        fng = ctx.get("fng", {})
        if isinstance(fng, dict):
            fng_val = fng.get("value", "N/A")
            fng_label = fng.get("label", "N/A")
        elif isinstance(fng, (int, float)):
            fng_val = fng
            fng_label = "N/A"
        else:
            fng_val = "N/A"
            fng_label = "N/A"

        global_ctx = ctx.get("global", {})
        mcap = global_ctx.get("mcap", "N/A")
        mcap_chg24 = global_ctx.get("change_24h", "N/A")
        btc_dom = global_ctx.get("btc_dom", "N/A")
        eth_dom = global_ctx.get("eth_dom", "N/A")

        news_ctx = ctx.get("news", {})
        news_sentiment = news_ctx.get("sentiment", 0)
        news_label = news_ctx.get("label", "N/A")
        news_samples = news_ctx.get("samples", [])
        samples_str = "; ".join(str(s) for s in news_samples[:3])

        mtf = {}
        mtf_raw = ctx.get("mtf")
        if isinstance(mtf_raw, dict) and pair in mtf_raw:
            mtf = mtf_raw[pair]
        mtf_trend = mtf.get("trend", "N/A") if isinstance(mtf, dict) else "N/A"
        mtf_rsi = mtf.get("rsi", "N/A") if isinstance(mtf, dict) else "N/A"
        mtf_timeframe = mtf.get("timeframe", "1h") if isinstance(mtf, dict) else "1h"

        strategy = price_info.get("strategy", {})
        sl_pct = strategy.get("sl_pct", 0.025)
        tp_pct = strategy.get("tp_pct", 0.05)
        max_pos = price_info.get("max_position_usd", 5)
        max_daily_loss = price_info.get("max_daily_loss_usd", 2)

        feed_note = (f"Feed REAL ({feed}) — velas del mercado en vivo (Binance Testnet). "
                     f"El análisis técnico refleja precios reales, no sintéticos.") \
            if feed in ("spot", "futures") else \
            "Feed sintético (simulado con volatilidad basada en el par base)."

        ctx_filter_on = strategy.get("ctx_filter", False)
        ctx_filter_note = ""
        if ctx_filter_on:
            if isinstance(fng_val, (int, float)) and (fng_val >= 80 or fng_val <= 10):
                ctx_filter_note = (f"F&G EXTREMO ({fng_val} — {fng_label}): "
                                  f"el filtro de contexto está ACTIVADO. Solo entra si MTF acompaña ({mtf_trend}).")
            else:
                ctx_filter_note = f"F&G {fng_val} ({fng_label}) — no extremo; filtro neutro."
        else:
            ctx_filter_note = "Filtro de contexto desactivado (ctx_filter=false)."

        crossover = "NEUTRAL"
        try:
            ef = float(ema_f) if ema_f not in ("N/A", None) else None
            es = float(ema_s) if ema_s not in ("N/A", None) else None
            if ef is not None and es is not None:
                crossover = "LONG" if ef > es else "SHORT" if ef < es else "NEUTRAL"
        except Exception:
            pass

        # Incluir historial de rendimiento reciente si está disponible
        sim_performance = price_info.get("sim_performance", "")

        preset = str(price_info.get("preset", "standard")).lower()
        preset_name = str(price_info.get("preset_name", "Estándar"))

        if preset == "conservative":
            profile_guide = """- PERFIL OPERATIVO ACTIVO: CONSERVADOR (Swing / Alta precisión).
  * Exige máxima confluencia técnica (EMA clara, RSI centrado 45-65 sin sobrecompra, cuerpo de vela sólido).
  * Ante la menor duda, contradicción MTF o volumen anémico, recomienda WAIT (confianza requerida >= 65%).
  * En ajuste_sl_tp, sugiere SL holgado (sl_factor 1.0-1.15) para evitar barridos y TP amplio (tp_factor 1.0-1.25) solo en setups A+."""
        elif preset == "aggressive":
            profile_guide = """- PERFIL OPERATIVO ACTIVO: AGRESIVO (Scalp rápido / Alta frecuencia).
  * Prioriza impulsos veloces, roturas de volatilidad y momentum inmediato.
  * Si la dirección técnica acompaña la señal de System 1, aprueba con rapidez (confianza requerida >= 40%).
  * En ajuste_sl_tp, sugiere SL ceñido (sl_factor 0.85-0.95) para recortar pérdidas rápido y TP ágil (tp_factor 0.85-1.0)."""
        else:
            profile_guide = """- PERFIL OPERATIVO ACTIVO: ESTÁNDAR (Equilibrado / Laya 1x).
  * Balance óptimo entre frecuencia y protección con R:R ~2.2:1 (confianza requerida >= 50%).
  * Aprueba setups consistentes sin contradicciones mayores en el contexto."""

        prompt = f"""Eres un analista técnico y fundamental de cripto con enfoque en gestión de riesgo. Analiza los datos EN TIEMPO REAL del par {pair} para un agente Laya (System 1 + System 2) con presupuesto limitado ($20-$50) bajo el perfil {preset_name.upper()}. Responde SOLO con un JSON válido, sin texto fuera del JSON.

--- PERFIL AGÉNTICO ACTIVO ({preset_name.upper()}) ---
{profile_guide}

--- ANÁLISIS TÉCNICO EN TIEMPO REAL ---
- Par / feed: {pair} | feed: {feed} | {feed_note}
- Precio actual: ${price}
- EMA 9 / EMA 21: {ema_f} / {ema_s} (crossover: {crossover})
- RSI (14): {rsi} {'(zonas extremas >70 / <30: evitar sin divergencia)' if (isinstance(rsi, (int,float)) and (rsi > 70 or rsi < 30)) else ''}
- Tendencia de velas: {candle_dir}
- Volumen relativo (20-velas): {vol_ratio} (umbral 1.2x)
- ATR(14): {atr} | Ratio cuerpo de vela (min {min_body}): {body_ratio} | Extensión ATR (max {max_ext_atr}): {ext_ratio}
- Señal Laya System 1: {signal_str} | Fuerza LONG: {int(strength_l*100)}% | Fuerza SHORT: {int(strength_s*100)}%
- Intervalo: {tf} | Posición máx: ${max_pos} (capital ${capital}) | SL: {sl_pct*100:.1f}% | TP: {tp_pct*100:.1f}%

--- ANÁLISIS FUNDAMENTAL EN TIEMPO REAL (fuentes públicas, sin claves) ---
- Fear & Greed Index: {fng_val} ({fng_label})
- Macro global (CoinGecko): MCap {mcap}, cambio 24h {mcap_chg24}%, BTC dom {btc_dom}%, ETH dom {eth_dom}%
- Sentimiento titulares RSS (CoinDesk + Cointelegraph): {news_label} (score {news_sentiment}, {len(news_samples) if isinstance(news_samples,list) else 0} muestras). Muestras: {samples_str}
- Sesgo multi-temporal (MTF 1h en {pair}): tendencia {mtf_trend}, RSI {mtf_rsi}
- Nota de contexto: {ctx_filter_note}
{sim_performance}

--- REGLAS DE DECISIÓN (prioriza protección con $20-$50) ---
- Si RSI > 70 o < 30 SIN divergencia -> ABSTAIN o WAIT.
- Si volumen < 1.2x media y tendencia neutral -> ABSTAIN.
- Si F&G >= 80 o <= 10 (extremo) y MTF NO acompaña la dirección de la señal -> ABSTAIN (filtro de contexto). Si MTF acompaña, puede entrar con cautela.
- Si cuerpo de vela < {min_body} (doji / mecha) -> evitar entrada (calidad baja).
- Si el precio está > {max_ext_atr}x ATR lejos de EMA21 en la dirección de la señal -> evitar perseguir (agotamiento).
- Prioriza SL/TP predefinidos; no prometas ganancia máxima, busca consistencia.
- El JSON debe ser válido. Sin comentarios ni texto fuera del JSON.

Responde con EXACTAMENTE estas claves:
{{
  "macro_tendencia": "alcista | bajista | neutral",
  "confianza": 0.0 a 1.0,
  "niveles_clave": "descripción breve de soporte/resistencia",
  "sentimiento_mercado": "positivo | negativo | neutral",
  "recomendacion_laya": "LONG | SHORT | WAIT | ABSTAIN",
  "razon_corta": "máximo 30 palabras explicando la razón principal (técnica + fundamental + contexto)",
  "filtro_extra": {{ "exigir_volumen_alto": true/false, "evitar_sobrecompra": true/false, "esperar_confirmacion_mtf": true/false }},
  "ajuste_sl_tp": {{ "sl_factor": 1.0, "tp_factor": 1.0, "nota": "" }}
}}

Datos del contexto de riesgo: capital disponible ${capital}, max posición ${max_pos}, max pérdida diaria ${max_daily_loss}, feed {feed}, MTF {mtf_timeframe}.

Nota de comisiones (Binance real): Spot estándar 0.1% ($0.02 por lado, $0.04 ciclo completo, ~500 trades con $20); Spot BNB 0.075% ($0.015/$0.03, ~666); Futuros Taker 0.05% ($0.01/$0.02, ~1000); Futuros Maker 0.02% ($0.004/$0.008, ~2500). Orden mínima spot ~$5-$10 (obliga 50-100% capital en $20). Apalancamiento alto en futuros puede liquidar $20 en segundos. Funding fee: cada 8h.
"""
        return prompt

    def _make_request(self, payload: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Realiza la petición HTTP con reintentos. Soporta OpenAI-compatible y Google Gemini."""
        is_gemini = "generativelanguage.googleapis.com" in self.endpoint

        if is_gemini:
            # Gemini API usa API key como query parameter
            url = f"{self.endpoint}?key={self.api_key}"
            headers = {"Content-Type": "application/json"}
        else:
            # OpenAI-compatible (OpenRouter, etc.)
            url = self.endpoint
            headers = {
                "Content-Type": "application/json",
                "Authorization": f"Bearer {self.api_key}",
            }

        self.last_error = ""
        for attempt in range(self.max_retries + 1):
            try:
                resp = requests.post(url, headers=headers, json=payload, timeout=self.timeout)
                resp.raise_for_status()
                self.last_error = ""
                return resp.json()
            except requests.exceptions.Timeout:
                err_msg = f"Timeout ({self.timeout}s) en intento {attempt + 1}/{self.max_retries + 1}"
            except requests.exceptions.HTTPError as e:
                status = e.response.status_code
                # 400/401/403 = credencial/petición inválida: reintentar no ayudará
                err_msg = f"HTTP {status}: {e.response.text[:200]}"
                if status in (400, 401, 403):
                    self.last_error = err_msg
                    if not self.auth_failed:
                        self.auth_failed = True
                        print(f"[DEEP] ERROR ({status}): {err_msg}")
                        print("[DEEP] Credencial no válida. System 2 pausado; el bot continuará operando con System 1.")
                    return None
            except Exception as e:
                err_msg = f"Error de conexión: {e}"

            if attempt < self.max_retries:
                print(f"[DEEP] {err_msg} — reintentando en {self.retry_delay}s...")
                time.sleep(self.retry_delay)
            else:
                print(f"[DEEP] ERROR tras {self.max_retries + 1} intentos: {err_msg}")
                return None

        return None

    def call_llm(self, prompt: str) -> Dict[str, Any]:
        is_gemini = "generativelanguage.googleapis.com" in self.endpoint

        if is_gemini:
            # Formato Google Gemini API
            payload = {
                "contents": [{"parts": [{"text": prompt}]}],
                "generationConfig": {
                    "temperature": self.temperature,
                    "maxOutputTokens": 450,
                    "topP": 1,
                    "topK": 40,
                },
            }
        else:
            # Formato OpenAI-compatible (OpenRouter, etc.)
            payload = {
                "model": self.model,
                "messages": [{"role": "user", "content": prompt}],
                "temperature": self.temperature,
                "max_tokens": 450,
                "top_p": 1,
                "frequency_penalty": 0,
                "presence_penalty": 0,
                "stream": False,
            }

        data = self._make_request(payload)
        if not data:
            return self._fallback_response(self.last_error or "Error de conexión o timeout tras reintentos")

        try:
            if is_gemini:
                # Respuesta Gemini: candidates[0].content.parts[0].text
                candidates = data.get("candidates", [])
                if not candidates:
                    return self._fallback_response("Respuesta Gemini sin candidates")
                content = candidates[0].get("content", {}).get("parts", [{}])[0].get("text", "")
            else:
                # Respuesta OpenAI: choices[0].message.content
                content = data.get("choices", [{}])[0].get("message", {}).get("content", "")

            if not content:
                return self._fallback_response("Respuesta vacía del LLM")

            # Extraer JSON de la respuesta (puede venir con markdown u otro texto)
            start = content.find("{")
            end = content.rfind("}") + 1
            if start >= 0 and end > start:
                parsed = json.loads(content[start:end])
            else:
                return self._fallback_response("Respuesta LLM malformada: no contiene JSON válido")

            # Validar claves requeridas
            required_keys = ("recomendacion_laya", "confianza", "razon_corta",
                           "macro_tendencia", "sentimiento_mercado", "niveles_clave",
                           "filtro_extra", "ajuste_sl_tp")
            for k in required_keys:
                if k not in parsed:
                    parsed[k] = self._default_value(k)

            # Validar valores de recomendación
            valid_recs = ("LONG", "SHORT", "WAIT", "ABSTAIN")
            if parsed["recomendacion_laya"] not in valid_recs:
                parsed["recomendacion_laya"] = "WAIT"

            # Validar confianza
            try:
                parsed["confianza"] = max(0.0, min(1.0, float(parsed["confianza"])))
            except Exception:
                parsed["confianza"] = 0.0

            # Validar estructura de filtros
            if not isinstance(parsed.get("filtro_extra"), dict):
                parsed["filtro_extra"] = {"exigir_volumen_alto": False, "evitar_sobrecompra": False, "esperar_confirmacion_mtf": False}
            if not isinstance(parsed.get("ajuste_sl_tp"), dict):
                parsed["ajuste_sl_tp"] = {"sl_factor": 1.0, "tp_factor": 1.0, "nota": ""}

            # Validar factores SL/TP
            for factor_key in ("sl_factor", "tp_factor"):
                try:
                    val = float(parsed["ajuste_sl_tp"].get(factor_key, 1.0))
                    parsed["ajuste_sl_tp"][factor_key] = max(0.5, min(2.0, val))
                except Exception:
                    parsed["ajuste_sl_tp"][factor_key] = 1.0

            return parsed

        except json.JSONDecodeError as e:
            return self._fallback_response(f"Error parseando JSON: {e}")
        except Exception as e:
            return self._fallback_response(f"Error procesando respuesta: {e}")

    def _default_value(self, key: str) -> Any:
        defaults = {
            "recomendacion_laya": "WAIT",
            "confianza": 0.0,
            "razon_corta": "Falta clave en respuesta",
            "macro_tendencia": "neutral",
            "sentimiento_mercado": "neutral",
            "niveles_clave": "N/A",
            "filtro_extra": {"exigir_volumen_alto": False, "evitar_sobrecompra": False, "esperar_confirmacion_mtf": False},
            "ajuste_sl_tp": {"sl_factor": 1.0, "tp_factor": 1.0, "nota": "Fallback por clave faltante"},
        }
        return defaults.get(key, None)

    def _fallback_response(self, reason: str) -> Dict[str, Any]:
        return {
            "recomendacion_laya": "WAIT",
            "razon_corta": reason,
            "confianza": 0.0,
            "filtro_extra": {"exigir_volumen_alto": False, "evitar_sobrecompra": False, "esperar_confirmacion_mtf": False},
            "ajuste_sl_tp": {"sl_factor": 1.0, "tp_factor": 1.0, "nota": f"Fallback: {reason}"},
            "macro_tendencia": "neutral",
            "sentimiento_mercado": "neutral",
            "niveles_clave": "N/A",
        }

    def analyze(self, price_info: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        if not self.enabled:
            return None
        if getattr(self, "auth_failed", False):
            return self._fallback_response(getattr(self, "last_error", "") or "API Key no válida o expirada")

        prompt = self.build_prompt(price_info)
        result = self.call_llm(prompt)

        # Enriquecer resultado con metadatos
        result["_timestamp"] = datetime.now(timezone.utc).isoformat()
        result["_provider"] = self.provider
        result["_model"] = self.model
        result["_pair"] = price_info.get("pair", "BTCUSDT")
        result["_feed"] = price_info.get("feed", "synthetic")
        ctx = price_info.get("context") or {}
        result["_context_sources"] = {
            "fng": bool(ctx.get("fng")),
            "global": bool(ctx.get("global")),
            "news": bool(ctx.get("news")),
            "mtf": bool(ctx.get("mtf")),
        }
        result["_latency_ms"] = price_info.get("_deep_latency_ms", 0)

        return result


# Función de conveniencia para testing rápido
def test_deep_analysis():
    """Test rápido del motor de análisis profundo (usa la config real de config.json)."""
    import os
    here = os.path.dirname(os.path.realpath(__file__))
    cfg_path = os.path.join(here, "config.json")
    cfg = {}
    if os.path.exists(cfg_path):
        try:
            with open(cfg_path, "r", encoding="utf-8") as f:
                cfg = json.load(f)
        except Exception as e:
            print(f"[DEEP] ERROR leyendo config.json: {e} — se usará config por defecto.")
    else:
        print("[DEEP] config.json no encontrado; se usará config por defecto.")

    engine = DeepAnalysisEngine(cfg)
    if not engine.enabled:
        print("Test omitido: análisis desactivado o API key no configurada")
        return

    price_info = {
        "pair": "BTCUSDT",
        "price": 67500.0,
        "ema_fast": 67200.0,
        "ema_slow": 66800.0,
        "rsi": 58.5,
        "volume_ratio": 1.35,
        "candle_direction": "alcista",
        "timeframe": "5m",
        "capital": 20,
        "feed": "spot",
        "atr": 1200.0,
        "body_ratio": 0.65,
        "max_ext_atr": 1.8,
        "ext_ratio": 0.58,
        "min_body": 0.35,
        "signal": "LONG",
        "strength_long": 0.75,
        "strength_short": 0.25,
        "context": {
            "fng": {"value": 45, "label": "Neutral"},
            "global": {"mcap": "2.4T", "change_24h": 1.2, "btc_dom": 54.2, "eth_dom": 16.8},
            "news": {"sentiment": 15, "label": "Optimista", "samples": ["Bitcoin ETF inflows surge", "Fed rate cut hopes boost crypto"]},
            "mtf": {"BTCUSDT": {"trend": "ALCISTA", "rsi": 62, "timeframe": "1h"}},
        },
        "strategy": {"sl_pct": 0.025, "tp_pct": 0.05, "ctx_filter": True},
        "max_position_usd": 5,
        "max_daily_loss_usd": 2,
    }

    print(f"Enviando request a {engine.provider} / {engine.model}...")
    start = time.time()
    result = engine.analyze(price_info)
    elapsed = (time.time() - start) * 1000

    if result:
        print(f"\n[OK] Analisis completado en {elapsed:.0f}ms")
        print(json.dumps(result, indent=2, ensure_ascii=False))
    else:
        print("[FAIL] Analisis fallo")


if __name__ == "__main__":
    test_deep_analysis()