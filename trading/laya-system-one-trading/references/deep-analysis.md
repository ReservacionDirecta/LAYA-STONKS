Deep Analysis (System 2) feeding Laya System 1

When user asks for LLM feeding trading decisions:

Prompt structure (JSON-only output, no extra text):
- Input: pair price, EMA fast/slow, RSI, volume_ratio, candle_direction, capital, feed (synthetic/spot/futures), context (fng, global, news, mtf).
- Technical sources: EMA crossover, RSI (zones >70/<30), volume ratio (>1.2x mean), candle body ratio, ATR, feed type.
- Fundamental sources: Fear & Greed Index (alternative.me), CoinGecko /global (mcap, change_24h, btc_dom, eth_dom), RSS titles (CoinDesk + Cointelegraph) lexicon score, MTF 1h trend + RSI per pair.
- Output keys: macro_tendencia, confianza (0-1), niveles_clave, sentimiento_mercado, recomendacion_laya (LONG/SHORT/WAIT/ABSTAIN), razon_corta (max 30 words, include technical + fundamental + context), filtro_extra ({exigir_volumen_alto, evitar_sobrecompra, esperar_confirmacion_mtf}), ajuste_sl_tp ({sl_factor, tp_factor}).

Integration rules:
- `enabled`: false by default. Must be set true in config.json with valid `api_key_env`.
- `use_for_filter`: if Deep recommends ABSTAIN/WAIT and Laya wants entry, cancel entry (`filter_applied: true`). If F&G >= 80 or <= 10 (extreme) and MTF does not confirm signal direction with `ctx_filter: true`, cancel entry.
- `use_for_sl_tp_adjust`: only when explicitly enabled. Adjust factors applied to SL/TP for reporting; never rely solely on LLM for risk adjustments without user confirmation.
- If API call fails or key missing: fall back to System 1. Report blocker honestly. Never invent LLM results.

Exact fee table for $20 budget (always state):
- Spot standard 0.1%: $0.02 per side, $0.04 cycle, ~500 trades on $20.
- Spot BNB 0.075%: $0.015/$0.03, ~666 trades.
- Futures Taker 0.05%: $0.01/$0.02, ~1000 trades.
- Futures Maker 0.02%: $0.004/$0.008, ~2500 trades.
- Spot min order: ~$5-$10 (forces 50-100% capital allocation at $20).
- Funding fee: charged every 8h on futures positions.
- High leverage on futures can liquidate $20 in seconds; avoid with low experience.

Budget reminder: with $20, fees (~0.2% round-trip spot standard) consume quickly. Deep analysis does not eliminate risk; it adds a filter layer. Prioritize protection. Recommend $50-$100 minimum.
