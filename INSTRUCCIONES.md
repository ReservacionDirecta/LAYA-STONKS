Cómo implementar y usar Laya STONKS (pasos reales)

1. NO CAMBIES A LIVE sin leer esto primero:
   - $20 es muy poco. Binance cobra: Spot estándar 0.1% → $0.02 por lado, ciclo completo $0.04 (~500 trades con $20). BNB 0.075% → $0.015/$0.03 (~666). Futuros Taker 0.05% → $0.01/$0.02 (~1000). Futuros Maker 0.02% → $0.004/$0.008 (~2500).
   - Orden mínima spot ~$5-$10 según par (obliga 50-100% del capital con $20; sin diversificación ni promedio).
   - Futuros: apalancamiento alto (10x-20x) puede liquidar $20 en segundos. Funding fee: cada 8h. NO recomendado con poco conocimiento.
   - En 10 trades: ~$0.4 en comisiones spot. Si pierdes 2 trades con SL 2.5%, pierdes ~$1.25. El capital se va rápido.
   - Se recomienda empezar con $50-$100 mínimo si vas a hacer scalping con comisiones de spot.

2. Configuración:
   - Edita config.json. Cambia "mode" a "live" SOLO si tienes API keys con permisos de trading y entiendes los riesgos.
   - Las API keys deben tener IP restringida y solo permisos de "Spot Trading" (no retiros).

3. Papel primero (recomendado):
   - Deja "mode": "paper" y ejecuta: python bot.py
   - Observa las señales y los SL/TP propuestos. Ajusta los porcentajes si ves que son muy apretados o muy amplios para tu par.

4. Señales que usa (Laya System 1 — rápido):
   - EMA 9 > EMA 21 + RSI entre 50 y 70 + volumen > 1.2x media + vela alcista = señal LONG.
   - No hay análisis profundo; es una decisión de patrón. Esto es intencional (System 1 = rápido).

5. Gestión de riesgo aplicada:
   - Cada señal propone SL del 2.5% y TP del 5% (ratio 1:2). Esto protege el capital y no busca el máximo profit por trade.
   - Máximo $5 por operación con $20 de capital (25%). Si pierdes 4 veces seguidas, aún tienes reserva.
   - Solo 1 operación abierta a la vez (max_trades_open = 1).

6. Si decides ir a live:
   - Coloca manualmente las órdenes OCO (One-Cancels-Other) en Binance con SL y TP.
   - NO modifiques SL o TP después de entrar (disciplina).
   - Registra cada trade en un archivo (puedes agregar un log_csv en bot.py).

7. Limitaciones conocidas de este diseño:
   - Deep Analysis (System 2) con datos fundamentales en tiempo real: Fear & Greed, CoinGecko macro, titulares RSS, MTF 1h. Requiere GEMINI_API_KEY (o equivalente) y `deep_analysis.enabled: true` en config.json.
   - No usa aprendizaje automático (ML) — es un bot basado en reglas técnicas.
   - No garantiza ganancias; es un marco técnico, no un producto financiero.

Estado del archivo:
- README.md: diseño y filosofía
- config.json: parámetros (edita con cuidado)
- bot.py: código ejecutable (paper por defecto)
