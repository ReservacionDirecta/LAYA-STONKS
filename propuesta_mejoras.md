
=== ANÁLISIS DEL PUNTO ACTUAL (sim_state.json actualizado) ===
Estado persistido: 2026-10-29T00:03:42
Simuladores: sim-1 (BTC Solo), sim-2 (ETH Solo), sim-3 (Multi BTC/ETH/BNB)

--- HALLAZGOS POR SIMULADOR ---
1. sim-1 BTC Solo (102 trades): WR 57.8%, PnL +$3.63, 0 posiciones. Bueno. 36% salidas momentum.
2. sim-2 ETH Solo (95 trades): WR 55.8%, PnL +$2.44, 0 posiciones. Aceptable. 34% salidas momentum.
3. sim-3 Multi (174 trades): WR 53.4%, PnL +$3.90, pero BNB -$0.42 (58t, 55.2% WR, 1 liquidación).

--- PROBLEMAS IDENTIFICADOS ---
• BNBUSDT en multi-par: PnL negativo (-$0.42), 1 liquidación (futuros), 55 trades. El par no funciona con los params actuales.
• Momentum adverso: ~35-36% de salidas en todos los simuladores, con PnL promedio negativo. El filtro sale en contragolpes débiles.
• ETH en multi-par tiene mejor PnL (+$3.00) que solo (+$2.44), indicando sinergia positiva del contexto multi.
• Comisiones son bajas ($0.23-$0.30 con $20 inicial) — no son el cuello de botella.

--- MEJORAS PROPUESTAS ---
1. Excluir BNB del multi-par (o usar max_ext_atr más estricto para BNB: 1.8→1.2, min_body_ratio 0.35→0.5)
2. Aumentar momentom_grace_bars (ya aplicado: 5) y momentom_min_strength (ya aplicado: 75) — verificar tras 50 trades más
3. Para multi-par: limitar trades simultáneos por par o agregar cooldown más largo (6→10) para evitar overtrading
4. Deep Analysis (System 2): el prompt ahora incluye datos históricos por par y datos fundamentales — verificar con API key
5. Estética: paleta aplicada, modal de configuración, botón JSON para análisis externo
