LAYA — Agente de Trading con IA (System 1 · modo paper)
Presupuesto: $20 USD por agente (capital muy limitado — comisiones Binance pueden consumirlo rápido)
Objetivo: un agente autónomo que decide rápido tipo "System 1" con señales técnicas, sin análisis profundo.
Tipo: Scalping (1-5 min) / Midterm (1-4h). Entrada precisa, salida cautelosa.

Filosofía Laya System 1 para trading:
- Decisión rápida basada en señales pre-filtradas, no análisis fundamental profundo.
- Sistema 1 = intuición basada en patrones (indicadores + acción de precio).
- Stop loss y take profit definidos ANTES de la orden (evita decisión emocional después).
- No busca máximo profit por trade; busca consistencia y protección del capital.

Señales más efectivas (evidencia técnica):
1. EMA 9 / EMA 21 crossover + volumen creciente (confirmación de momentum)
2. RSI (14) > 50 para largos, < 50 para cortos; evitar zonas extremas (>70 / <30) sin divergencia
3. Soporte/Resistencia local (último pivot) como referencia de SL/TP
4. MACD histograma cruzando cero con pendiente (confirmación tendencia)
5. Volumen relativo > 1.2x media (confirmación de movimiento real, evita falsos)

Reglas de gestión de riesgo con $20:
- Comisiones Binance (spot real): estándar 0.1% → $0.02 por lado, ciclo completo (abrir + cerrar) $0.04 (~500 trades con $20). BNB 0.075% → $0.015/$0.03 (~666). Futuros Taker 0.05% → $0.01/$0.02 (~1000). Futuros Maker 0.02% → $0.004/$0.008 (~2500).
- Orden mínima spot: ~$5-$10 según par (obliga 50-100% del capital con $20, sin diversificación ni promedio).
- Posición máxima: no más del 25% del capital ($5) por trade. Deja reserva para comisiones y pérdidas.
- Futuros (Binance Futures): fee más bajo (~0.02% maker), pero mayor riesgo. Apalancamiento alto (10x-20x) puede liquidar $20 en segundos por una fluctuación mínima. Funding fee: cada 8h.
- Binance Spot fee: ~0.1% por lado. En 10 trades: ~$0.4 de comisión. Impacto real con $20.
- Stop Loss (SL): 1.5% - 3% del precio de entrada (apretado para scalping; más amplio para midterm).
- Take Profit (TP): 2.5% - 5% (ratio TP/SL ~ 1.5:1 a 2:1). No exprimir máximo; salir antes.
- Máximo trades simultáneos: 1 (capital limitado). No diversificar con $20.
- Máximo pérdida diaria: 10% del capital ($2). Si se alcanza, detener todo por hoy.

Flujo de decisión (Laya System 1 — rápido):
1. Filtrado (pre-trade): par BTC/USDT o ETH/USDT (alta liquidez, bajo spread). Evitar altcoins ilíquidas.
2. Señal: EMA 9 > EMA 21 + RSI(14) > 50 + volumen > 1.2x + precio cerca de soporte (largo). Invertir para corto si se quiere.
3. Confirmación rápida (si/no): ¿Precio rompió el pivot con volumen? Si no, descartar.
4. Entrada: orden límite cerca del precio actual (evita deslizamiento). Tamaño: $5 máximo.
5. Inmediatamente: configurar SL y TP en Binance (orden OCO: One-Cancels-Other). No modificar después (disciplina).
6. Salida: si llega a TP → vender. Si llega a SL → aceptar pérdida. No esperar "a ver si rebota".
7. Registro: anotar resultado, señal usada, y emoción (si se sintió tentación de mover SL/TP).

Implementación técnica propuesta:
- Python 3.12 + python-binance (cliente Binance oficial).
- Datos: Binance REST API / WebSocket (klines) para precios en tiempo real.
- Indicadores: pandas + ta-lib / ta (biblioteca de análisis técnico).
- Ejecución: paper trading primero; luego live con API keys (restricciones de IP, permisos de trading desactivados por defecto — ACTIVAR MANUALMENTE con cuidado).
- Almacenamiento: SQLite local para historial de trades y señales.

Riesgos explícitos (leer antes de cualquier paso):
- $20 es muy poco para operar en Binance con comisiones de spot; una secuencia de pérdidas pequeñas puede liquidar el capital.
- El trading automático NO garantiza ganancias; puede perder todo el capital.
- Binance requiere verificación (KYC) para operar con dinero real.
- Las API keys son sensibles; nunca subirlas a repositorios públicos.
- Esto NO es asesoramiento financiero; es un diseño técnico para aprender.

Estado: diseño en archivo. Código ejemplo en bot.py (paper/live configurable). No se conectará a Binance hasta que confirmes.

NUEVO — Interfaz dinámica y análisis profundo:
- dashboard.html: interfaz dinámica (local) que lee backtest_log.json. Muestra señales Laya, análisis profundo (si habilitado) y tabla de trades simulados.
- deep_analysis.py: capa System 2 (LLM) con prompt estructurado. Configurado en config.json (enabled: false por defecto). Usa Gemini/LLM para recomendaciones ABSTAIN/LONG/SHORT/WAIT que filtran a Laya.
- backtest_log.json: registro automático de cada ciclo del bot. Se genera al correr bot.py.

NUEVO — Dashboard en tiempo real (TradingView Lightweight Charts):
- server.py: servidor Flask que ejecuta al agente Laya System 1 en modo paper (mercado simulado en tiempo real) y expone API REST + Server-Sent Events (SSE).
- dashboard.html (servido por server.py): gráfico dinámico de velas con EMA 9/21, RSI y volumen usando la biblioteca open source "TradingView Lightweight Charts" (Apache 2.0). Muestra cartera actual, activos/posición abierta, órdenes activas (SL/TP), historial de trades y el log de tareas de Laya en vivo.
- Selector de par (BTC, ETH, BNB, XRP/Ripple, SOL, ADA, DOGE, LINK, AVAX, LTC, DOT, TRX) con estado independiente por activo: cartera, posición, historial y tareas propios de cada par. La simulación usa movimientos porcentuales, así que funciona igual con BTC (~$65k) y con XRP (~$0.62).
- Selector de periodo del gráfico (5s, 15s, 1m, 5m, 15m, 1H, 4H, 1D): cambia el intervalo de las velas y la escala temporal del gráfico. La simulación es acelerada (1 vela cada 2 s de tiempo real), así que el eje temporal avanza más rápido que el reloj.
- Botones de control del agente: Activar, Pausar, Parar y Reiniciar (este último reinicia solo el par activo).
- Diagnóstico de señal en vivo: cuando la señal es "NONE", el panel de señal muestra qué condición falta (EMA9/EMA21, RSI, volumen o vela alcista) para que se vea que el agente está evaluando y no colgado.
- Herramientas del gráfico: zoom ±, ajustar a pantalla, escala logarítmica/lineal, descargar captura PNG, pantalla completa, crosshair con tooltip (OHLC + volumen + RSI + EMAs), marcadores de entrada/salida de trades (L/S y TP/SL) y toggles para mostrar/ocultar EMA, Volumen, RSI y Trades.
- Herramientas de dibujo (capa propia sobre el canvas del gráfico): línea de tendencia, línea curva (bézier), curva a mano alzada, línea horizontal, línea vertical, rectángulo, texto, deshacer y borrar todo. Los dibujos se anclan a precio + índice de vela, así se mantienen al hacer zoom/pan y al llegar velas nuevas.
- Visor de procesamiento en vivo: un panel que muestra el pipeline del ciclo actual (Mercado → Indicadores → Filtro → Decisión) y un stream con lo que Laya calcula en cada ciclo (precio, EMAs, RSI, volumen y señal), para observar el "razonamiento" del agente en tiempo real.

NUEVO — Estado persistente y capacidad operativa:
- Persistencia del estado (sim_state.json): el estado de todos los simuladores (inicio/pausa/parada, saldo, posiciones abiertas, historial, tareas y velas de TODOS sus pares) se guarda en `sim_state.json` cada 10 s, tras cada acción de control y al cerrar el servidor (Ctrl+C). Recargar la página o reiniciar el servidor NO pierde una simulación iniciada: sigue iniciada donde quedó (el reloj de las velas se reancla a la hora actual).
- Cambio de moneda funcional: el selector de par muestra los 12 activos y funciona en cualquier simulador. Si eliges un par que el simulador aún no opera, se añade automáticamente (con mensaje en el log de tareas; en multi-activo la cartera se comparte) y la gráfica cambia inmediatamente al mercado nuevo (con su propia escala de precio). Cambiar de simulador activo también reconstruye la gráfica con los datos correctos.
- Simulaciones simultáneas: el bucle del servidor avanza todos los simuladores en paralelo y cada uno puede estar iniciado/pausado/detenido de forma independiente. El panel "Simuladores en paralelo" lista todos con su saldo, patrimonio, PnL, posiciones y estado; tiene botones por simulador (Activar/Pausar/Parar/Eliminar) que funcionan aunque estés viendo otro, permite crear simuladores nuevos (nombre + capital + uno o varios pares) y cambiar el saldo del simulador activo cuando está detenido.

NUEVO — Binance Testnet con datos reales (Spot y Futuros USDⓈ-M):
- config.json: `"binance": {"testnet": true}` y `"mode": "live"`. Las claves del config son de testnet (verificadas con testnet_check.py): saldo ficticio (~10,000 USDT + BTC/ETH/BNB de prueba) y velas REALES de mercado. Dinero real nunca entra en juego mientras testnet sea true.
- bot.py en modo live conecta a https://testnet.binance.vision y calcula señales con klines REALES (con fallback sintético si falla la conexión). Requiere `pip install python-binance` (ya instalado). La colocación de órdenes AÚN no está implementada (solo señales y registro).
- Dashboard con selector de origen de datos: el desplegable "🎲 Datos: Simulado / 📡 Spot Testnet (reales) / ⚡ Futuros Testnet (reales)" de la barra de control alterna el origen de las velas. Los modos reales usan `testnet.binance.vision` (spot, `/api/v3/...`) o `testnet.binancefuture.com` (futuros USDⓈ-M, `/fapi/v1/...`), ambos públicos y sin claves: la gráfica muestra velas reales (poller cada 10 s, actualización de la vela parcial en vivo) y el agente paper opera encima de esos precios reales. La cartera NO usa órdenes de testnet: sigue siendo paper (saldo simulado gestionado por el simulador, ahora anclado a precios reales).
- ANALISIS: bot.py lee klines de spot; el dashboard puede alimentarse de spot o de futuros. Si quieres que bot.py lea de la testnet de futuros, baja su propio cliente con `Client(..., testnet=True)` y endpoints `/fapi` (no implementado aún).

NUEVO — Modo FUTUROS USDⓈ-M Testnet (SHORT en paper):
- Al elegir "⚡ Futuros Testnet (reales)" el dashboard opera contra velas reales de perpetuos (los 12 pares del proyecto cotizan allí) y ACTIVA el SHORT paper: posiciones cortas apalancadas con margen, nocional y precio de liquidación calculados (`liq = entrada × (1 ± 1/lev)`), margen reservado del saldo y pérdida máxima limitada exactamente al margen (liquidación nunca rompe la cartera).
- En modo spot las señales SHORT se registran con aviso ("ignorado en spot paper") y no se abren; en futuros se abren como SHORT con el apalancamiento del bloque `strategy.leverage` (por defecto x5).
- TODO sigue siendo paper: no se colocan órdenes en ninguna API de Binance. El apalancamiento solo afecta al modelo matemático del simulador.

NUEVO — Pulso pulido de Laya System 1 (ciclo de decisión en el snapshot):
- **Presentación a 60fps (presentador SMOOTH)**: los datos siguen llegando por SSE con el timer del servidor (sin cambios), pero el dashboard separa presentación de datos: un bucle `requestAnimationFrame` interpola el precio grande, equity, PnL no realizado y PnL de posición, y repinta la vela viva + EMA9/EMA21 + RSI de la gráfica con suavizado exponencial hacia la última verdad del snapshot. Sin saltos por segundo: todo gliding fluido; cuando el valor se clava en el objetivo, el repintado se detiene (coste ~0). LAYA EYE usa el precio interpolado para el punto vivo de la escala.

NUEVO — **Intelligence Analytics** (la mente de Laya contabilizada):
- Servidor: cada simulador graba una **curva de equity** (1 punto por ciclo, hasta 900 con decimado automático de lo viejo, persistida en sim_state.json). En cada snapshot emite `analytics` de forma gratis: score (0–100 + grado A+…D), profit factor (a infinito si nunca pierde), expectancy ($ y % por operación), acierto, max/current drawdown, sharpe por operación, media de barras en posición, mejor/peor trade, rachas (actual W/L y mejores históricas).
- **Agrupación de operaciones lógicas**: el TP1 parcial y el cierre final comparten identidad (par + hora de apertura) → cuentan como UNA operación; la estadística no infla el win rate con parciales.
- Desgloses: por lado (LONG vs SHORT), por par (ordenado por PnL) y por razón de cierre (Take Profit / TP1 parcial / Trailing / Stop Loss / Liquidación / Salida momentum / Stop de tiempo / Manual).
- Panel en el dashboard (canvas + fichas): fichas de score/PF/expectancy/acierto/DD/racha; curva de equity con línea base del capital inicial y minigráfico de drawdown underwater; barras de PnL por operación con marca púrpura cuando hubo TP1 parcial; desglose de razones. Redibuja solo al llegar datos o al destello de trade nuevo (barato en CPU).
- **Contabilidad transparente en la Cartera (KPI)**: la identidad se ve a simple vista — Cartera = Efectivo libre + En posición (margen reservado) + PnL no realizado, y el backend expone `portfolio.reserved_margin` y `portfolio.max_daily_loss`. El "PnL de hoy" es el PnL realizado de la jornada: resetea a medianoche (persistido, sobrevive reinicios del servidor) y es el valor que vigila el límite de pérdida diaria (el agente se detiene al tocarlo). "PnL realizado (total)" sigue siendo el acumulado de todo el historial.
- **Órdenes activas enriquecidas**: cada orden de salida (Stop Loss, Take Profit, TP1 parcial, Liquidación) incluye el **monto operable** (cantidad que ejecuta × nivel, en USD), la **distancia %** que el precio debe viajar para dispararla y el **PnL $ del nivel** (resultado si ese nivel ejecuta, vs entrada; liquidación = margen exacto). Además, un encabezado por posición agrupa sus órdenes con contexto: par, lado, apalancamiento, margen, PnL vivo, barras en posición y trailing ON. El fallback demo del front emite los mismos campos.
- **LAYA EYE (motor visual en canvas)**: panel desplegable con el funcionamiento interno en vivo — las 6 CAPAS del agente (Datos → Indicadores → Filtro → Decisión → Gestión → Cartera) con paquetes animados que recorren el pipeline en cada ciclo; la escala de la posición con SL/TP1/trailing/TP/liq y el precio latiendo en su nivel; y la cartera paper con equity, barra de PnL, sparkline de velas y fuerza de señal L/S. Pasa el ratón por el canvas para congelar la animación.
- El servidor emite `pulse` con los 5 pasos del ciclo: Mercado → Indicadores → Filtro → Decisión → Gestión, cada uno con estado (ok/pendiente) y detalle en vivo; el visor "Procesamiento" y el diagrama de flujo del cardiograma los muestran tal cual.
- Chip "Fuerza" de señal: porcentaje de condiciones cumplidas por dirección (EMA/RSI/Volumen/Vela) para LONG y SHORT.
- ATR(14) calculado por mercado; indica la volatilidad usada por el trailing.

NUEVO — Entradas y salidas estratégicas (bloque `strategy` de config.json):
- `tp1_pct` (0.025): al avanzar +2.5% a favor se ejecuta **TP1 parcial** — cierra `tp1_size_pct` (50%) de la posición y asegura ganancias.
- `break_even` (true): tras TP1 el SL sube a la entrada (riesgo residual cero).
- `trail_atr_mult` (2.0): la parte restante ("runner") usa **trailing por ATR**: nivel = mejor precio alcanzado − 2×ATR (nunca por debajo del break-even) y cierra si se toca.
- `momentum_exit` (true): si aparece una señal contraria con fuerza ≥ 75%, sale el resto ("momentum flip").
- `time_stop_bars` (0 = off): cierra tras N barras sin alcanzar SL/TP ni objetivos.
- `cooldown_bars` (6): tras cerrar un trade, el par queda N barras "en cooldown" sin re-entrar (evita repetir entradas en la misma barra de ruido).

NUEVO — Anti-churn / anti-persecución (bloque `strategy` de config.json), afinado con el análisis de efectividad:
- `momentum_grace_bars` (2): ignora la salida-momentum en las primeras N barras de la posición. El contragolpe inmediato tras la entrada (churn de 1-4 barras) ya no corta trades que estaban funcionando.
- `momentum_min_strength` (60): la señal contraria debe tener fuerza ≥ 60% (junto al mínimo base de 75%) para expulsar la posición; bloques débiles de RSI/volumen no la cierran.
- `cooldown_loss_mult` (2.0): tras cerrar con pérdida, el cooldown se multiplica (6 → 12 barras). Tras ganancia se mantiene el base.
- **Cooldown por velas reales**: el cooldown ahora cuenta VELAS del feed, no ticks — en spot/futuros 5m significa minutos reales entre entradas por par (antes, en feed real, 6 "barras" duraban solo 12 s, lo que propiciaba el churn).
- `max_ext_atr` (1.8): bloquea la entrada si el precio quedó > 1.8×ATR lejos de la EMA21 (LONG tras celo / SHORT tras dump) — evita perseguir movimientos agotados.
- `min_body_ratio` (0.35): la vela de señal debe tener cuerpo real (|cierre−apertura| ≥ 35% del rango alto-bajo) — las velas de mecha/doji no disparan entradas.
- `sl_slip_atr` (0.25): el SL/trailing en paper rellena en su nivel con un deslizamiento máximo de 0.25×ATR — un hueco violento del testnet ya no convierte un SL de -2.5% en una pérdida de -46% (como el trade #63 del diagnóstico).
- Filtros desactivables a 0/0.0 (defaults neutros); todos editables en el panel (CONFIG_SPEC) y visibles en "Esperando: ..." cuando bloquean una señal.
- El panel de órdenes activas muestra SL (o trailing), TP, TP1 parcial y el precio de liquidación en futuros; el historial marca cierres parciales, apalancamiento y barras en posición.
- Todos los parámetros se ajustan en config.json (`"strategy": {...}`) y sobreviven a reinicios (persisten en el estado).
- Los periodos 5s/15s no existen en las testnets, así que en modo real siguen simulados; 1m/5m/15m/1H/4H/1D sí son reales. Pares no disponibles en testnet caen automáticamente a simulado con aviso en el log de tareas.
- El modo de origen de datos se persiste en sim_state.json junto con el resto del estado: reiniciar el servidor conserva qué mercados eran reales, de qué kind (spot/futuros) y cuáles simulados.
- Verificación de claves/endpoints: `python testnet_check.py` (solo lectura, no opera). Prueba del motor estratégico: `python test_strategy.py` (offline, sin red ni estado).

NUEVO — Pulsos de Laya ampliados (sección "Pulsos de Laya" del dashboard):
- **Barra de fuerza de señal** en el nodo 3 · Filtro: porcentaje 0–100% de fuerza por dirección (usa `pulse.strength`/`diagnostics.strength` del servidor; en local cuenta las condiciones cumplidas del filtro), con etiquetas "L nn% · S nn%" y la barra teñida de rojo si domina el SHORT.
- **Nodo 5 · Gestión**: el diagrama de flujo ahora tiene 5 nodos (Mercado → Indicadores → Filtro → Decisión → Gestión). Gestión muestra en vivo la posición abierta: lado + apalancamiento, SL, TP1 (✓ si ya ejecutó), Break-even, trailing, TP, precio de liquidación, entrada, PnL y barras en posición; sin posición muestra la espera o el cooldown restante.
- **Ramas con bloqueos reales**: cada rama (LONG / NONE / SHORT) indica el motivo real que la impide, en ámbar con "⛔": agente pausado/detenido, cooldown activo (N barras), pérdida diaria al tope, máximo de posiciones abiertas, SHORT desactivado o mercado sin futuros. Una rama bloqueada NO se ilumina aunque la señal exista.
- **Historial de señales** (bajo el ECG): últimas 8 decisiones con hora, señal (LONG/SHORT/NONE), barra de intensidad y marca ✦ cuando en ese latido se ejecutó un trade; contador de entradas registrado.
- **"Última señal hace Xs"**: contador vivo bajo la stat "Última señal" (segundos / minutos / horas). Solo se reinicia con señales no-NONE: un NONE no borra el reloj.
- Todo se limpia con `pulseReset()` (cambio de par/simulador o de origen de datos).

NUEVO — Panel "⚙️ Configuración de Laya" (parámetros editables en vivo):
- Tarjeta desplegable bajo la barra de control con 4 grupos: **Riesgo** (SL, TP, tamaño máx. de posición, pérdida diaria máx., trades abiertos máx.), **Estrategia** (TP1, fracción TP1, break-even, trailing ATR, salida momentum, stop de tiempo, cooldown, SHORT on/off, futuros paper, apalancamiento y los 6 filtros anti-churn: gracia momentum, fuerza mínima, cooldown×pérdida, extensión ATR, cuerpo mínimo de vela, deslizamiento SL), **Indicadores** (EMA rápida/lenta, periodo RSI, factor de volumen) y **Velocidad y capital** (intervalo de tick, capital inicial). Sliders sincronizados con inputs numéricos y switches para los booleanos.
- **Aplicar en vivo**: `POST /api/config` → `Manager.update_config()` valida y recorta cada parámetro (`CONFIG_SPEC`: tipo + rango), lo escribe en el dict `params` COMPARTIDO por referencia (Market/Simulator lo ven al instante), recalcula indicadores y señal de todos los mercados, añade entrada al log de tareas y sube la versión del snapshot. Sin reiniciar nada.
- **Persistencia**: si todo se aplicó, se guarda `config.json` de forma atómica (tmp + `os.replace`) más `sim_state.json`; los cambios sobreviven a reinicios. El estado saneado viaja en `snapshot.config`, así que el panel se repuebla solo en cada snapshot (salvo que el usuario esté editando: con cambios sin aplicar no se pisa, y el tag "Cambios sin aplicar" lo indica; "↺ Restablecer" vuelve a los valores aplicados).
- **Espejo 1:1**: las 27 claves `CONFIG_SPEC` del servidor = las 27 filas `data-ck` del HTML = las 27 claves de `CFG_META`/`Sim.config()` (verificado por script). Los bloques de config.json se respetan (`CONFIG_BLOCK`: raíz / `indicators` / `strategy`) y las claves con nombre distinto en params se remapean (`CONFIG_PARAM`: `max_position_usd`→`max_pos`, `max_daily_loss_usd`→`max_daily_loss`, `max_trades_open`→`max_open`, `volume_factor`→`vol_factor`, `capital_usd`→`capital0`).
- `tick_interval` es dinámico: `Manager.tick_interval` (no la constante global) gobierna el bucle y el snapshot; cambiarlo desde el panel reajusta la velocidad del agente al instante. En modo local el panel aplica igual vía `Sim.applyConfig()` (reinicia su timer y recalcula pares).
- Nota: `capital_usd` solo afecta a resets y agentes nuevos (no muta el saldo vivo); el panel lo indica en su pie de nota.

Para ejecutar el dashboard en tiempo real:
1. Instala Flask si no lo tienes: pip install flask
2. Ejecuta: python server.py
3. Abre en tu navegador: http://127.0.0.1:5050
4. Pulsa "Activar" y observa cómo actúa Laya en tiempo real (señales, órdenes, PnL).

Nota: la biblioteca de gráficos se sirve localmente desde `vendor/lwc.js` (con CDN como respaldo, sin depender de internet). Si abres `dashboard.html` sin `server.py` (por ejemplo con un servidor estático), el dashboard detecta que no hay API y entra en "modo local (simulado)": toda la simulación del agente corre en el navegador y los botones igual funcionan. El mercado es sintético (no usa las API keys de Binance) y NO opera con dinero real.
