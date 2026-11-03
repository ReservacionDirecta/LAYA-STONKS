#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Laya STONKS — prueba del motor estratégico de Laya System 1 (paper).

Verifica, SIN red y SIN tocar sim_state.json:
  1. Apertura LONG spot (sin apalancamiento) con campos nuevos.
  2. TP1 parcial (50%) + break-even + trailing activado.
  3. Cierre del resto con el trailing y saldo consistente.
  4. SHORT en futuros: margen/nocional/liq y cierre por TP con PnL correcto.
  5. Liquidación de futuros: pérdida limitada exactamente al margen.
  6. Cooldown: no re-entra hasta terminar las barras.
  7. SHORT ignorado en modo spot con aviso limitado.
  ... y el resto de secciones (finales incluidas) hasta la 14:
      13) Comisiones Binance reales (spot 0.1% / futuros 0.05%, neto al cierre).
      14) Backtest offline por el mismo circuito + helpers de contexto (sin red).
Uso: python test_strategy.py
"""

import time
import server

TF = 300
T0 = int(time.time()) - TF * 400
FAILS = []


def check(name, cond, extra=""):
    status = "OK " if cond else "FALLO"
    print(f"  [{status}] {name}" + (f" — {extra}" if extra else ""))
    if not cond:
        FAILS.append(name)


def make_sim(futures, pairs=("BTCUSDT",), max_open=5):
    params = {"max_pos": 5.0, "sl_pct": 0.025, "tp_pct": 0.05, "max_open": max_open,
              "max_daily_loss": 2.0, "tf_seconds": TF,
              "ema_fast": 9, "ema_slow": 21, "rsi_period": 14, "vol_factor": 1.2}
    params.update(server.STRATEGY_DEFAULTS)
    params["tp_sl_mode"] = "fixed"
    params["futures_enabled"] = futures
    params["fees_enabled"] = False   # las secciones 13 activan fees explícitamente:
                                     # todas las pruebas previas esperan PnL sin comisiones
    sim = server.Simulator("t", "T", 20.0, list(pairs), params)
    m = None
    px = 100.0
    for p in pairs:
        m = sim.markets[p]
        m.candles = []
        px = 100.0
        for i in range(30):
            o = px
            px = px + (1 if i % 3 == 0 else -0.5)
            m.candles.append({"time": T0 + i * TF, "open": o, "high": max(o, px) + 0.4,
                              "low": min(o, px) - 0.4, "close": px, "volume": 100})
        m.price = px
        m.recompute()
        m.evaluate()
        m.feed = "spot"          # el test controla las velas manualmente:_market.tick() no-op
    return sim, m


def push(m, px):
    o = m.price
    t = m.candles[-1]["time"] + TF
    m.candles.append({"time": t, "open": o, "high": max(o, px) + 0.4,
                      "low": min(o, px) - 0.4, "close": px, "volume": 100})
    if len(m.candles) > server.MAX_CANDLES:
        m.candles = m.candles[-server.MAX_CANDLES:]
    m.price = px
    m.recompute()
    m.evaluate()


print("=" * 62)
print(" 1) LONG spot: apertura sin apalancamiento")
sim, m = make_sim(False)
m.signal = "LONG"
sim.tick()
pos = sim.positions.get("BTCUSDT")
check("posición LONG abierta", pos is not None)
check("lev=1 / nocional=margen", pos and pos["leverage"] == 1 and float(pos["notional"]) == float(pos["size_usd"]))
check("sin liq en spot", pos and pos["liq"] is None)
check("tp1 = entrada*1.025", pos and abs(pos["tp1_price"] - pos["entry"] * 1.025) < 0.01,
      extra=f"tp1={pos['tp1_price']} entry={pos['entry']}" if pos else "")
check("cash descontado", abs(sim.cash - 15.0) < 1e-6, f"cash={sim.cash}")
eq, un, pv = sim.equity()
check("equity = 20 mientras no cambia precio", abs(eq - 20.0) < 0.5, f"eq={eq:.4f}")

print(" 2) TP1 parcial + break-even + trailing")
push(m, 102.8)                    # cruza tp1 (102.5), no tp (105)
sim.manage("BTCUSDT")
pos = sim.positions["BTCUSDT"]
partials = [t for t in sim.history if t.get("partial")]
check("parcial registrada en historial", len(partials) == 1,
      extra=f"qty={pos['qty']:.6f}" if pos else "sin pos")
check("tp1_done=True", pos["tp1_done"] is True)
check("SL movido a entrada (BE)", abs(pos["sl"] - pos["entry"]) < 1e-9, f"sl={pos['sl']}")
check("trailing activado", pos["trail"] is not None, f"trail={pos['trail']}")

print(" 3) Cierre del resto por stop; saldo consistente")
cash_antes = sim.cash
push(m, 96.0)                     # cruza trailing (suelo BE=100)
sim.manage("BTCUSDT")
check("posición cerrada", "BTCUSDT" not in sim.positions)
tr = sim.history[-1]
check("cierre con PnL percibible", abs(sim.cash - cash_antes) > 0.01,
      f"cash={sim.cash:.4f} pnl={tr['pnl']:.4f} reason={tr['reason']}")
total = sum(t["pnl"] for t in sim.history)
check("cartera = capital inicial + PnL total", abs(sim.cash - (20.0 + total)) < 1e-6,
      f"cash={sim.cash:.4f} pnl_total={total:.4f}")

print(" 4) Cooldown por velas reales: bloquea la re-entrada")
m.signal = "LONG"
sim.tick()
check("cooldown activo tras cerrar", sim.cooldown.get("BTCUSDT", 0) > 0 and "BTCUSDT" in sim.cooldown_till,
      extra=str(sim.cooldown))
sim.tick()
check("tick sin vela nueva: sigue bloqueada la re-entrada", "BTCUSDT" not in sim.positions)
for _ in range(int(server.STRATEGY_DEFAULTS["cooldown_bars"])):
    push(m, m.price)                 # el cooldown avanza con las velas del feed
    m.signal = "LONG"
    sim.tick()
check("re-entró tras agotar el cooldown en velas", "BTCUSDT" in sim.positions)
if "BTCUSDT" in sim.positions:
    sim.close("BTCUSDT", m.price, "Cierre de prueba")

print(" 4b) PnL mixto sigue consistente")
total = sum(t["pnl"] for t in sim.history)
check("cartera = capital inicial + PnL total", abs(sim.cash - (20.0 + total)) < 1e-6,
      f"cash={sim.cash:.4f} pnl_total={total:.4f}")

print(" 5) SHORT en futuros: sizing y TP con apalancamiento")
sim2, m2 = make_sim(True)
m2.signal = "SHORT"
sim2.tick()
pos = sim2.positions.get("BTCUSDT")
check("posición SHORT abierta", pos is not None and pos["side"] == "SHORT")
check("lev=5", pos and pos["leverage"] == 5)
check("nocional = 5*margen", pos and float(pos["notional"]) == float(pos["size_usd"]) * 5,
      extra=f"notional={pos['notional']}" if pos else "")
check("qty = nocional/precio", pos and abs(pos["qty"] - 25.0 / m2.price) < 0.01)
check("liq por encima de la entrada", pos and pos["liq"] > pos["entry"], f"liq={pos['liq']}")
cash_antes = sim2.cash
push(m2, pos["tp"] * 0.999)
sim2.manage("BTCUSDT")
check("TP SHORT cerrado", "BTCUSDT" not in sim2.positions)
tr = sim2.history[-1]
esperado = (pos["entry"] - tr["exit"]) * 25.0 / pos["entry"]
check("PnL SHORT = (entrada-salida)*qty", abs(tr["pnl"] - esperado) < 0.01,
      f"pnl={tr['pnl']:.4f} esperado={esperado:.4f}")

print(" 6) Liquidación: pérdida clavada al margen")
sim3, m3 = make_sim(True)
m3.signal = "SHORT"
sim3.tick()
pos = sim3.positions["BTCUSDT"]
push(m3, pos["liq"] * 1.02)      # cruza la liquidación
sim3.manage("BTCUSDT")
check("cerrada por liquidación", "BTCUSDT" not in sim3.positions)
tr = sim3.history[-1]
check("reason Liquidación (futuros)", tr["reason"] == "Liquidación (futuros)", tr["reason"])
check("pnl = -precio margen", abs(tr["pnl"] - (-5.0)) < 1e-6, f"pnl={tr['pnl']:.4f}")
check("el daño no rompe la cartera", abs(sim3.cash - 15.0) < 1e-6, f"cash={sim3.cash:.4f}")

print(" 7) SHORT ignorado en modo spot (aviso limitado)")
sim4, m4 = make_sim(False)
m4.signal = "SHORT"
sim4.tick()
sim4.tick()
sim4.tick()
check("sin posición", "BTCUSDT" not in sim4.positions)
aviso = [t for t in sim4.tasks if "SHORT detectado" in t["message"]]
check("aviso de futuros emitido (máx 1 vez por ventana)", len(aviso) == 1, str(len(aviso)))

print(" 8) snapshot: pulse + ordenes estratégicas presentes")
sim, m = make_sim(False)
m.signal = "NONE"
snap = sim.snapshot_detail()
pulse = snap.get("pulse") or {}
ks = [s.get("k") for s in pulse.get("steps", [])]
check("pulse con 5 pasos", ks == ["Mercado", "Indicadores", "Filtro", "Decisión", "Gestión"], str(ks))
check("orden TP1 en snapshot (sin posición no aplica)", snap.get("active_orders") == [])
push(m, 200.0)
m.signal = "LONG"
sim.tick()
snap = sim.snapshot_detail()
tipos = {o["type"] for o in snap["active_orders"]}
check("órdenes SL/TP/TP1", {"Stop Loss", "Take Profit", "TP1 parcial"} <= tipos, str(tipos))
check("pulse gestión refleja posición", "LONG" in (pulse and snap["pulse"]["steps"][4]["v"]),
      snap["pulse"]["steps"][4]["v"] if snap.get("pulse") else "")

print(" 9) anti-spam en tick: aviso 'Máximo de' solo con señal real y máximo 1 por ventana")
NESIM_PAIRS = ("BTCUSDT", "ETHUSDT")
sim5, m5 = make_sim(False, pairs=NESIM_PAIRS, max_open=1)
sim5.params["note_gap"] = 0.5          # ventana de 0.5s para el limitador
mb = sim5.markets["BTCUSDT"]
me = sim5.markets["ETHUSDT"]
mb.signal = "LONG"
me.signal = "NONE"
sim5.tick()
check("BTC abierta (ocupa el único hueco)", "BTCUSDT" in sim5.positions)
# Ráfaga con señal NONE en ETHUSDT: NO debe haber avisos de "Máximo de"
for _ in range(12):
    sim5.tick()
n0 = len([t for t in sim5.tasks if "Máximo de" in t["message"]])
check("0 avisos con señal NONE", n0 == 0, f"avisos={n0}")
# Señal LONG en ETHUSDT: aviso UNA sola vez dentro de la ventana de tiempo
me.signal = "LONG"
for _ in range(12):
    sim5.tick()
notes = [t for t in sim5.tasks if "Máximo de" in t["message"]]
check("exactamente 1 aviso 'Máximo de' en ráfaga", len(notes) == 1, str(len(notes)))
check("singular correcto con max_open=1",
      len(notes) == 1 and "1 posición abierta" in notes[0]["message"]
      and "LONG" in notes[0]["message"] and "esperando hueco" in notes[0]["message"],
      notes[0]["message"] if notes else "")
# Expirada la ventana (>0.5s), puede volver a avisar: así el usuario no pierde el contexto
time.sleep(0.6)
sim5.tick()
notes2 = [t for t in sim5.tasks if "Máximo de" in t["message"]]
check("vuelve a avisar tras expirar la ventana", len(notes2) == 2, str(len(notes2)))
check("ETHUSDT nunca abrió (max_open=1)", "ETHUSDT" not in sim5.positions)

print("10) Intelligence Analytics: métricas, desgloses y score")
simA, mA = make_sim(False)
simA.params["cooldown_bars"] = 0
an0 = simA.snapshot_detail()["analytics"]
check("sin trades: score en [0,100] y grado '-'",
      0 <= an0["score"] <= 100 and an0["grade"] == "-", f"{an0['score']}/{an0['grade']}")
check("equity curve vacía sin ticks", len(an0["equity"]) == 0)

push(mA, 101.0)
mA.signal = "LONG"
simA.tick()
simA.close("BTCUSDT", mA.price, "TP alcanzado")     # oper 1: PnL 0 (gana por definición >=0)
push(mA, 102.0)
mA.signal = "LONG"
simA.tick()                                          # oper 2: entrada 102
push(mA, 106.0)
simA.manage("BTCUSDT")                               # dispara TP1 parcial
simA.close("BTCUSDT", mA.price, "Trailing Stop (ATR)")
mA.signal = "NONE"                                   # evitar re-entrada al tocar la curva
simA.tick()                                          # puntos de curva extra

an = simA.snapshot_detail()["analytics"]
t = an["tiles"]
check("2 operaciones lógicas (parcial+final agrupadas)", an["trades"] == 2, str(an["trades"]))
check("total_pnl = cash - capital (deriva de redondeo acotada)",
      abs(t["total_pnl"] - (simA.cash - 20.0)) < 1e-3,
      f"total_pnl={t['total_pnl']} cash={simA.cash}")
check("win_rate 100% con 2 cerradas Ganadoras",
      t["win_rate"] == 100.0 and t["wins"] == 2 and t["losses"] == 0, str(t["win_rate"]))
check("profit factor infinito (sin pérdidas)", t["profit_factor"] is None, str(t["profit_factor"]))
check("max DD 0% (equity solo sube)", t["max_dd_pct"] == 0.0, str(t["max_dd_pct"]))
check("desgloses por lado", an["by_side"]["LONG"]["trades"] == 2 and an["by_side"]["LONG"]["wins"] == 2)
razones = {d["reason"]: d for d in an["by_reason"]}
check("razón Take Profit contabilizada", razones.get("Take Profit", {}).get("n") == 1, str(razones))
check("razón Trailing contabilizada", razones.get("Trailing", {}).get("n") == 1)
check("lista de trades con parcial marcado",
      any(x["partials"] >= 1 for x in an["trade_list"]), str([(x['partials']) for x in an['trade_list']]))
check("curva de equity con puntos de tick", len(an["equity"]) >= 3, str(len(an["equity"])))
check("score en [0,100] con grado real", 0 <= an["score"] <= 100 and an["grade"] != "-",
      f"{an['score']}/{an['grade']}")
check("expectancy positiva", t["expectancy"] > 0, str(t["expectancy"]))

print("11) PnL de hoy: reset por cambio de fecha + campos de cartera")
simB, mB = make_sim(False)
mB.signal = "NONE"
push(mB, 100.0)
simB.daily = 5.0
simB.daily_date = "2000-01-01"                       # dia viejo
simB.tick()
hoy = server.now_iso()[:10]
check("cambio de dia: PnL de hoy resetea a 0",
      simB.daily == 0.0 and simB.daily_date == hoy, f"{simB.daily} / {simB.daily_date}")
simB.daily = -3.0
simB.tick()
check("mismo dia: no vuelve a resetear", simB.daily == -3.0, str(simB.daily))
pf = simB.snapshot_detail()["portfolio"]
check("portfolio expone margen reservado y limite diario",
      "reserved_margin" in pf and "max_daily_loss" in pf,
      f"reserved_margin={pf.get('reserved_margin')} max_daily_loss={pf.get('max_daily_loss')}")
check("identidad: equity = efectivo + margen + no realizado",
      abs(pf["equity"] - (pf["cash"] + pf["reserved_margin"] + pf["unrealized_pnl"])) < 1e-3,
      f"eq={pf['equity']} vs suma={pf['cash'] + pf['reserved_margin'] + pf['unrealized_pnl']}")

# -- órdenes activas enriquecidas: monto, distancia, PnL de nivel --
simB.daily = 0.0
mB.signal = "LONG"
simB.tick()                                          # abre posicion paper
pc = simB.snapshot_detail()
ords = pc["active_orders"]
check("ordenes enriquecidas (usd, dist_pct, pnl_level, contexto)",
      bool(ords) and all(("usd" in o and "dist_pct" in o and "pnl_level" in o
                          and "pside" in o and "margin" in o) for o in ords),
      (str(ords[0]) if ords else "sin ordenes"))
o_sl = next((o for o in ords if o["type"] == "Stop Loss"), None)
o_tp = next((o for o in ords if o["type"] == "Take Profit"), None)
check("distancia: SL por debajo y TP por encima (LONG)",
      o_sl is not None and o_tp is not None and o_sl["dist_pct"] < 0 and o_tp["dist_pct"] > 0,
      f"sl={o_sl['dist_pct'] if o_sl else None}% tp={o_tp['dist_pct'] if o_tp else None}%")
check("monto = cantidad ejecutada x nivel",
      o_tp is not None and abs(o_tp["usd"] - round(o_tp["qty_exec"] * o_tp["price"], 2)) < 0.02,
      o_tp and f"usd={o_tp['usd']} qty={o_tp['qty_exec']} tp={o_tp['price']}")
check("TP1 parcial cierra solo el 50% del monto",
      any(o["type"] == "TP1 parcial" for o in ords) is False or
      abs(next(o for o in ords if o["type"] == "TP1 parcial")["qty_exec"]
          - o_tp["pos_qty"] * simB._st("tp1_size_pct")) < 1e-9)
check("PnL de nivel TP es ganancia vs entrada", o_tp is not None and o_tp["pnl_level"] >= 0)
pf2 = pc["portfolio"]
check("identidad con posicion abierta (margen > 0)",
      abs(pf2["equity"] - (pf2["cash"] + pf2["reserved_margin"] + pf2["unrealized_pnl"])) < 1e-3,
      f"eq={pf2['equity']} margen={pf2['reserved_margin']}")

print("12) Filtros anti-churn / anti-persecucion (afinado de señales)")

# -- 12a: max_ext_atr bloquea entradas en movimientos extendidos --
simE, mE = make_sim(False)
px, candles = 100.0, []                      # serie alcista regular (RSI~-60)
for i in range(40):
    o = px
    px = px + (1.2 if i % 3 == 0 else -0.4)
    candles.append({"time": T0 + i * TF, "open": o, "high": max(o, px) + 0.05,
                    "low": min(o, px) - 0.05, "close": px,
                    "volume": 200 if i % 3 == 0 else 60})
mE.candles = candles
mE.price = px
mE.recompute()
mE.feed = "spot"
check("serie base alcanza LONG sin filtros", mE.evaluate() == "LONG",
      str(mE.diagnostics.get("strength")))
simE.params["max_ext_atr"] = 0.5
sig = mE.evaluate()
check("extensión bloquea LONG (ext_ok False)",
      sig == "NONE" and mE.diagnostics["long"]["ext_ok"] is False,
      f"sig={sig} ext_ratio={mE.diagnostics.get('ext_ratio')} max={mE.diagnostics.get('ext_atr')}")
simE.params["max_ext_atr"] = 4.0
check("extensión relajada deja pasar el LONG", mE.evaluate() == "LONG",
      str(mE.diagnostics.get("ext_ratio")))

# -- 12b: min_body_ratio bloquea velas de mecha (doji-like) --
o = mE.candles[-1]["close"]
mE.candles[-1] = {"time": mE.candles[-1]["time"], "open": o, "high": o + 3,
                  "low": o - 3, "close": o + 0.01, "volume": 200}
mE.price = o + 0.01
mE.recompute()
simE.params["min_body_ratio"] = 0.35
sig = mE.evaluate()
check("vela sin cuerpo bloqueada (body_ok False)",
      sig == "NONE" and mE.diagnostics["long"]["body_ok"] is False,
      f"sig={sig} body_ratio={mE.diagnostics.get('body_ratio')} min={mE.diagnostics.get('body_min')}")
simE.params["min_body_ratio"] = 0.0
check("sin filtro de cuerpo, mismo mercado produce LONG", mE.evaluate() == "LONG",
      str(mE.diagnostics.get("body_ratio")))

# -- 12c: gracia — el rebote contrario no expulsa en las primeras barras --
simG, mG = make_sim(True)
mG.signal = "SHORT"
simG.tick()
check("posición SHORT abierta (12c)", "BTCUSDT" in simG.positions)
simG.params["momentum_grace_bars"] = 2
mG.signal = "LONG"
mG.diagnostics["strength"] = {"long": 1.0, "short": 0.0}
simG.tick()
check("rebote contrario ignorado dentro de la gracia", "BTCUSDT" in simG.positions)
simG.params["momentum_grace_bars"] = 0
simG.tick()
check("sin gracia: sale por momentum contrario", "BTCUSDT" not in simG.positions)
check("motivo correcto", simG.history[-1]["reason"].startswith("Salida por momentum"),
      simG.history[-1]["reason"])

# -- 12d: umbral de fuerza mínima de la señal contraria --
simH, mH = make_sim(True)
mH.signal = "SHORT"
simH.tick()
check("posición SHORT abierta (12d)", "BTCUSDT" in simH.positions)
simH.params["momentum_min_strength"] = 100.0
mH.signal = "LONG"
mH.diagnostics["strength"] = {"long": 0.85, "short": 0.0}
simH.tick()
check("rebote 85% < 100%: posición sobrevive", "BTCUSDT" in simH.positions)
mH.diagnostics["strength"] = {"long": 1.0, "short": 0.0}
simH.tick()
check("rebote 100% >= 100%: sale", "BTCUSDT" not in simH.positions)
simH.cooldown.clear()
simH.cooldown_till.clear()
simH.params["momentum_min_strength"] = 0.0
mH.diagnostics["strength"] = {"long": 0.75, "short": 0.0}
mH.signal = "SHORT"
simH.tick()                                             # re-entra SHORT
mH.signal = "LONG"
simH.tick()
check("umbral neutro conserva la base de 75%", "BTCUSDT" not in simH.positions,
      "razon=" + (simH.history[-1]["reason"] if simH.history else "n/a"))

# -- 12e: cooldown ampliado tras cerrar con pérdida --
simI, mI = make_sim(False)
simI.params["cooldown_loss_mult"] = 2.0
mI.signal = "LONG"
simI.tick()
simI.close("BTCUSDT", mI.price * 0.97, "SL alcanzado")
check("pérdida ensancha el cooldown x2", simI.cooldown.get("BTCUSDT") == 12,
      str(simI.cooldown.get("BTCUSDT")))
check("bloqueo por vela registrado", simI.cooldown_till.get("BTCUSDT", 0) > 0,
      str(simI.cooldown_till.get("BTCUSDT")))
simI.cooldown.clear()
simI.cooldown_till.clear()
simI.params["cooldown_loss_mult"] = 1.0
mI.signal = "LONG"
simI.tick()
simI.close("BTCUSDT", mI.price * 1.02, "TP alcanzado")
check("ganancia mantiene el cooldown base", simI.cooldown.get("BTCUSDT") == 6,
      str(simI.cooldown.get("BTCUSDT")))

# -- 12f: defaults neutros y saneamiento de rangos --
sd = server.STRATEGY_DEFAULTS
check("defaults anti-churn neutros",
      sd["momentum_grace_bars"] == 0 and sd["momentum_min_strength"] == 0
      and sd["cooldown_loss_mult"] == 1.0 and sd["max_ext_atr"] == 0.0
      and sd["min_body_ratio"] == 0.0 and sd["sl_slip_atr"] == 0.0)
ss = server.sanitize_strategy({"momentum_grace_bars": -5, "momentum_min_strength": 400,
                               "cooldown_loss_mult": 0.5, "max_ext_atr": 99, "min_body_ratio": 7,
                               "sl_slip_atr": -1})
check("sanitizado acota los rangos",
      ss["momentum_grace_bars"] == 0 and ss["momentum_min_strength"] == 100.0
      and ss["cooldown_loss_mult"] == 1.0 and ss["max_ext_atr"] == 10.0
      and ss["min_body_ratio"] == 1.0 and ss["sl_slip_atr"] == 0.0)

# -- 12g: relleno del SL con deslizamiento acotado (anti-galope del feed) --
simK, mK = make_sim(False)
mK.signal = "LONG"
simK.tick()
mK.atr = 2.0
sl0 = simK.positions["BTCUSDT"]["sl"]
mK.price = sl0 * 0.90                                  # hueco brutal bajo el stop
simK.params["sl_slip_atr"] = 0.25
simK.manage("BTCUSDT")
trK = simK.history[-1]
check("SL con hueco rellena cerca del stop (cap 0.25 ATR)",
      trK["reason"] == "SL alcanzado" and abs(trK["exit"] - (sl0 - 0.5)) < 1e-6,
      f"exit={trK['exit']} stop={sl0}")
check("pérdida acotada al cap", trK["pnl"] > -5.0 - trK["qty"] * sl0, str(trK["pnl"]))
simK2, mK2 = make_sim(False)
simK2.params["sl_slip_atr"] = 0.0
mK2.signal = "LONG"
simK2.tick()
sl2 = simK2.positions["BTCUSDT"]["sl"]
mK2.price = sl2 * 0.90
simK2.manage("BTCUSDT")
tr2 = simK2.history[-1]
check("con cap 0 el SL rellena exacto al nivel", abs(tr2["exit"] - sl2) < 1e-9,
      f"exit={tr2['exit']} stop={sl2}")

# -- 12h: el cooldown ahora agota en velas, no en ticks --
simL, mL = make_sim(False)
mL.signal = "LONG"
simL.tick()
simL.params["cooldown_bars"] = 2
simL.close("BTCUSDT", mL.price * 1.02, "TP alcanzado")
check("cooldown_till fijado al cerrar", "BTCUSDT" in simL.cooldown_till, str(simL.cooldown_till))
mL.signal = "LONG"
simL.tick()
simL.tick()
check("ticks sin vela nueva: no re-entra", "BTCUSDT" not in simL.positions)
push(mL, mL.price)
mL.signal = "LONG"
simL.tick()
check("1 vela avanzada: sigue bloqueado", "BTCUSDT" not in simL.positions)
push(mL, mL.price)
mL.signal = "LONG"
simL.tick()
check("2 velas avanzadas: cooldown agotado y re-entra", "BTCUSDT" in simL.positions)

print(" 13) Comisiones Binance reales: spot 0.1% / futuros 0.05%, neto al cierre")
# -- 13a: LONG spot: fee 0.1% en ambas piernas, PnL del registro ya neto --
simF, mF = make_sim(False)
simF.params["fees_enabled"] = True            # fee_spot por defecto 0.001
simF.params["fee_maker_spot"] = 0.001         # spot VIP0 base para este test analítico
mF.signal = "LONG"
simF.tick()
posF = simF.positions["BTCUSDT"]
fee_open_esp = float(posF["notional"]) * 0.001
check("fee de apertura anotado (spot 0.1%)", abs(posF["fee_open"] - fee_open_esp) < 1e-9,
      f"fee={posF['fee_open']:.6f} esp={fee_open_esp:.6f}")
check("fee_pendiente inicial = fee de apertura", abs(posF["fee_open_left"] - fee_open_esp) < 1e-9)
check("cash intacto al abrir (fee solo se ANOTA)", abs(simF.cash - 15.0) < 1e-6, f"cash={simF.cash}")
check("equity sin penalizar por fee pendiente", abs(simF.equity()[0] - 20.0) < 0.5,
      f"eq={simF.equity()[0]:.4f}")

entryF = float(posF["entry"])
qtyF = float(posF["qty"])
# TP1 parcial (50%): fee repartido + fee de salida sobre la parte vendida
simF.close("BTCUSDT", entryF * 1.025, "TP1 parcial (+2.5%)", qty=qtyF * 0.5)
partF = [t for t in simF.history if t.get("partial")][-1]
frac_qty = qtyF * 0.5
fee_part_esp = fee_open_esp * 0.5 + (entryF * 1.025) * frac_qty * 0.001
net_part_esp = (entryF * 1.025 - entryF) * frac_qty - fee_part_esp
check("parcial comisionada en su registro", abs(partF["fee"] - round(fee_part_esp, 6)) < 1e-9,
      f"fee={partF['fee']:.6f} esp={fee_part_esp:.6f}")
check("parcial PnL = bruto - fees", abs(partF["pnl"] - round(net_part_esp, 4)) < 1e-9,
      f"pnl={partF['pnl']:.6f} esp={net_part_esp:.6f}")
posF = simF.positions["BTCUSDT"]
check("fee pendiente residual tras parcial", abs(posF["fee_open_left"] - fee_open_esp * 0.5) < 1e-9,
      f"left={posF['fee_open_left']:.6f}")

# cierre total del resto: fee residual + fee de salida completos
exit_px = entryF * 1.01
cash_prev = simF.cash
simF.close("BTCUSDT", exit_px, "TP alcanzado")
totF = simF.history[-1]
qty_rest = float(posF["qty"]) if posF else qtyF * 0.5
fee_tot_esp = fee_open_esp * 0.5 + exit_px * qty_rest * 0.001
net_tot_esp = (exit_px - entryF) * qty_rest - fee_tot_esp
check("cierre total lleva su comisión", abs(totF["fee"] - round(fee_tot_esp, 6)) < 1e-9,
      f"fee={totF['fee']:.6f} esp={fee_tot_esp:.6f}")
check("cierre total PnL neto de fees", abs(totF["pnl"] - round(net_tot_esp, 4)) < 1e-9,
      f"pnl={totF['pnl']:.6f} esp={net_tot_esp:.6f}")
check("fees_paid acumula pierna abierta + parcial +",
      abs(simF.fees_paid - (fee_open_esp + (entryF * 1.025) * frac_qty * 0.001 + exit_px * qty_rest * 0.001)) < 1e-6,
      f"fees={simF.fees_paid:.6f}")
check("cartera = capital + PnL histórico (neto)",
      abs(simF.cash - (20.0 + sum(t["pnl"] for t in simF.history))) < 1e-4,
      f"cash={simF.cash:.4f}")

# -- 13b: SHORT x5 en futuros: tasa 0.05% --
simS5, mS5 = make_sim(True)
simS5.params["fees_enabled"] = True           # fee_futs por defecto 0.0005
mS5.signal = "SHORT"
simS5.tick()
posS = simS5.positions["BTCUSDT"]
check("fee de apertura futuros (0.05%)", abs(posS["fee_open"] - float(posS["notional"]) * 0.0005) < 1e-9,
      f"fee={posS['fee_open']:.6f}")
# -- 13c: liquidación no añade fees (la pérdida ya es el margen) --
fees_antes_liq = simS5.fees_paid
simS5.close("BTCUSDT", posS["liq"], "Liquidación (futuros)")
tq = simS5.history[-1]
check("liquidación: PnL = margen completo", abs(tq["pnl"] - (-float(posS["size_usd"]))) < 1e-6,
      f"pnl={tq['pnl']:.4f}")
check("liquidación no cobra fees extra", abs(tq["fee"]) < 1e-12 and abs(simS5.fees_paid - fees_antes_liq) < 1e-9,
      f"fee={tq['fee']} fees_paid={simS5.fees_paid:.6f}")

# -- 13d: fees_enabled=False -> PnL bruto clásico, coste 0 --
simZ, mZ = make_sim(False)                     # make_sim deja fees off
mZ.signal = "LONG"
simZ.tick()
check("fees off: posición sin coste", abs(simZ.positions["BTCUSDT"]["fee_open"]) < 1e-12)
eZ = float(simZ.positions["BTCUSDT"]["entry"])
simZ.close("BTCUSDT", eZ * 1.02, "TP alcanzado")
tz = simZ.history[-1]
gross_esp = (eZ * 1.02 - eZ) * float(tz["qty"])
check("fees off: PnL = bruto sin fee", abs(tz["pnl"] - gross_esp) < 1e-6,
      f"pnl={tz['pnl']:.6f} esp={gross_esp:.6f}")
check("fees off: registro sin comisión", tz["fee"] == 0.0 and simZ.fees_paid == 0.0,
      f"fee={tz['fee']} paid={simZ.fees_paid}")
# el fee tampoco puede hundir la pérdida más allá del margen
simG, mG = make_sim(False)
simG.params["fees_enabled"] = True
mG.signal = "LONG"
simG.tick()
eG = float(simG.positions["BTCUSDT"]["entry"])
simG.close("BTCUSDT", eG * 0.90, "SL alcanzado")
tg = simG.history[-1]
check("PnL neta nunca supera el margen (fee incluido)", tg["pnl"] >= -5.0 - 1e-9, f"pnl={tg['pnl']:.4f}")

print(" 14) Backtest offline por el mismo circuito + helpers de contexto (sin red)")
# -- 14a: replay con velas armadas; evaluate parcheado a LONG para que sea determinista --
_candles = []
_px = 100.0
_t0 = T0 - TF * 40
for _i in range(280):
    _o = _px
    _fase = (_i // 40) % 2
    _px = _px + (0.6 if _fase == 0 else -0.6)
    _c = _px
    _candles.append({"time": _t0 + _i * TF, "open": _o, "high": max(_o, _c) + 0.2,
                     "low": min(_o, _c) - 0.2, "close": _c,
                     "volume": 100 + (150 if _i % 4 == 0 else 0)})
_bp = {"max_pos": 5.0, "sl_pct": 0.025, "tp_pct": 0.05, "max_open": 1,
       "max_daily_loss": 1e9, "tf_seconds": TF, "ema_fast": 9, "ema_slow": 21,
       "rsi_period": 14, "vol_factor": 1.2}
_bp.update(server.STRATEGY_DEFAULTS)
_bp["fees_enabled"] = True
_orig_ev = server.Market.evaluate
server.Market.evaluate = lambda self: "LONG"
try:
    rbt = server.Backtester.replay(_candles, "BTCUSDT", "5m", "spot",
                                   params=_bp, capital=20.0, label="prueba offline")
finally:
    server.Market.evaluate = _orig_ev
check("backtest offline ok", rbt.get("ok") is True, str(rbt.get("error", "")))
check("velas reprocesadas completas", rbt.get("candles") == 280, str(rbt.get("candles")))
check("etiqueta de origen local", rbt.get("source") == "prueba offline", str(rbt.get("source")))
check("posición forzada cerrada al final", rbt.get("open_positions") == 0, str(rbt.get("open_positions")))
check("hay trades lógicos", rbt.get("trades", 0) >= 1, str(rbt.get("trades")))
check("comisiones cobradas en el backtest", rbt.get("fees", 0.0) > 0.0, f"fees={rbt.get('fees')}")
check("equity final = capital + PnL total", abs(rbt.get("equity_final", 0) - (20.0 + rbt.get("pnl", 0))) < 0.02,
      f"eq={rbt.get('equity_final')} pnl={rbt.get('pnl')}")
check("curva de equity presente", isinstance(rbt.get("equity"), list) and len(rbt["equity"]) > 0)
check("sin límite diario activado (test puro)", rbt.get("daily_stops") == 0, str(rbt.get("daily_stops")))
_bp2 = dict(_bp)
_bp2["fees_enabled"] = False
server.Market.evaluate = lambda self: "LONG"
try:
    rbt2 = server.Backtester.replay(_candles, "BTCUSDT", "5m", "spot",
                                    params=_bp2, capital=20.0, label="prueba offline")
finally:
    server.Market.evaluate = _orig_ev
check("backtest sin fees: coste 0", rbt2.get("fees", -1) == 0.0, str(rbt2.get("fees")))

# -- 14b: helpers de contexto (F&G, noticias, MTF) -- #
check("F&G en español (miedo extremo / neutral / codicia extrema)",
      server.fng_label_es(10) == "Miedo extremo" and server.fng_label_es(50) == "Neutral"
      and server.fng_label_es(90) == "Codicia extrema")
check("noticias positivas -> sentimiento +", server.score_news_titles(["Market RALLy: bitcoin surges to record"]) > 0)
check("noticias negativas -> sentimiento -",
      server.score_news_titles(["Market crash: exchange hack, SEC ban crackdown"]) < 0)
check("noticias neutras -> 0", server.score_news_titles(["A quiet day of consolidation"]) == 0.0)
check("clase de sentimiento", server.sentiment_class(-40) == "Pesimista"
      and server.sentiment_class(40) == "Optimista" and server.sentiment_class(5) == "Neutral")
_up = [{"close": 100.0 + _i * 0.08} for _i in range(60)]
_dn = [{"close": 100.0 - _i * 0.08} for _i in range(60)]
_ft = [{"close": 100.0} for _ in range(60)]
mt_up = server.mtf_trend_from_candles(_up)
mt_dn = server.mtf_trend_from_candles(_dn)
mt_ft = server.mtf_trend_from_candles(_ft)
check("MTF alcista en subida", mt_up["trend"] == "ALCISTA", str(mt_up))
check("MTF bajista en caída", mt_dn["trend"] == "BAJISTA", str(mt_dn))
check("MTF lateral en rango", mt_ft["trend"] == "LATERAL", str(mt_ft))
check("MTF con pocos datos", server.mtf_trend_from_candles([{"close": 1.0}])["trend"] == "SIN DATOS")

# -- 14c: puerta de contexto en tick (params.ctx_filter, extremo F&G) --
simC, mC = make_sim(False)
simC.params["ctx_filter"] = True
simC.context = {"fng": {"value": 90, "label": "Codicia extrema"},
                "mtf": {"BTCUSDT": {"trend": "BAJISTA"}}}
mC.signal = "LONG"
simC.tick()
check("ctx extremo + MTF desalineada: bloquea la entrada", "BTCUSDT" not in simC.positions)
simC.context = {"fng": {"value": 90, "label": "Codicia extrema"},
                "mtf": {"BTCUSDT": {"trend": "ALCISTA"}}}
simC.tick()
check("ctx extremo + MTF alineada: deja entrar", "BTCUSDT" in simC.positions)
simC2 = server.Simulator("c2", "C2", 20.0, ["BTCUSDT"], dict(simC.params))
mC2 = simC2.markets["BTCUSDT"]
simC2.context = {"fng": {"value": 50, "label": "Neutral"},
                 "mtf": {"BTCUSDT": {"trend": "BAJISTA"}}}
mC2.candles = list(mC.candles)
mC2.price = mC.price
mC2.recompute()
mC2.evaluate()
mC2.feed = "spot"
mC2.signal = "LONG"
simC2.tick()
check("F&G neutra: la puerta no interviene", "BTCUSDT" in simC2.positions)

print("=" * 62)
if FAILS:
    print(f" RESULTADO: {len(FAILS)} pruebas FALLARON: {FAILS}")
    raise SystemExit(1)
print(" RESULTADO: todas las pruebas del motor estratégico PASARON.")
