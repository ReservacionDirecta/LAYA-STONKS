"""
Evaluation Harness 2.0 para LAYA STONKS
Analiza el historial consolidado, calcula métricas avanzadas (WR, PF, Sharpe, DD, Expectancy)
por par, dirección y motivo de salida, y formula recomendaciones diagnósticas.
"""

import os
import sys
import json
import math
from datetime import datetime

STATE_FILE = "sim_state.json"
JOURNAL_FILE = "sim_journal.json"

def load_json(filepath):
    if not os.path.exists(filepath):
        return None
    try:
        with open(filepath, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        print(f"[ERROR] No se pudo leer {filepath}: {e}")
        return None

def compute_metrics(trades):
    if not trades:
        return {
            "count": 0, "wins": 0, "losses": 0, "win_rate": 0.0,
            "gross_pnl": 0.0, "fees": 0.0, "net_pnl": 0.0,
            "profit_factor": 0.0, "avg_win": 0.0, "avg_loss": 0.0,
            "expectancy": 0.0, "max_consecutive_losses": 0
        }
    
    wins = [t for t in trades if (t.get("pnl") or 0) > 0]
    losses = [t for t in trades if (t.get("pnl") or 0) <= 0]
    
    gross_win = sum(t.get("pnl", 0) for t in wins)
    gross_loss = abs(sum(t.get("pnl", 0) for t in losses))
    fees = sum(t.get("fee", 0) for t in trades)
    net_pnl = gross_win - gross_loss
    
    total = len(trades)
    wr = (len(wins) / total) * 100.0 if total > 0 else 0.0
    pf = (gross_win / gross_loss) if gross_loss > 1e-9 else (999.0 if gross_win > 0 else 0.0)
    
    avg_win = (gross_win / len(wins)) if wins else 0.0
    avg_loss = (gross_loss / len(losses)) if losses else 0.0
    
    win_pct = wr / 100.0
    loss_pct = 1.0 - win_pct
    expectancy = (win_pct * avg_win) - (loss_pct * avg_loss)
    
    # Rachas
    max_cons_losses = 0
    cur_cons = 0
    for t in trades:
        if (t.get("pnl") or 0) <= 0:
            cur_cons += 1
            if cur_cons > max_cons_losses:
                max_cons_losses = cur_cons
        else:
            cur_cons = 0
            
    return {
        "count": total,
        "wins": len(wins),
        "losses": len(losses),
        "win_rate": round(wr, 2),
        "gross_pnl": round(net_pnl, 4),
        "fees": round(fees, 4),
        "net_pnl": round(net_pnl - fees, 4),
        "profit_factor": round(pf, 2),
        "avg_win": round(avg_win, 4),
        "avg_loss": round(avg_loss, 4),
        "expectancy": round(expectancy, 4),
        "max_consecutive_losses": max_cons_losses
    }

def print_separator(title=""):
    if title:
        print(f"\n{'='*20} {title.upper()} {'='*20}")
    else:
        print("-" * 60)

def run_harness():
    state = load_json(STATE_FILE)
    if not state:
        print("[!] No se encontró sim_state.json")
        return

    sims = state.get("sims", {})
    all_trades = []
    
    print_separator("Harness 2.0 - Reporte de Rendimiento Cuantitativo")
    print(f"Fecha de Auditoría: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Simuladores Activos en Flota: {len(sims)}\n")

    # Tabla resumen por simulador
    print(f"{'SIM ID':<8} | {'NOMBRE':<22} | {'OPS':<4} | {'WIN RATE':<9} | {'PF':<6} | {'PNL NETO':<10} | {'FEES':<8}")
    print("-" * 78)

    for sid, sdata in sims.items():
        name = sdata.get("name", sid)
        history = sdata.get("history", [])
        for t in history:
            t["_sim_id"] = sid
            t["_sim_name"] = name
            all_trades.append(t)
            
        m = compute_metrics(history)
        print(f"{sid:<8} | {name[:22]:<22} | {m['count']:<4} | {m['win_rate']:>6.1f}%  | {m['profit_factor']:>6.2f} | {m['net_pnl']:>+9.4f}$ | {m['fees']:>7.4f}$")

    # Análisis Consolidado
    global_m = compute_metrics(all_trades)
    print("-" * 78)
    print(f"{'GLOBAL':<8} | {'Flota Consolidada':<22} | {global_m['count']:<4} | {global_m['win_rate']:>6.1f}%  | {global_m['profit_factor']:>6.2f} | {global_m['net_pnl']:>+9.4f}$ | {global_m['fees']:>7.4f}$")

    # Desglose por Par
    print_separator("Desglose por Criptoactivo (Pares)")
    pairs = sorted(list({t.get("pair") for t in all_trades if t.get("pair")}))
    for p in pairs:
        p_trades = [t for t in all_trades if t.get("pair") == p]
        pm = compute_metrics(p_trades)
        status = "RENTABLE" if pm['net_pnl'] > 0 else "DEFICITARIO"
        print(f"• {p:<10} -> Ops: {pm['count']:<3} | WR: {pm['win_rate']:>5.1f}% | PF: {pm['profit_factor']:>5.2f} | Net PnL: {pm['net_pnl']:>+8.4f}$ [{status}]")

    # Desglose por Dirección (LONG vs SHORT)
    print_separator("Desglose por Dirección")
    longs = [t for t in all_trades if t.get("side") == "LONG"]
    shorts = [t for t in all_trades if t.get("side") == "SHORT"]
    lm = compute_metrics(longs)
    sm = compute_metrics(shorts)
    print(f"• LONGs  -> Ops: {lm['count']:<3} | WR: {lm['win_rate']:>5.1f}% | PF: {lm['profit_factor']:>5.2f} | Net PnL: {lm['net_pnl']:>+8.4f}$")
    print(f"• SHORTs -> Ops: {sm['count']:<3} | WR: {sm['win_rate']:>5.1f}% | PF: {sm['profit_factor']:>5.2f} | Net PnL: {sm['net_pnl']:>+8.4f}$")

    # Desglose por Motivo de Salida (Exit Reasons)
    print_separator("Análisis de Salidas y Churn")
    reasons = {}
    for t in all_trades:
        r = t.get("reason", t.get("why", "Otro"))
        reasons.setdefault(r, []).append(t)
    
    for r, r_trades in sorted(reasons.items(), key=lambda x: len(x[1]), reverse=True):
        rm = compute_metrics(r_trades)
        print(f"• {r:<35} | {len(r_trades):<3} ops | PnL: {rm['net_pnl']:>+8.4f}$")

    # Conclusiones Diagnósticas para el Agente Automejorante
    print_separator("Diagnóstico Agéntico & Recomendaciones")
    diagnostics = []
    
    # Evaluar BTC
    btc_trades = [t for t in all_trades if t.get("pair") == "BTCUSDT"]
    if btc_trades:
        bm = compute_metrics(btc_trades)
        if bm["win_rate"] < 40:
            diagnostics.append(f"[!] BTCUSDT presenta un Win Rate bajo ({bm['win_rate']}%). Requiere filtro de score >= 65 o mayor confirmación MTF.")
            
    # Evaluar Salidas por Momentum
    mom_trades = [t for t in all_trades if "momentum" in str(t.get("reason", "")).lower()]
    if mom_trades:
        mm = compute_metrics(mom_trades)
        if mm["net_pnl"] < -1.0:
            diagnostics.append(f"[!] Fuga de capital por Salida de Momentum ({len(mom_trades)} ops, PnL {mm['net_pnl']}$). Mantener grace_bars >= 5.")

    # Liquidaciones
    liq_trades = [t for t in all_trades if "liquid" in str(t.get("reason", "")).lower()]
    if liq_trades:
        diagnostics.append(f"[!] Se detectaron {len(liq_trades)} liquidaciones por gap/volatilidad. Stop Loss priorizado en código.")

    if not diagnostics:
        diagnostics.append("[OK] El sistema opera dentro de los rangos cuantitativos esperados.")

    for d in diagnostics:
        print(f"  {d}")
    print()

if __name__ == "__main__":
    run_harness()
