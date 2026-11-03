#!/usr/bin/env python3
"""Analyze Laya System 1 trading effectiveness from saved state"""
import json
import os

with open('sim_state.json', 'r') as f:
    data = json.load(f)

sims = data['sims']

print("=" * 60)
print("LAYA SYSTEM 1 - HISTORICAL PERFORMANCE ANALYSIS")
print("=" * 60)

total_trades = 0
total_wins = 0
total_losses = 0
total_pnl = 0.0

for sim_id, sim in data['sims'].items():
    print(f"\n{'='*60}")
    print(f"SIMULADOR: {sim['name']} ({sim_id})")
    print(f"  Modo: {sim.get('mode', 'single')} | Pares: {sim['pairs']}")
    print(f"  Estado: {sim['status']} | Ciclos: {sim['cycle']}")
    print(f"  Capital inicial: ${sim['capital']:.2f} | Efectivo: ${sim['cash']:.2f}")
    print(f"  PnL realizado: ${sim['realized_pnl']:.4f} | Diario: ${sim['daily']:.2f}")
    
    history = sim.get('history', [])
    trades_count = len(history)
    wins = 0
    losses = 0
    pnl_total = 0.0
    
    if history:
        print(f"  Trades: {len(history)}")
        for t in history:
            result = "WIN" if t['pnl'] > 0 else "LOSS"
            print(f"  Trade {t['id']}: {t['side']} {t['pair']} | Entrada: ${t['entry']:.2f} -> Salida: ${t['exit']:.2f} | PnL: ${t['pnl']:.2f} ({t['pnl_pct']:.2f}%) | {t['reason']} | {result}")
            if t['pnl'] > 0:
                wins += 1
            else:
                losses += 1
            pnl_total += t['pnl']
        total_trades += trades_count
        total_wins += wins
        total_losses += losses
        total_pnl += pnl_total
        
        if trades_count > 0:
            wr = (wins / trades_count) * 100
            print(f"  Win Rate: {wr:.1f}% | PnL Total: ${pnl_total:.2f}")
    else:
        print("  Sin trades registrados")

# Global summary
print("\n" + "=" * 60)
print("RESUMEN GLOBAL")
print("=" * 60)
print(f"Total trades: {total_trades}")
print(f"Victorias: {total_wins} | Derrotas: {total_losses}")
if total_trades > 0:
    wr = (total_wins / total_trades) * 100
    print(f"Win Rate global: {wr:.1f}%")
    print(f"PnL total acumulado: ${total_pnl:.4f}")
    
    # Average trade
    wins_list = [t['pnl'] for s in data['sims'].values() for t in s.get('history', []) if t['pnl'] > 0]
    losses_list = [t['pnl'] for s in data['sims'].values() for t in s.get('history', []) if t['pnl'] < 0]
    avg_win = sum(wins_list) / max(1, len(wins_list)) if wins_list else 0
    avg_loss = abs(sum(losses_list)) / max(1, len(losses_list)) if losses_list else 0
    print(f"Promedio ganancia: ${avg_win:.2f} | Promedio pérdida: ${abs(avg_loss):.2f}")

# Analyze signal quality
print("\n" + "=" * 60)
print("ANÁLISIS DE SEÑALES (Laya System 1)")
print("=" * 60)
print("Sistema: EMA 9/21 + RSI(14) + Volumen > 1.2x + vela alcista/bajista")
print("Stop Loss: 2.5% | Take Profit: 5% (Ratio 1:2)")
print("Máx 1 posición abierta | Máx $5 por trade | Stop diario $2")

print("\nCONCLUSIONES:")
print("1. Sistema usa estrategia de seguimiento de tendencia con filtro de momentum")
print("2. Ratio riesgo/recompensa 1:2 protege capital")
print("3. Filtro de volumen evita entradas en mercados ilíquidos")
print("4. RSI 50-70 evita sobrecompra; RSI 30-50 para shorts")
print("5. Stop Loss ajustado (2.5%) limita riesgo por trade a $0.125 ($5 * 2.5%)")

print("\nRECOMENDACIONES DE MEJORA:")
print("1. Agregar trailing stop para proteger ganancias en trends fuertes")
print("2. Implementar filtro de horario (evitar trading nocturno/asiático)")
print("3. Agregar filtro de correlación BTC (evitar trades contra tendencia BTC)")
print("4. Considerar reducción de tamaño en racha de pérdidas (martingale inverso)")
print("5. Añadir confirmación de volumen en timeframe superior (1h/4h)")