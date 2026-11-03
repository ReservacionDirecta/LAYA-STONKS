import json

with open('sim_state.json', 'r') as f:
    data = json.load(f)

sims = data.get('sims', {})

print("=" * 80)
print("ANÁLISIS DETALLADO DE LAS 8 SIMULACIONES - Laya System 1")
print("=" * 80)

total_pnl = 0
total_trades = 0
total_wins = 0
total_fees = 0
total_gp = 0
total_gl = 0

for sid, s in sims.items():
    name = s.get('name', 'N/A')
    capital = s.get('capital', 0)
    cash = s.get('cash', 0)
    realized = s.get('realized_pnl', 0)
    equity = cash + realized
    history = s.get('history', [])
    trades = len(history)
    wins = sum(1 for t in history if t.get('pnl', 0) > 0)
    pnl = sum(t.get('pnl', 0) for t in history)
    gp = sum(t['pnl'] for t in history if t.get('pnl', 0) > 0)
    gl = abs(sum(t['pnl'] for t in history if t.get('pnl', 0) < 0))
    pf = gp / gl if gl > 0 else float('inf')
    wr = (wins / len(history) * 100) if history else 0
    fees = s.get('fees_paid', 0)
    cycles = s.get('cycle', 0)
    roi = (sum(t.get('pnl', 0) for t in history) / s.get('capital', 1)) * 100 if s.get('capital', 0) else 0
    
    total_gp += sum(t['pnl'] for t in s.get('history', []) if t.get('pnl', 0) > 0)
    total_gl += abs(sum(t['pnl'] for t in history if t.get('pnl', 0) < 0))
    
    print(f"\n--- {name} ({sid}) ---")
    print(f"  Capital: ${s.get('capital', 0):.2f} | Cash: ${s.get('cash', 0):.2f} | Equity: ${s.get('cash', 0) + s.get('realized_pnl', 0):.2f}")
    print(f"  PnL Realizado: ${s.get('realized_pnl', 0):.4f}")
    print(f"  Trades: {len(history)} | Wins: {sum(1 for t in history if t.get('pnl', 0) > 0)}")
    print(f"  Win Rate: {round(wins/len(history)*100, 1) if history else 0}%")
    print(f"  Profit Factor: {round(pf, 2) if pf != float('inf') else 'inf'}")
    print(f"  Fees Pagados: ${s.get('fees_paid', 0):.4f}")
    print(f"  Ciclos: {s.get('cycle', 0)}")
    print(f"  ROI: {roi:.2f}%")

print("\n" + "=" * 80)
print("RESUMEN TOTAL FLOTA")
print("=" * 80)