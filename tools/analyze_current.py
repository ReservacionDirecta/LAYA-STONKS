import json

with open('D:/Users/yerct/Laya STONKS/sim_state.json') as f:
    data = json.load(f)

sims = data.get('sims', {})
print('=== RESUMEN FLOTA (Paper Trading Sintético) ===')
print()

total_trades = 0
total_wins = 0
total_realized = 0
total_fees = 0

for sid, sim in sims.items():
    name = sim.get('name')
    capital = sim.get('capital')
    realized = sim.get('realized_pnl')
    fees = sim.get('fees_paid')
    trades = len(sim.get('history', []))
    wins = sum(1 for t in sim.get('history', []) if t.get('pnl', 0) > 0)
    gross_profit = sum(t.get('pnl', 0) for t in sim.get('history', []) if t.get('pnl', 0) > 0)
    gross_loss = abs(sum(t.get('pnl', 0) for t in sim.get('history', []) if t.get('pnl', 0) < 0))
    wr = (wins/trades*100) if trades else 0
    pf = gross_profit/gross_loss if gross_loss else float('inf')
    roi = (realized/capital*100) if capital else 0
    status = sim.get('status')
    pairs = sim.get('pairs')
    total_trades += trades
    total_wins += wins
    total_realized += realized
    total_fees += fees
    print(f'{sid:6} | {name:20} | ${capital:>6.0f} | {str(pairs):25} | {status:8} | Trades:{trades:3} | WR:{wr:5.1f}% | PF:{pf:5.2f} | ROI:{roi:6.2f}% | Net:${realized:>6.2f} | Fees:${fees:.2f}')

print()
print(f'TOTALES: Trades={total_trades} | Wins={total_wins} | WR={total_wins/total_trades*100:.1f}% | Net PnL=${total_realized:.2f} | Fees=${total_fees:.2f}')