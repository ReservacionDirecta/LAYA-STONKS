import urllib.request
import json
import sys
from collections import defaultdict

sys.stdout.reconfigure(encoding='utf-8')

def run_analysis():
    req = urllib.request.Request('http://127.0.0.1:5050/api/state', headers={'User-Agent': 'Mozilla/5.0'})
    with urllib.request.urlopen(req, timeout=5) as r:
        s = json.loads(r.read())

    history = s.get('history', [])
    portfolio = s.get('portfolio', {})
    positions = s.get('positions', [])

    print("=============================================================")
    print("      REPORTE EJECUTIVO: SIMULADOR MULTI-MONEDA (SIM-6)")
    print("=============================================================")
    print(f"Capital Inicial:    ${portfolio.get('initial_capital', 50.0):.2f} USD")
    print(f"Equity Actual:      ${portfolio.get('equity', 0.0):.4f} USD")
    print(f"Efectivo Libre:     ${portfolio.get('cash', 0.0):.4f} USD")
    print(f"Margen en Uso:      ${portfolio.get('reserved_margin', 0.0):.4f} USD")
    print(f"PnL Realizado:      {portfolio.get('realized_pnl', 0.0):+.4f} USD")
    print(f"PnL No Realizado:   {portfolio.get('unrealized_pnl', 0.0):+.4f} USD")
    print(f"PnL Hoy (Realized): {portfolio.get('daily_realized', 0.0):+.4f} USD")
    print(f"Comisiones Pagadas: ${portfolio.get('fees_paid', 0.0):.4f} USD")
    print(f"Total Operaciones:  {len(history)}")
    
    print("\n[1] POSICIONES ACTIVAS EN ESTE MOMENTO:")
    if not positions:
        print("  Sin posiciones abiertas.")
    else:
        for p in positions:
            tp1_txt = "✓ TP1 Cobrado (Break-Even activo)" if p.get('tp1_done') else "Pendiente"
            trail_txt = f"${p.get('trail')}" if p.get('trail') else "Inactivo"
            print(f"  • {p.get('pair'):10s} | {p.get('side'):5s} 1x | Entrada: ${p.get('entry')} | Actual: ${p.get('price')} | PnL: {p.get('pnl'):+.4f} USD | Margen: ${p.get('size_usd')} | TP1: {tp1_txt} | Trailing: {trail_txt}")

    by_pair = defaultdict(lambda: {'count': 0, 'wins': 0, 'pnl': 0.0, 'fees': 0.0, 'reasons': defaultdict(int)})
    by_reason = defaultdict(lambda: {'count': 0, 'pnl': 0.0})
    by_side = defaultdict(lambda: {'count': 0, 'wins': 0, 'pnl': 0.0})

    wins = []
    losses = []

    for t in history:
        p = t.get('pair', 'UNKNOWN')
        pnl = float(t.get('pnl', 0.0) or 0.0)
        fee = float(t.get('fee', 0.0) or 0.0)
        side = t.get('side', 'UNKNOWN')
        reason = t.get('reason', 'UNKNOWN')
        
        by_pair[p]['count'] += 1
        by_pair[p]['pnl'] += pnl
        by_pair[p]['fees'] += fee
        by_pair[p]['reasons'][reason] += 1
        if pnl >= 0:
            by_pair[p]['wins'] += 1
            wins.append(pnl)
        else:
            losses.append(pnl)
            
        by_reason[reason]['count'] += 1
        by_reason[reason]['pnl'] += pnl
        
        by_side[side]['count'] += 1
        by_side[side]['pnl'] += pnl
        if pnl >= 0:
            by_side[side]['wins'] += 1

    print("\n[2] RENDIMIENTO DESGLOSADO POR MONEDA:")
    for p, d in sorted(by_pair.items()):
        c = d['count']
        w = d['wins']
        wr = (w / c * 100) if c > 0 else 0
        print(f"  • {p:10s} | {c:2d} trades | Win Rate: {wr:5.1f}% ({w}/{c}) | PnL Neto: {d['pnl']:+.4f} USD | Comisiones: ${d['fees']:.4f}")

    print("\n[3] DESGLOSE POR MOTIVO DE SALIDA (ESTRATEGIAS LAYA):")
    for r, d in sorted(by_reason.items(), key=lambda x: x[1]['count'], reverse=True):
        print(f"  • {r:32s} | {d['count']:2d} operaciones | PnL Acumulado: {d['pnl']:+.4f} USD")

    print("\n[4] RENDIMIENTO POR DIRECCIÓN (LONG vs SHORT):")
    for side, d in sorted(by_side.items()):
        c = d['count']
        w = d['wins']
        wr = (w / c * 100) if c > 0 else 0
        print(f"  • {side:6s} | {c:2d} trades | Win Rate: {wr:5.1f}% | PnL Neto: {d['pnl']:+.4f} USD")

    gross_win = sum(wins)
    gross_loss = abs(sum(losses))
    pf = (gross_win / gross_loss) if gross_loss > 0 else 999.0
    avg_win = (gross_win / len(wins)) if wins else 0.0
    avg_loss = (gross_loss / len(losses)) if losses else 0.0

    print("\n[5] MÉTRICAS CLAVE DE RIESGO / BENEFICIO:")
    print(f"  • Ganancia Bruta (Wins):    +{gross_win:.4f} USD")
    print(f"  • Pérdida Bruta (Losses):   -{gross_loss:.4f} USD")
    print(f"  • Profit Factor:             {pf:.2f}")
    print(f"  • Ganancia Promedio:        +{avg_win:.4f} USD")
    print(f"  • Pérdida Promedio:         -{avg_loss:.4f} USD")
    print(f"  • Ratio Riesgo/Beneficio:    {(avg_win/avg_loss if avg_loss > 0 else 0):.2f}")
    print(f"  • Máximo Drawdown Simulado:  {(50.0 - min(50.0, portfolio.get('equity', 50.0))) / 50.0 * 100:.2f}%")

if __name__ == '__main__':
    run_analysis()
