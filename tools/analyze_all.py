#!/usr/bin/env python3
"""Analyze all simulation histories"""
import server

m = server.manager
for sid, sim in m.simulators.items():
    print(f"=== {sid}: {sim.name} ===")
    print(f"  Status: {sim.status}")
    print(f"  Cycle: {sim.cycle}")
    print(f"  History trades: {len(sim.history)}")
    for t in sim.history[-3:]:
        print(f'  Trade {t["id"]}: {t["side"]} {t["pair"]} entry=${t["entry"]:.2f} exit=${t["exit"]:.2f} pnl=${t["pnl"]:.2f} ({t["pnl_pct"]:.2f}%) reason={t["reason"]}')
    print()

# Also check market state for sim-1
m = server.manager.simulators['sim-1'].markets['BTCUSDT']
print(f"\n=== Current BTCUSDT Market State ===")
print(f"  Price: {m.price:.2f}")
print(f"  EMA9: {m.indicators.get('ema_fast', 0):.2f}")
print(f"  EMA21: {m.indicators.get('ema_slow', 0):.2f}")
print(f"  RSI: {m.indicators.get('rsi')}")
print(f"  Volume ratio: {m.indicators.get('volume_ratio')}")
print(f"Signal: {m.signal}")
print(f"Signal note: {m.signal_note}")
import json
print(f"Diagnostics: {json.dumps(m.diagnostics, indent=2)}")