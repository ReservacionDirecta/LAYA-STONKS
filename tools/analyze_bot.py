#!/usr/bin/env python3
"""Analyze Laya System 1 trading effectiveness"""
import server
import json

m = server.manager
sim1 = m.simulators['sim-1']

print("=== sim-1 (BTC Solo) ===")
print(f"Status: {sim1.status}")
print(f"Cycle: {sim1.cycle}")
print(f"History trades: {len(sim1.history)}")

for t in sim1.history:
    print(f'  Trade {t["id"]}: {t["side"]} {t["pair"]} entry=${t["entry"]:.2f} exit=${t["exit"]:.2f} pnl=${t["pnl"]:.2f} ({t["pnl_pct"]:.2f}%) reason={t["reason"]}')

# Check current market state
m = m.simulators['sim-1'].markets['BTCUSDT']
print(f"\nCurrent market state:")
print(f"  Price: {m.price:.2f}")
print(f"  EMA9: {m.indicators.get('ema_fast', 0):.2f}")
print(f"  EMA21: {m.indicators.get('ema_slow', 0):.2f}")
print(f"  RSI: {m.indicators.get('rsi')}")
print(f"  Volume ratio: {m.indicators.get('volume_ratio')}")
print(f"Signal: {m.signal}")
print(f"Signal note: {m.signal_note}")
print(f"Diagnostics: {json.dumps(m.diagnostics, indent=2)}")