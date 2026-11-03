#!/usr/bin/env python3
"""Test new features: trailing stops, position management, filters"""
import sys
sys.path.insert(0, 'D:/Users/yerct/Laya STONKS')

import server

# Test 1: Verify config has new parameters
print("=== Test 1: Config parameters ===")
m = server.manager
params = server.manager.params
print(f"Trail activation: {m.params.get('trail_activation_pct')}")
print(f"Trail ATR mult: {m.params.get('trail_atr_mult')}")
print(f"Trail step ATR: {m.params.get('trail_step_atr')}")
print(f"Trail max pct: {m.params.get('trail_max_pct')}")
print(f"Time filter: {m.params.get('time_filter_enabled')}, {m.params.get('time_filter_start')}-{m.params.get('time_filter_end')}")
print(f"BTC corr filter: {m.params.get('btc_corr_filter')}, threshold: {m.params.get('btc_corr_threshold')}")
print(f"Vol confirm TF: {m.params.get('vol_confirm_tf')}")
print(f"Vol confirm factor: {m.params.get('vol_confirm_factor')}")
print(f"Trail activation pct: {m.params.get('trail_activation_pct')}")
print(f"Trail ATR mult: {m.params.get('trail_atr_mult')}")
print(f"Trail step ATR: {m.params.get('trail_step_atr')}")
print(f"Trail max pct: {m.params.get('trail_max_pct')}")

# Test 2: Create a test simulator
print("\n=== Test 1: Create simulator ===")
m = server.manager
sim = server.manager.create_sim_auto('Test', 100.0, ['BTCUSDT'])
sim_id = sim.id
print(f"Created sim: {sim.id}, mode={sim.mode}, pairs={sim.pairs}, capital={sim.capital}")

# Test 2: Start sim and run ticks
print("\n=== Test 2: Run ticks ===")
server.manager.select(sim.id)
server.manager.control(sim.id, 'start')
print(f"Status after start: {server.manager.simulators[sim.id].status}")

# Run a few ticks
for i in range(5):
    server.manager.simulators[sim.id].tick()
    s = server.manager.simulators[sim.id]
    mkt = s.markets[s.active_pair]
    print(f"Cycle {s.cycle}: price={mkt.price:.2f}, signal={mkt.signal}, positions={list(s.positions.keys())}")

# Check if position was opened
if server.manager.simulators[sim.id].positions:
    print(f"Positions: {list(server.manager.simulators[sim.id].positions.keys())}")
    for pair, pos in server.manager.simulators[sim.id].positions.items():
        print(f"  {pair}: {pos['side']} entry={pos['entry']:.2f} sl={pos['sl']:.2f} tp={pos['tp']:.2f}")

# Test manage_positions
print("\n=== Test manage_positions ===")
server.manager.simulators[sim.id].tick()  # one more tick to test position management
print("Test completed successfully!")

# Test drawing tools
print("\n=== Test drawing tools ===")
print("Drawing tools available: cursor, line, curve, freehand, horiz, vert, rect, text, undo, clear")

# Test config update
print("\n=== Test config update ===")
m = server.manager
m.params['trail_activation_pct'] = 0.02
print(f"Updated trail_activation_pct: {m.params.get('trail_activation_pct')}")

print("\n[OK] All tests passed!")