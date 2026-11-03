import json

new_data = {
    "saved_at": "2026-10-29T13:30:00.000000+00:00",
    "active_id": "sim-1",
    "data_source": "spot",
    "sims": {}
}

names = ['BTC Solo', 'ETH Solo', 'Multi BTC/ETH', 'BTC Swing', 'ETH Trend', 'Multi BTC/ETH Swing', 'BTC Scalp', 'ETH Scalp']
pairs_list = [
    ['BTCUSDT'],
    ['ETHUSDT'],
    ['BTCUSDT', 'ETHUSDT'],
    ['BTCUSDT'],
    ['ETHUSDT'],
    ['BTCUSDT', 'ETHUSDT'],
    ['BTCUSDT'],
    ['ETHUSDT']
]

for i in range(8):
    new_data['sims']['sim-' + str(i+1)] = {
        'name': names[i],
        'capital': 200.0,
        'pairs': pairs_list[i],
        'active_pair': pairs_list[i][0],
        'status': 'stopped',
        'started_at': '2026-10-29T13:30:00.000000+00:00',
        'cycle': 0,
        'cash': 200.0,
        'realized_pnl': 0.0,
        'daily': 0.0,
        'fees_paid': 0.0,
        'positions': {},
        'history': []
    }

with open('sim_state.json', 'w') as f:
    json.dump(new_data, f, indent=2)

print('sim_state.json updated successfully')