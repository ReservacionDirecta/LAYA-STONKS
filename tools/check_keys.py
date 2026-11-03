import json

with open('sim_state.json', 'r') as f:
    data = json.load(f)

for k, v in data['sims'].items():
    print(f"{k}: {list(v.keys())}")