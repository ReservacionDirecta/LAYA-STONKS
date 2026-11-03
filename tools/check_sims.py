import json

with open('D:/Users/yerct/Laya STONKS/sim_state.json', 'r') as f:
    data = json.load(f)

sims = data.get('sims', {})
for sid, sim in sims.items():
    print(f'{sid}: name={sim.get("name")}, capital={sim.get("capital")}, pairs={sim.get("pairs")}, status={sim.get("status")}')