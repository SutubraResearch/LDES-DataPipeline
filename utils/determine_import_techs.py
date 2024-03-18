
def
result_dict = {}
for eff in efficiencies:
    region, input_comm = eff[0], eff[1]
    if input_comm in fuels:
        if region not in result_dict:
            result_dict[region] = []
        result_dict[region].append(input_comm)
    else:
        print(f"Warning: {input_comm} not found in fuels.toml")