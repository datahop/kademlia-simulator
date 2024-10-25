import json
import random

# Function to generate latency data
def generate_latency_data(num_nodes: int, latency_range: tuple) -> list:
    data_list = [[ 0 for _ in range(num_nodes) ] for _ in range(num_nodes) ]
    data = []
    for x in range(num_nodes):
        for y in range(x, num_nodes):
            l = random.randint(latency_range[0], latency_range[1])
            data_list[x][y] = l
            data_list[y][x] = l
        data_list[x][x] = 5
        # Generate a random latency list for each node, simulating latency between nodes
    for i in range(len(data_list)):
        data.append({
            "node": i,
            "latency": data_list[i]
        })
    return data

num_nodes = 3000 # Example number of nodes
latency_range = (20, 130)  # Example range for latency values
# Generate the data
latency_data = generate_latency_data(num_nodes, latency_range)

# Specify the output file path
output_file = "latency_data.json"

# Write the generated JSON data to a file
with open(output_file, "w") as file:
    json.dump(latency_data, file, indent=4)

print(f"JSON data has been written to {output_file}")
