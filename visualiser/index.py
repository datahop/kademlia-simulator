import pandas as pd
import networkx as nx

from rich.console import Console
from pyvis.network import Network

console = Console(record = True)
# ---------------------------------------------------------------------------- #
#                                 File Reading                                 #
# ---------------------------------------------------------------------------- #
# ? Read contents of operation
op_df = pd.read_csv('log_folder/operation.csv')
# src | id | type | messages
# ? Read contents of msg
msg_df = pd.read_csv('log_folder/msg.csv')
# dst | src | id | type | status

nx_graph = nx.Graph()

num_len = 6
physics = True

for index, row in msg_df.iterrows():
    id = str(row["id"])[:num_len]
    src = str(row["src"])[:num_len]
    dst = str(row["dst"])[:num_len]
    
    nx_graph.add_node(id, label=id, physics=physics)
    nx_graph.add_edge(src, dst, physics=physics)
    
for index, row in op_df.iterrows():
    id = str(row["id"])[:num_len]
    nx_graph.add_node(id, label=id, color="green", physics=physics)
    
    
nt = Network('720px', '1280px', notebook=True)
nt.from_nx(nx_graph)
nt.show("nx.html")