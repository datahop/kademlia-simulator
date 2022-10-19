from email import message
import pandas as pd
import networkx as nx

from rich.console import Console
from pyvis.network import Network

console = Console(record = True)
# ---------------------------------------------------------------------------- #
#                                 File Reading                                 #
# ---------------------------------------------------------------------------- #
# ? Read contents of operation
op_df = pd.read_csv('log_folder/operations.csv')
# src | id | type | messages
# ? Read contents of msg
msg_df = pd.read_csv('log_folder/msg.csv')
# dst | src | id | type | status

nx_graph = nx.Graph()

num_len = 6
physics = True

message_ids = set(msg_df["id"])

for index, row in op_df.iterrows():
    # ? Get the messages from the operation
    messages = [int(x) for x in row["messages"].split("|")]
    # ? Match the operation message with the messages.csv messages
    operated_messages = list(set(messages) & set(message_ids))
    # ? Add all operation src to graph
    nx_graph.add_node(row["src"], label=str(row["src"])[:5], physics=physics)
    
for index, row in msg_df.iterrows():
    src = row["src"]
    dst = row["dst"]
    id = row["id"]
    for message_id in operated_messages:
        if id == message_id:
            nx_graph.add_node(src, label=str(src)[:5], physics=physics)
            nx_graph.add_node(dst, label=str(dst)[:5], physics=physics)
            nx_graph.add_edge(src, dst)
    
nt = Network('750px', '750px', notebook=True)
nt.from_nx(nx_graph)
nt.show("nx.html")