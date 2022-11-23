from email import message
import pandas as pd
import networkx as nx

from rich.console import Console
from rich.markdown import Markdown
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

num_len = 6
physics = False
image_width = 750
image_height = 750

nt = Network(str(image_width) + 'px', str(image_width) + 'px', notebook=True, directed=True)

message_ids = set(msg_df["id"])

for item in op_df["src"]:
    # ? Add the operation node to the graph  
    nt.add_node(item, label=str(item)[:5], physics=physics, color="green")
    

for index, row in op_df.iterrows():
    op_src = row["src"]
    console.print(Markdown("# " + str(op_src)), style="bold white")
    
    # ? Get the messages from the operation
    messages = [int(x) for x in row["messages"].split("|")]
    
    # ? Get all messages used in the operation
    operated_msg_df = (msg_df.loc[ msg_df["id"].isin(messages) ])
    
    for msg_index, msg_row in operated_msg_df.iterrows():
        msg_src = msg_row["src"]
        msg_dst = msg_row["dst"]
        
        # ? Add the src and dst nodes of the message
        if len(nt.get_node(msg_src)) == 0:  # ? Check if nodes have already been drawn.
            nt.add_node(msg_src, str(msg_src)[:5], physics=physics)
        nt.add_node(msg_dst, str(msg_dst)[:5], physics=physics)
        
        # ? Connect the src and dst nodes of the message
        nt.add_edge(msg_src, msg_dst, title=msg_index, physics=physics)

    # console.print(Markdown("# op_df"), style="bold white")
    # print(op_df)
    # console.print(Markdown("---"), style="bold white")

    # console.print(Markdown("# msg_df"), style="bold white")
    # print(msg_df)
    # console.print(Markdown("---"), style="bold white")

    # console.print(Markdown("# operated_msg_df"), style="bold white")
    # print(operated_msg_df)
    # console.print(Markdown("---"), style="bold white")
        
nt.show("nx.html")