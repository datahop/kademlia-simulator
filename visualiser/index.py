from lib2to3.pytree import Node
import matplotlib.pyplot as plt
import pandas as pd

from rich.console import Console

console = Console(record = True)

with console.status("Visualising..."):
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

    message_ids = set(msg_df["id"])

    fig, ax = plt.subplots(figsize=(30, 30))

    max_op_id = op_df.loc[op_df["src"].idxmax()]["id"]
    min_op_id = op_df.loc[op_df["src"].idxmin()]["id"]

    ax.set_ylim(ymin=min_op_id * 0.8, ymax= max_op_id * 1.2)
    for index, row in op_df.iterrows():
        op_id = row["id"]
        op_src = row["src"]

        message_ids = [int(x) for x in row["messages"].split("|")]
        msg_dsts = list((msg_df.loc[ msg_df["id"].isin(message_ids) ])["dst"])

        # print(str(op_src) + ": ", end='')
        # print(msg_dsts)
        
        # ? op_id is the id of the op so that we can plot multiple operations by using the op_id on the y-axis
        # ? op_src is the src node (node all the way on the left)
        # ? msg_dsts is the list of nodes that have received from the op_src (nodes on the line in an ordered fashion)

        min_node = op_src if min(msg_dsts) > op_src else min(msg_dsts)
        max_node = max(msg_dsts)
        node_color = "#c1e7ff"
        node_label_color = "#004c6d"
        dot_size = 2000
        min_val = 0
        max_val = max_node * 1.2
        
        ax.scatter([op_src], [op_id], s=dot_size, color="white", edgecolors=node_label_color)
        ax.annotate(str(op_src)[:5], (op_src, op_id), color=node_label_color, fontweight="bold", fontsize=40)
        
        ax.scatter(msg_dsts, [op_id] * len(msg_dsts), s=dot_size, color="white", edgecolors=node_label_color)
        
        for dst in msg_dsts:
            ax.annotate(str(dst)[:5], (dst, op_id), color=node_label_color, fontweight="bold", fontsize=40)
        
        ax.set_xscale("log")
        ax.set_yticklabels([])
        ax.set_xticklabels([])
        ax.set_yticks([])
        ax.set_xticks([])
        ax.spines.right.set_visible(False)
        ax.spines.left.set_visible(False)
        ax.spines.top.set_visible(False)
        ax.spines.bottom.set_visible(False)
        
        plt.savefig("output.png")