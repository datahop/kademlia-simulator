from pprint import pprint
import matplotlib.pyplot as plt
import pandas as pd
import sys

from rich.console import Console
console = Console(record = True)

args = sys.argv[1:]
if len(args) > 1:
    console.print("Too many arguments passed in.", style="bold red")
    sys.exit()
elif len(args) == 1:
    op_id = int(args[0])

def visualise_op(op_id, op_df, msg_df):
    fig, ax = plt.subplots(figsize=(30, 30))
    
    for _, row in op_df.iterrows():
        if int(row["id"]) == int(op_id):
            op_src = row["src"]

    try:
        op_src
    except NameError:
        console.print("Couldn't find an operation with the specified ID: " + str(op_id), style="bold red")
        sys.exit()

    ax.set_ylim(ymin=op_id * 0.8, ymax= op_id * 1.2)

    message_ids = [int(x) for x in row["messages"].split("|")]
    msg_dsts = list((msg_df.loc[ msg_df["id"].isin(message_ids) ])["dst"])

    # ? op_id is the id of the op so that we can plot multiple operations by using the op_id on the y-axis
    # ? op_src is the src node (node all the way on the left)
    # ? msg_dsts is the list of nodes that have received from the op_src (nodes on the line in an ordered fashion)

    node_color = "#c1e7ff"
    node_label_color = "#004c6d"
    dot_size = 2000
    
    ax.scatter([op_src], [op_id], s=dot_size, color=node_color, edgecolors=node_label_color)
    ax.annotate(str(op_src)[:5], (op_src, op_id), color=node_label_color, fontweight="bold", fontsize=40)
    
    ax.scatter(msg_dsts, [op_id] * len(msg_dsts), s=dot_size, color=node_color, edgecolors=node_label_color)
    
    for dst in msg_dsts:
        ax.annotate(str(dst)[:5], (dst, op_id), color=node_label_color, fontweight="bold", fontsize=40)
    
    ax.set_xscale("log")
    ax.axis("off")
    
    plt.savefig("op_" +str(op_id)+ ".png")

def visualise_ops(op_df, msg_df):
    _, ax = plt.subplots(figsize=(30, 30))
    
    max_op_id = op_df.loc[op_df["src"].idxmax()]["id"]
    min_op_id = op_df.loc[op_df["src"].idxmin()]["id"]

    ax.set_ylim(ymin=min_op_id * 0.8, ymax= max_op_id * 1.2)
    for _, row in op_df.iterrows():
        op_id = row["id"]
        op_src = row["src"]

        message_ids = [int(x) for x in row["messages"].split("|")]
        msg_dsts = list((msg_df.loc[ msg_df["id"].isin(message_ids) ])["dst"])

        # ? op_id is the id of the op so that we can plot multiple operations by using the op_id on the y-axis
        # ? op_src is the src node (node all the way on the left)
        # ? msg_dsts is the list of nodes that have received from the op_src (nodes on the line in an ordered fashion)

        node_color = "#c1e7ff"
        node_label_color = "#004c6d"
        dot_size = 2000
        
        ax.scatter([op_src], [op_id], s=dot_size, color=node_color, edgecolors=node_label_color)
        ax.annotate(str(op_src)[:5], (op_src, op_id), color=node_label_color, fontweight="bold", fontsize=40)
        
        ax.scatter(msg_dsts, [op_id] * len(msg_dsts), s=dot_size, color=node_color, edgecolors=node_label_color)
        
        for dst in msg_dsts:
            ax.annotate(str(dst)[:5], (dst, op_id), color=node_label_color, fontweight="bold", fontsize=40)
        
        ax.set_xscale("log")
        ax.axis("off")
        
        plt.savefig("output.png")

with console.status("Visualising..."):
    # ? Read contents of operation
    op_df = pd.read_csv('log_folder/operations.csv')
    # src | id | type | messages
    # ? Read contents of msg
    msg_df = pd.read_csv('log_folder/msg.csv')
    # dst | src | id | type | status

    try:
        op_id
        visualise_op(op_id, op_df, msg_df)
    except NameError:
        visualise_ops(op_df, msg_df)