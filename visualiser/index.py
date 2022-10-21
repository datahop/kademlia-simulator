import matplotlib.pyplot as plt
import pandas as pd

from rich.console import Console

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

message_ids = set(msg_df["id"])

for index, row in op_df.iterrows():
    # ? Get the messages from the operation
    messages = [int(x) for x in row["messages"].split("|")]
    
    # ? Get all messages used in the operation
    operated_msg_df = (msg_df.loc[ msg_df["id"].isin(messages) ])

# ? op_df stores all operation sources.
# ? operated_msg_df stores all messages involved in that operation.
    
fig, axs = plt.subplots(len(op_df), figsize=(30, 30))


biggest_node_id = operated_msg_df["dst"].max() if operated_msg_df["dst"].max() > operated_msg_df["src"].max() else operated_msg_df["src"].max()
smallest_node_id = operated_msg_df["dst"].min() if operated_msg_df["dst"].min() < operated_msg_df["src"].min() else operated_msg_df["src"].min()

node_color = "#c1e7ff"
node_label_color = "#004c6d"
dot_size = 2000
min = 0
max = biggest_node_id * 1.2

for index, row in op_df.iterrows():
    if len(op_df) == 1:
        ax = axs
    else:
        ax = axs[index]

    # ax.axhline(y=index, color=node_label_color)
    ax.hlines(y=index, xmin=smallest_node_id, xmax=biggest_node_id, linewidth=5, color=node_color)
    
    ax.set_xlim(xmin=0, xmax=max)
    ax.set_ylim(ymin=-5, ymax=5)
    
    ax.set_yticklabels([])
    ax.set_xticklabels([])
    ax.set_yticks([])
    ax.set_xticks([])
        
    ax.scatter([int(x) for x in operated_msg_df["src"]], [index] * len(operated_msg_df), s=dot_size, color=node_color)
    ax.scatter([int(x) for x in operated_msg_df["dst"]], [index] * len(operated_msg_df), s=dot_size, color=node_color)
    
    for src in op_df["src"]:
        ax.annotate(str(src)[:5], (src, index), color=node_label_color, fontweight="bold", fontsize=30)
    for src in operated_msg_df["src"]:
        ax.annotate(str(src)[:5], (src, index), color=node_label_color, fontweight="bold", fontsize=30)
    for dst in operated_msg_df["dst"]:
        ax.annotate(str(dst)[:5], (dst, index), color=node_label_color, fontweight="bold", fontsize=30)
            
plt.savefig("output.png")