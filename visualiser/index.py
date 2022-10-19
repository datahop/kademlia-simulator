import pandas as pd

"""
1. Read in contents of log_folder
    1.1. Read conents of msg.csv
    1.2. Read contents of operation.csv
2. For each operation in operation.csv:
    2.1. Get the message id
    2.2. Find all messages with that id
    2.3. Show the original src from the operation and all dst
"""

op_df = pd.read_csv('log_folder/operation.csv')
msg_df = pd.read_csv('log_folder/msg.csv')

for index, row in op_df.iterrows():
    id = row["id"]
    