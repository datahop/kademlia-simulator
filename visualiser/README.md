# Visualiser

This visualiser works by showing the nodes being communicated with during an operation. It shows the nodes involved in the lookup relative to the hash space.

## Dependencies
The code uses the following Python libraries which can be installed using `pip install <library_name>`:

- `matplotlib`
- `pandas`

Make sure that the directory follows this structure:
```
- 📂 log_folder
    - 📃 msg.csv
    - 📃 operations.csv
- 📃 index.py
```

## Usage

To run it just run:
```
python index.py
```

To visualise a specific operation just pass in the operation ID:
```
python index.py <operation_id>
```
Example:
```
python index.py 21
```

## How It Works
1. Read and store the contents of operations.csv.
2. Read and store the contents of msg.csv.
3. Visualise a single or multiple operations.
4. Get the src node of the operation.
5. Get all the message IDs involved in that operation.
6. Get all the message destinations nodes for each message ID.
7. Plot and annotate (label) the operation source node.
8. Plot and annotate (label) all of the message destination nodes.
9. Make the x-axis scale in a log-based manner so that the nodes are nicely presented (otherwise the few nodes with smaller values will get squashed up together).
10. Hide the axes and their information
11. If multiple operations are being visualised then go back to 4.