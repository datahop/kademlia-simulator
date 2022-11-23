from dash import dcc, Input, Output, callback, html, dash_table
from plotly.subplots import make_subplots

import dash
import dash_bootstrap_components as dbc
import pandas as pd
import plotly.graph_objects as go

op_df = pd.read_csv("log_folder/operations.csv")

app = dash.Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP])

app.layout = html.Div([
    html.H1(
        "Kademlia Visualiser",
        className="display-1", 
        style={
            "textAlign": "center",
            "marginTop": "3vh"
        }
    ),
    dbc.Container(
        dbc.Row([
            html.P(
                "Click on an operation in the table on the left to view it on the graph on the right.",
                className="lead",
                style={"textAlign": "center"}
            ),
            dbc.Col([
                dash_table.DataTable(
                    op_df.to_dict("records"), 
                    [{"name": i, "id": i} for i in op_df.columns],
                    id="table",
                    style_cell={"textAlign": "center"}
                ),
            ], style={"textAlign": "center"}),
            dbc.Col(
                dcc.Graph(
                    id="graph", 
                    style={
                        "height": "70vh"
                    }
                ),
            )
        ]),
    style={"padding": "5vh 3vw 0 3vw"},
    fluid=True
    )
])

@callback(Output("graph", "figure"), Input("table", "active_cell"))
def update_graphs(active_cell):
    msg_df = pd.read_csv("log_folder/msg.csv")
    
    if active_cell:
        op_id = active_cell["row_id"]
        
        for _, row in op_df.iterrows():
            if int(row["id"]) == int(op_id):
                op = row

        message_ids = [int(x) for x in op["messages"].split("|")]
        msg_dsts = list((msg_df.loc[ msg_df["id"].isin(message_ids) ])["dst"])
        
        # ? op_id is the id of the op so that we can plot multiple operations by using the op_id on the y-axis
        # ? op_src is the src node (node all the way on the left)
        # ? msg_dsts is the list of nodes that have received from the op_src (nodes on the line in an ordered fashion)

        x = [op["src"]] + msg_dsts
        y = [op_id] * len(msg_dsts) + [op_id]

        fig = go.Figure(
            go.Scatter(
                x = x, 
                y = y, 
                marker=dict(size = 50)
            )
        )
        
        fig.update_xaxes(type="log")
        fig.update_layout(title="Operation " + str(op_id))

        return fig
    else:
        max_op_id = op_df.loc[op_df["src"].idxmax()]["id"] * 1.01
        min_op_id = op_df.loc[op_df["src"].idxmin()]["id"] * 0.99
        
        fig = make_subplots(rows=1, cols=1)
        
        for _, row in op_df.iterrows():
            message_ids = [int(x) for x in row["messages"].split("|")]
            msg_dsts = list((msg_df.loc[ msg_df["id"].isin(message_ids) ])["dst"])
            
            # ? op_id is the id of the op so that we can plot multiple operations by using the op_id on the y-axis
            # ? op_src is the src node (node all the way on the left)
            # ? msg_dsts is the list of nodes that have received from the op_src (nodes on the line in an ordered fashion)

            x = [row["src"]] + msg_dsts
            y = [row["id"]] * len(x)

            fig.add_trace(
                go.Scatter(
                    x = x, 
                    y = y, 
                    marker=dict(size = 50)
                )
            )
            
            fig.update_xaxes(type="log")
            fig.update_layout(
                yaxis_range=[min_op_id, max_op_id],
                showlegend=False,
                margin=dict(t=0, r=0)
            )

        return fig

if __name__ == "__main__":
    app.run_server(debug=True)