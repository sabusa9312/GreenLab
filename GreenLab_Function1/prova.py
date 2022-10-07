import pandas as pd 
from azure.storage.blob import ContainerClient
from io import StringIO
conn_str = "DefaultEndpointsProtocol=https;AccountName=greenlabits;AccountKey=NfnLp43FPKzqkUbUMNqSd46P1P54Aw9qIzccpqdiY037RoB1diFcHblRzI0O8Y0zrjY6Ux1o5DQg+AStGKiP7Q==;EndpointSuffix=core.windows.net"
container = "file"
container_client = ContainerClient.from_connection_string(
    conn_str=conn_str, 
    container_name=container
    )
def cleaning(loss_states):
    loss_states = pd.read_csv(StringIO(downloaded_blob.content_as_text()))
    loss_states.drop(['Domain','Element','Unit'],axis=1,inplace=True)
    loss_states.rename(columns={"Value": "Loss of food by states (tonnes)"},inplace = True)
    loss_states.dropna(inplace=True)
    loss_states['Loss of food by states (tonnes)'] = loss_states['Loss of food by states (tonnes)'].astype(int)
    print(loss_states)
