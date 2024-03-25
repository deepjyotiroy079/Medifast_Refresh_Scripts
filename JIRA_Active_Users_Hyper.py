#%%
import requests, json, csv
from requests.auth import HTTPBasicAuth
import tableauserverclient as TSC
import sys
import pandas as pd
from pathlib import Path
import tableauhyperapi
from tableauhyperapi import HyperProcess, Telemetry, \
Connection, CreateMode, \
NOT_NULLABLE, NULLABLE, SqlType, TableDefinition, \
Inserter, \
escape_name, escape_string_literal, \
HyperException

JIRA_ENDPOINT_BASE = "https://medifast.atlassian.net"

headers = {"Accept": "application/json"}
auth = HTTPBasicAuth("paul.dee@medifastinc.com", "xwWnI0reNo5gF1HaWHqNFDAF")

CHUNK_SIZE = 1000

query = {
    "startAt": -CHUNK_SIZE,
    "maxResults": CHUNK_SIZE,
}

users = {}

while True:
    query["startAt"] += CHUNK_SIZE
    req = requests.request(
        "GET",
        f"{JIRA_ENDPOINT_BASE}/rest/api/3/users/search",
        headers=headers,
        auth=auth,
        params=query,
    )
    data = json.loads(req.text)

    if len(data) == 0:
        break  # Last page reached

    for chunk in data:
        aid = chunk["accountId"]
        if aid in users.keys():
            break  # Last page reached
        else:
            users[aid] = chunk

f = open('JiraActiveUsers.csv', 'w', newline='')
writer = csv.writer(f, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
writer.writerow(["Name", "ID", "Email"])

for user in users.values():
    if  user.get("active") == 1: # Active users only
        name = user.get("displayName", None)
        id = user.get("accountId", None)
        email = user.get("emailAddress", None)
        writer.writerow([name, id, email])        

f.close()


token_name = 'python'
token_secret = 'VWMHgJGURtyTrX66wKE9tA==:vBPJBilNXLU8gsdBHJt8ZksiReoiFYEW'

def server_details():
    import tableauserverclient as TSC
    sitename = 'ictableauprod'
    tableau_auth = TSC.PersonalAccessTokenAuth(token_name, token_secret, sitename)
    server = TSC.Server('https://prod-useast-a.online.tableau.com/', use_server_version=True)
    return server, tableau_auth

    
path_to_database = Path("JiraUsers.hyper") #Output name
process_parameters = {
    "log_file_max_count": "2",
    "log_file_size_limit": "100M",
    "log_config": ""
}
with HyperProcess(telemetry=Telemetry.DO_NOT_SEND_USAGE_DATA_TO_TABLEAU, parameters=process_parameters) as hyper:

    connection_parameters = {"lc_time": "en_US"}

    with Connection(endpoint=hyper.endpoint,
                    database=path_to_database,
                    create_mode=CreateMode.CREATE_AND_REPLACE,
                    parameters=connection_parameters) as connection:
        
        jobs_table = TableDefinition(table_name="JiraUsers",
            columns=[
                TableDefinition.Column('Name', SqlType.text() )
                , TableDefinition.Column('ID', SqlType.text() )
                , TableDefinition.Column('Email', SqlType.varchar(max_length=250) )
            ])

        connection.catalog.create_table(table_definition=jobs_table)

        # Using path to current file, create a path that locates CSV file packaged with these examples.
        path_to_csv = "JiraActiveUsers.csv" #input Name
        print('path:', path_to_csv)
        count_in_jobs_table = connection.execute_command(
            command=f"COPY {jobs_table.table_name} from {escape_string_literal(path_to_csv)} with "
            f"(format csv, NULL 'NULL', delimiter ',', header)")

        print(f"The number of rows in table {jobs_table.table_name} is {count_in_jobs_table}.")

    print("The connection to the Hyper file has been closed.")
print("The Hyper process has been shut down.")
   
import tableauserverclient as TSC
server, tableau_auth = server_details()

server.auth.sign_in(tableau_auth)
project_id = 'a37bc4eb-05b4-4b4b-8dba-c72e846782fb' ##IC ADMIN project
new_datasource = TSC.DatasourceItem(project_id)
file_path = 'JiraUsers.hyper' #input name for publishing

with server.auth.sign_in(tableau_auth): 
    new_datasource = server.datasources.publish(new_datasource, file_path, 'Overwrite')
print('JiraUsers.hyper published')