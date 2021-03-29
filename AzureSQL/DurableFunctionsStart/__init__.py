# This function an HTTP starter function for Durable Functions.
# Before running this sample, please:
# - create a Durable orchestration function
# - create a Durable activity function (default name is "Hello")
# - add azure-functions-durable to requirements.txt
# - run pip install -r requirements.txt
 
import logging
import pyodbc

import azure.functions as func
import azure.durable_functions as df

async def main(req: func.HttpRequest, starter: str) -> func.HttpResponse:
    client = df.DurableOrchestrationClient(starter)

    functionName = req.route_params["FunctionName"]
    entriesNum = req.route_params["NumberOfEntries"]

    #SQL database attributes
    server = 'server-kymira.database.windows.net'
    database = 'database-kymira'
    username = 'Viktor'
    password = '121233Vv'   
    driver= '{ODBC Driver 17 for SQL Server}'
    connection = 'DRIVER='+driver+';SERVER='+server+';PORT=1433;DATABASE='+database+';UID='+username+';PWD='+ password

    query = "DELETE FROM Settings; INSERT INTO Settings(activity_name, entries_num) VALUES('%s', %d)" % (functionName, int(entriesNum))
                
    with pyodbc.connect(connection) as conn:
        with conn.cursor() as cursor:
            cursor.execute(query)

    instance_id = await client.start_new(functionName, None, None)

    logging.info(f"Started orchestration with ID = '{instance_id}'.")

    return client.create_check_status_response(req, instance_id)