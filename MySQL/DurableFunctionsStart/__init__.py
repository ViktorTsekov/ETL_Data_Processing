# This function an HTTP starter function for Durable Functions.
# Before running this sample, please:
# - create a Durable orchestration function
# - create a Durable activity function (default name is "Hello")
# - add azure-functions-durable to requirements.txt
# - run pip install -r requirements.txt
 
import logging
import pyodbc
import mysql.connector

import azure.functions as func
import azure.durable_functions as df

from ConnectionString import *

async def main(req: func.HttpRequest, starter: str) -> func.HttpResponse:
    client = df.DurableOrchestrationClient(starter)

    functionName = req.route_params["FunctionName"]
    entriesNum = req.route_params["NumberOfEntries"]

    db = getDatabaseConnection()
    cursor = db.cursor()

    cursor.execute("DELETE FROM Settings")

    query = "INSERT INTO Settings(activity_name, entries_num) VALUES('%s', %d)" % (functionName, int(entriesNum))            
    cursor.execute(query)
    db.commit()

    instance_id = await client.start_new(functionName, None, None)

    logging.info(f"Started orchestration with ID = '{instance_id}'.")

    return client.create_check_status_response(req, instance_id)