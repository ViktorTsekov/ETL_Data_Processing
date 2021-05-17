# This function an HTTP starter function for Durable Functions.
# Before running this sample, please:
# - create a Durable orchestration function
# - create a Durable activity function (default name is "Hello")
# - add azure-functions-durable to requirements.txt
# - run pip install -r requirements.txt

import azure.functions as func
import azure.durable_functions as df

from . ConnectionString import *

async def main(req: func.HttpRequest, starter: str) -> func.HttpResponse:
    client = df.DurableOrchestrationClient(starter)

    #Get function parameters
    functionName = req.route_params["FunctionName"]
    startTimestamp = req.route_params["StartTimestamp"]
    endTimestamp = req.route_params["EndTimestamp"]

    #Get connection to the database
    db = getDatabaseConnection()
    cursor = db.cursor()

    #Setup settings
    cursor.execute("DELETE FROM Settings")

    query = "INSERT INTO settings(process_name, start_tstamp, end_tstamp) VALUES('%s', '%s', '%s')" % (functionName, startTimestamp, endTimestamp)            
    cursor.execute(query)
    
    #Close the connection
    db.commit()
    cursor.close()
    db.close()

    instance_id = await client.start_new(functionName, None, None)

    return client.create_check_status_response(req, instance_id)