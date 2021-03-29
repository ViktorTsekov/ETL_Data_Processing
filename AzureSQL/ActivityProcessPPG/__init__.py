# This function is not intended to be invoked directly. Instead it will be
# triggered by an orchestrator function.
# Before running this sample, please:
# - create a Durable orchestration function
# - create a Durable HTTP starter function
# - add azure-functions-durable to requirements.txt
# - run pip install -r requirements.txt

import logging
import json
import zlib
import pyodbc
import pymongo
import numpyencoder

import azure.functions as func

from pymongo import MongoClient
from numpyencoder import NumpyEncoder
from base64 import b64decode, b64encode

def uncompress_samples(b64_str):
    try:
        compressed_str = b64decode(b64_str)
        encoded_str = zlib.decompress(compressed_str)
        packed_dict = json.loads(encoded_str)
    except Exception as e:
        print(e)
    else:
        return packed_dict

def packed_dict_to_list(packed_dict):
    list_samples = []
    list_keys = list(packed_dict.keys())
    if (type(packed_dict[list_keys[0]]) == str):            
        for key in list_keys:
            packed_dict[key] = json.loads(packed_dict[key])
    num_samples = len(list(packed_dict.values())[0])

    for i in range(0,num_samples):
        sample = {}
        for key in list_keys:
            sample[key] = packed_dict[key][i]
        list_samples += [sample]
        
    return list_samples

def main(name: str) -> str:
    #Mongo database attributes
    cluster = MongoClient('mongodb://kymetric-data-db:V1csLEVTjrLcHL90q7kw22BJn35kiJEkLlC8ffBENzv1HZQUwUBzQsRbxLQppzH8LsZzqwizpKA1O4tSprGBHQ==@kymetric-data-db.mongo.cosmos.azure.com:10255/?ssl=true&replicaSet=globaldb&retrywrites=false&maxIdleTimeMS=120000&appName=@kymetric-data-db@')
    db = cluster['kymetric-hub-dd']
    collection = db['imperial_ppg']

    #SQL database attributes
    server = 'server-kymira.database.windows.net'
    database = 'database-kymira'
    username = 'Viktor'
    password = '121233Vv'   
    driver= '{ODBC Driver 17 for SQL Server}'
    connection = 'DRIVER='+driver+';SERVER='+server+';PORT=1433;DATABASE='+database+';UID='+username+';PWD='+ password

    count = 0
    propertyID = 0
    entriesLimit = 0

    with pyodbc.connect(connection) as conn:
        with conn.cursor() as cursor:
            cursor.execute("SELECT entries_num FROM Settings")
            row = cursor.fetchone()
            entriesLimit = row[0]

    mongoResults = collection.find({}).limit(entriesLimit)

    #Read data from mongo database
    for result in mongoResults:
        #Collect all of the fields
        packetInfo = result.get("packet_info")
        samples = result.get("samples")
        
        tstamp = packetInfo.get("tstamp")
        sampleID = packetInfo.get("id")
        device = packetInfo.get("device")
        bufferSize = packetInfo.get("buffer_size")
        sampleRate = packetInfo.get("sample_rate")
        sensorType = packetInfo.get("sensor_type")
        contentType = packetInfo.get("content_type")
        compressed = packetInfo.get("compressed")
        content = packetInfo.get("content-type")
        dataType = packetInfo.get("data-type")
        
        #Populate the data properties table
        if count ==  0:
            count = count + 1
            
            #Find an unique ID for the new row in the table
            with pyodbc.connect(connection) as conn:
                with conn.cursor() as cursor:
                    cursor.execute("SELECT MAX(ID) FROM Data_Properties")
                    row = cursor.fetchone()                
                    propertyID = row[0]

                    if propertyID is None:
                        propertyID = 1
                    else:
                        propertyID = propertyID + 1
            
            propertiesQuery = "INSERT INTO Data_Properties(ID, device, buffer_size, sample_rate, sensor_type, content_type, compressed, content, data_type) VALUES(%d, '%s', %d, %d, '%s', '%s', '%s', '%s', '%s')" % (propertyID, device, bufferSize, sampleRate, sensorType, contentType, compressed, content, dataType)

            with pyodbc.connect(connection) as conn:
                with conn.cursor() as cursor:
                    cursor.execute(propertiesQuery)
        
        samplesPackedDict = uncompress_samples(samples)
        samplesList = packed_dict_to_list(samplesPackedDict)

        #Populate the samples table
        for sample in samplesList:
            gla_2 = sample.get('gla2')
            glb_2 = sample.get('glb2')
            ira_2 = sample.get('ira2')
            irb_2 = sample.get('irb2')
            rla_2 = sample.get('rla2')
            rlb_2 = sample.get('rlb2')

            samplesQuery = "INSERT INTO PPG_Processed(gla_2, glb_2, ira_2, irb_2, rla_2, rlb_2, tstamp, property_ID) VALUES(%f, %f, %f, %f, %f, %f, '%s', %d)" % (gla_2, glb_2, ira_2, irb_2, rla_2, rlb_2, tstamp, propertyID)

            with pyodbc.connect(connection) as conn:
                with conn.cursor() as cursor:
                    cursor.execute(samplesQuery)

    return "Activity ProcessPPG Finished"