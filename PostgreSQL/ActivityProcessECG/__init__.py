# This function is not intended to be invoked directly. Instead it will be
# triggered by an orchestrator function.
# Before running this sample, please:
# - create a Durable orchestration function
# - create a Durable HTTP starter function
# - add azure-functions-durable to requirements.txt
# - run pip install -r requirements.txt

from pymongo import MongoClient
from datetime import datetime
from base64 import b64decode, b64encode

from . ConnectionString import *
from . PackageTools import *

def main(name: str) -> str:
    #Mongo database attributes
    cluster = MongoClient('mongodb://devtest-db:LKqCyxHxYRKA39Pis7lUGLX0cnUXzMwzXeU8vzzk9aVwf1MF6pXIhx1URdvMJmSc3kZBHJHXRumSjkvjAk77rQ==@devtest-db.mongo.cosmos.azure.com:10255/?ssl=true&replicaSet=globaldb&retrywrites=false&maxIdleTimeMS=120000&appName=@devtest-db@')
    db = cluster['dev-kymetric-db']
    collection = db['debug_device_ecg-raw']

    #SQL database attributes
    db = getDatabaseConnection()
    cursor = db.cursor()

    count = 0
    propertyID = 0
    startTimestamp = ''
    endTimestamp = ''

    #Get the specified settings
    cursor.execute("SELECT * FROM settings")
    row = cursor.fetchall()
    
    for x in row:
        startTimestamp = x[1]
        endTimestamp = x[2]

    mongoResults = collection.find({})
    
    #Read data from mongo database
    for result in mongoResults:
        #Collect all of the fields from the package
        packetInfo = result.get("packet_info")
        samples = result.get("samples")
        
        id = packetInfo.get("id")
        tstamp = packetInfo.get("tstamp")
        tstampDecrypted = decrypt_tstamp(tstamp)
        device = packetInfo.get("device")
        device = str(device).replace('-', '_')
        bufferSize = packetInfo.get("buffer_size")
        sampleRate = packetInfo.get("sample_rate")
        sensorType = packetInfo.get("sensor_type")
        contentType = packetInfo.get("content_type")
        compressed = packetInfo.get("compressed")
        #content = packetInfo.get("content-type")
        #dataType = packetInfo.get("data-type")
        
        #Populate the data properties table
        if count ==  0:
            count = count + 1

            propertiesQuery = "INSERT INTO data_properties(device, buffer_size, sample_rate, sensor_type, content_type, compressed, content, data_type) VALUES('%s', %d, %d, '%s', '%s', '%s', '%s', '%s')" % (device, bufferSize, sampleRate, sensorType, contentType, compressed, contentType, ' ')
            cursor.execute(propertiesQuery)

            cursor.execute("SELECT MAX(ID) FROM data_properties")
            row = cursor.fetchall()

            for x in row:
                propertyID = x[0]
        
        tstampObject = datetime.strptime(tstampDecrypted, '%Y-%m-%d %H:%M:%S')

        #Check if the current entry is in the specified time range
        if(tstampObject >= startTimestamp and tstampObject <= endTimestamp):
            samplesPackedDict = uncompress_samples(samples)
            samplesList = packed_dict_to_list(samplesPackedDict)

            #Create the new samples table
            tableCreationQuery = "CREATE TABLE IF NOT EXISTS %s(id VARCHAR(255), ecg_1 FLOAT, ecg_2 FLOAT, ecg_3 FLOAT, ecg_4 FLOAT, ecg_5 FLOAT, tstamp TIMESTAMP, property_ID INTEGER)" % (device)
            cursor.execute(tableCreationQuery)
            
            #Populate the samples table
            for sample in samplesList:
                ecg1 = sample.get('ecg1')
                ecg2 = sample.get('ecg2')
                ecg3 = sample.get('ecg3')
                ecg4 = sample.get('ecg4')
                ecg5 = sample.get('ecg5')

                count = count + 1

                samplesQuery = "INSERT INTO %s(id, ecg_1, ecg_2, ecg_3, ecg_4, ecg_5, tstamp, property_ID) VALUES('%s', %f, %f, %f, %f, %f, '%s', %d)" % (device, id, ecg1, ecg2, ecg3, ecg4, ecg5, tstampDecrypted, propertyID)
                cursor.execute(samplesQuery)

    #Close the connection
    db.commit()
    cursor.close()
    db.close()
    
    message = "Activity ProcessECG Finished"

    return message