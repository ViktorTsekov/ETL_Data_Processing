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
import mysql.connector

import azure.functions as func

from pymongo import MongoClient
from numpyencoder import NumpyEncoder
from base64 import b64decode, b64encode
from ConnectionString import *

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
    collection = db['imperial_imu-processed']

    #SQL database attributes
    db = getDatabaseConnection()
    cursor = db.cursor()

    count = 0
    propertyID = 0
    entriesLimit = 0

    cursor.execute("SELECT entries_num FROM Settings")
    row = cursor.fetchall()
    
    for x in row:
        entriesLimit = x[0]

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

            propertiesQuery = "INSERT INTO Data_Properties(device, buffer_size, sample_rate, sensor_type, content_type, compressed, content, data_type) VALUES('%s', %d, %d, '%s', '%s', '%s', '%s', '%s')" % (device, bufferSize, sampleRate, sensorType, contentType, compressed, content, dataType)
            cursor.execute(propertiesQuery)

            cursor.execute("SELECT MAX(ID) FROM data_properties")
            row = cursor.fetchall()

            for x in row:
                propertyID = x[0]
        
        samplesPackedDict = uncompress_samples(samples)
        samplesList = packed_dict_to_list(samplesPackedDict)
        
        #Populate the samples table
        for sample in samplesList:
            ax_1 = sample.get('ax1')
            ay_1 = sample.get('ay1')
            az_1 = sample.get('az1')
            gx_1 = sample.get('gx1')
            gy_1 = sample.get('gy1')
            gz_1 = sample.get('gz1')
            mx_1 = sample.get('mx1')
            my_1 = sample.get('my1')
            mz_1 = sample.get('mz1')
            rh_1 = sample.get('rH1')

            samplesQuery = "INSERT INTO IMU_Processed(ax_1, ay_1, az_1, gx_1, gy_1, gz_1, mx_1, my_1, mz_1, rh_1, tstamp, property_ID) VALUES(%f, %f, %f, %f, %f, %f, %f, %f, %f, %d, '%s', %d)" % (ax_1, ay_1, az_1, gx_1, gy_1, gz_1, mx_1, my_1, mz_1, rh_1, tstamp, propertyID)
            cursor.execute(samplesQuery)

    db.commit()

    return "Activity ProcessIMU Finished"