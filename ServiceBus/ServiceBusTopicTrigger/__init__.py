import logging
import json

import azure.functions as func
from psycopg2.extensions import JSON

from . ConnectionString import *
from . PackageTools import *

def main(message: func.ServiceBusMessage):
    #Log the Service Bus Message as plaintext
    message_body = message.get_body().decode("utf-8")

    #Connect to the database
    conn = getDatabaseConnection()
    cursor = conn.cursor()

    #Get the attributes from the data package
    json_dict = json.loads(message_body)

    packet_info = json_dict["packet_info"]
    samples = json_dict["samples"]
    tstamp = json_dict["packet_info"]["tstamp"]
    id = json_dict["packet_info"]["id"]
    device = json_dict["packet_info"]["device"]
    device = str(device).replace('-', '_')
    buffer_size = json_dict["packet_info"]["buffer_size"]
    sample_rate = json_dict["packet_info"]["sample_rate"]
    sensor_type = json_dict["packet_info"]["sensor_type"]
    content_type = json_dict["packet_info"]["content_type"]
    compressed = json_dict["packet_info"]["compressed"]
    #content = json_dict["packet_info"]["content-type"]
    #data_type = json_dict["packet_info"]["data-type"]
    property_id = 0

    if(device.find('DSR') != -1 or device.find('dsr') != -1):
        #Populate the properties table
        property_query = "INSERT INTO data_properties(device, buffer_size, sample_rate, sensor_type, content_type, compressed, content, data_type) VALUES('%s', %d, %d, '%s', '%s', '%s', '%s', '%s')" % (device, buffer_size, sample_rate, sensor_type, content_type, compressed, content_type, ' ')
        cursor.execute(property_query)

        #Retrieve the property id of the newest entry
        cursor.execute("SELECT MAX(id) FROM data_properties")
        rows = cursor.fetchall()

        for row in rows:
            property_id = row[0]

        #Uncompress the samples
        samples_packed_dict = uncompress_samples(samples)
        sample_list = packed_dict_to_list(samples_packed_dict)
        tstamp_decrypted = decrypt_tstamp(tstamp)

        #Create the new samples table
        tableCreationQuery = "CREATE TABLE IF NOT EXISTS %s(ecg_1 FLOAT, ecg_2 FLOAT, ecg_3 FLOAT, ecg_4 FLOAT, ecg_5 FLOAT, tstamp TIMESTAMP, property_ID INTEGER)" % (device)
        cursor.execute(tableCreationQuery)

        #Populate the samples table
        for sample in sample_list:
            ecg1 = sample.get('ecg1')
            ecg2 = sample.get('ecg2')
            ecg3 = sample.get('ecg3')
            ecg4 = sample.get('ecg4')
            ecg5 = sample.get('ecg5')

            samples_query = "INSERT INTO %s(ecg_1, ecg_2, ecg_3, ecg_4, ecg_5, tstamp, property_ID) VALUES(%f, %f, %f, %f, %f, '%s', %d)" % (device, ecg1, ecg2, ecg3, ecg4, ecg5, tstamp_decrypted, property_id)
            cursor.execute(samples_query)

    #Close the connection
    conn.commit()
    cursor.close()
    conn.close()

    logging.info('Package Received')