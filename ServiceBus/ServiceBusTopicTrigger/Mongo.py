import pymongo
import time

#Mongo database attributes
cluster = pymongo.MongoClient('mongodb://kymetric-data-db:V1csLEVTjrLcHL90q7kw22BJn35kiJEkLlC8ffBENzv1HZQUwUBzQsRbxLQppzH8LsZzqwizpKA1O4tSprGBHQ==@kymetric-data-db.mongo.cosmos.azure.com:10255/?ssl=true&replicaSet=globaldb&retrywrites=false&maxIdleTimeMS=120000&appName=@kymetric-data-db@')
db = cluster['kymetric-hub-dd']
collection = db['tim_6-lead-ecg_ecg-raw']
mongoResults = collection.find({}).limit(10)

#2021-01-26 14:54:57

for result in mongoResults:
    print(result)