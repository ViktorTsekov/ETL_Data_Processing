import pymongo

#Mongo database attributes
cluster = pymongo.MongoClient('mongodb://devtest-db:LKqCyxHxYRKA39Pis7lUGLX0cnUXzMwzXeU8vzzk9aVwf1MF6pXIhx1URdvMJmSc3kZBHJHXRumSjkvjAk77rQ==@devtest-db.mongo.cosmos.azure.com:10255/?ssl=true&replicaSet=globaldb&retrywrites=false&maxIdleTimeMS=120000&appName=@devtest-db@')
db = cluster['dev-kymetric-db']
collection = db['debug_device_ecg-raw']
mongoResults = collection.find({}).limit(10)

#2021-01-26 14:54:57

for result in mongoResults:
    print(result)