import os
from pymongo import MongoClient
from bson.json_util import dumps
import zipfile
import boto3
from botocore.config import Config
from datetime import datetime

S3_ENDPOINT = os.environ['S3_ENDPOINT']
S3_ACCESS_KEY = os.environ['S3_ACCESS_KEY']
S3_SECRET_KEY = os.environ['S3_SECRET_KEY']
S3_BUCKET = os.environ['S3_BUCKET']
s3_config = Config(
    region_name = 'us-east-1',
    signature_version = 's3v4',
)

def storeFile(filePath, fileName) -> None:
    s3 = boto3.client('s3',
                        endpoint_url=S3_ENDPOINT,
                        aws_access_key_id=S3_ACCESS_KEY,
                        aws_secret_access_key=S3_SECRET_KEY,
                        config=s3_config)
    with open(filePath, 'rb') as f:
        s3.upload_file(filePath, S3_BUCKET, fileName)


def closeJsonFile(file, jsonFile, jsonFileName, territoryId, period):
    file.write(']')
    file.close()
    zipFileName = f"{period}-{territoryId}-tracks.zip"
    zipFile = "./" + zipFileName 
    with zipfile.ZipFile(zipFile, mode='w') as archive:
        archive.write(jsonFile, arcname=jsonFileName, compress_type=zipfile.ZIP_DEFLATED)        
        archive.close()
    storeFile(zipFile, zipFileName)
    os.remove(jsonFile)
    os.remove(zipFile)


def openJsonFile(jsonFile):
    file = open(jsonFile, 'w')
    file.write('[')
    return file


def storeTrackByTerritory(context, collection, territoryId) -> None:
    context.logger.info(f"extraxt tracks for {territoryId}")
    actual_period = None
    file = None
    jsonFileName = None
    jsonFile = None
    tracks = collection.find({"territoryId":territoryId}).sort("startTime", 1)
    for track in tracks:
        period = track["startTime"].strftime("%Y-%m")
        if actual_period == None:
            actual_period = str(period)
            jsonFileName = f"{actual_period}-{territoryId}-tracks.json"
            jsonFile = "./" + jsonFileName
            file = openJsonFile(jsonFile)
            file.write(dumps(track))
        elif actual_period == period:
            file.write(',\n')
            file.write(dumps(track))
        elif actual_period != period:
            closeJsonFile(file, jsonFile, jsonFileName, territoryId, actual_period)
            actual_period = str(period)
            jsonFileName = f"{actual_period}-{territoryId}-tracks.json"
            jsonFile = "./" + jsonFileName
            file = openJsonFile(jsonFile)
            file.write(dumps(track))
    if file:
        closeJsonFile(file, jsonFile, jsonFileName, territoryId, period)     


def handler(context, event):
    try:
        mongoUri = os.environ['MONGO_URI']
        client = MongoClient(mongoUri)
        database = client[os.environ['DB_NAME']]
        collection = database["trackedInstances"]
        storeTrackByTerritory(context, collection, event.path[1:])
        return context.Response(body='OK',
                                headers={},
                                content_type='text/plain',
                                status_code=200)
    except Exception as e:
        context.logger.error(e.message) 
        context.Response(body=e.message, 
                         headers={}, 
                         content_type='text/plain', 
                         staus_code=500)



#test local
#mongoUri = os.environ['MONGO_URI']
#client = MongoClient(mongoUri)
#database = client[os.environ['DB_NAME']]
#collection = database["trackedInstances"]
#storeTrackByTerritory(collection, "TAA")