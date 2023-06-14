import datetime
import logging
import azure.functions as func
from pymongo import MongoClient
import os
import datetime
import pandas as pd
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient, ContentSettings

app = func.FunctionApp()

@app.schedule(schedule="0 0 8 * * *", arg_name="myTimer", run_on_startup=True,
              use_monitor=False) 
def MongoSensorDataExporter(myTimer: func.TimerRequest) -> None:
    utc_timestamp = datetime.datetime.utcnow().replace(
        tzinfo=datetime.timezone.utc).isoformat()

    # Connect to mongo db and download latest sensor readings.
    mongoUserPw = os.environ.get('SENSOR_MONGODB_PW')

    client = MongoClient(f'mongodb+srv://mongouser:{mongoUserPw}@cluster.gbiuwau.mongodb.net/?retryWrites=true&w=majority')
    db = client['test']
    collection = db['sensor_readings_timeseries']

    now = datetime.datetime.now()
    entries = collection.find({'Timestamp': {'$lt': now}})

    df = pd.DataFrame(list(entries))
    csv_data = df.to_csv(index=False)

    # Clean up mongoDb readings
    collection.delete_many({})

    # Upload to blob storage
    filename = f'sensor_readings_{now.day}_{now.month}_{now.year}.csv'

    connection_str =  os.environ.get('AzureWebJobsStorage')
    blob_service_client = BlobServiceClient.from_connection_string(conn_str=connection_str)

    container_name = 'sensor-data'
    container_client = blob_service_client.get_container_client(container=container_name)
    container_client.upload_blob(name=filename, data=csv_data, content_settings=ContentSettings(content_type='text/csv'), overwrite=True)

    logging.info('Mongo exporter timer trigger function ran at %s', utc_timestamp)