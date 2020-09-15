# reads dpis from pubsub and write the in the historical storage (gcs)
# while writing it anonimyze the records using md5 hashing
import pytz
from google.cloud import storage
from datetime import datetime
from datetime import timedelta
import os
from google.cloud import pubsub
import hashlib

PROJECT = 'big-data-env'
BUCKET = 'vf-polimi-batch-data'
DPI_TOPIC_SUBSCRIPTION = 'dpi-subscription'
client=storage.Client()
bucket=client.get_bucket(BUCKET)

local_tz = pytz.timezone('Europe/Rome')

def utc_to_local(utc_dt):
    local_dt = utc_dt.replace(tzinfo=pytz.utc).astimezone(local_tz)
    return local_tz.normalize(local_dt)

with open('staging.csv', 'a') as file:
    file.write("number,signature,usage,timestamp" + '\n')

start_int = utc_to_local(datetime.now())

def callback(message):
    # performs MD5 hashing to apply some sort of anonymization on the historical data in GCS
    print("Saving to staing file: " + message.data.decode('UTF-8'))
    values = message.data.decode('UTF-8').split(',')
    hashed_number = hashlib.md5(values[0].encode('utf-8')).hexdigest()
    values[0] = hashed_number
    new_message = ','.join(values)
    with open('staging.csv', 'a') as file:
        file.write(new_message + '\n')
    now_dt = utc_to_local(datetime.now())
    global start_int
    if(now_dt > start_int + timedelta(seconds = 30)):
        # upload the file content into gcs in the right partition
        blob=bucket.blob('dpi/' + start_int.strftime("year=%Y/month=%-m/day=%-d/%H-%M-%S") + '.csv')
        blob.upload_from_filename('staging.csv')
        print('Uploaded file at gs://' + BUCKET + '/' + 'dpi/' + start_int.strftime("year=%Y/month=%-m/day=%-d/%H-%M-%S") + '.csv')
        # remove the file
        os.remove('staging.csv')
        # create a new file
        with open('staging.csv', 'a') as file:
            file.write("number,signature,usage,timestamp" + '\n')
        # reset strart_int
        start_int = now_dt


subscriber = pubsub.SubscriberClient()
subscription_path = subscriber.subscription_path(PROJECT, DPI_TOPIC_SUBSCRIPTION)
streaming_pull_future=subscriber.subscribe(subscription_path, callback=callback)
streaming_pull_future.result()
