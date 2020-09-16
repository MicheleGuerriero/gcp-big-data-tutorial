from google.cloud import pubsub
from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from subprocess import PIPE, Popen
from datetime import datetime
from pyspark.sql.types import StructType
from pyspark.sql.types import *
from google.cloud import storage
from pyspark.sql.functions import udf
import hashlib
import sys

PROJECT = sys.argv[1]
TOPIC_NAME = 'dpi-kpi-topic'
TOPIC_NAME_LATE_DPI = 'late-dpi-topic'

# Alternative solution in which data is read directly from PubSub (instead of GCS) and is then uploaded to HDFS in chunks

# create data folder in hdfs
# put = Popen(["hdfs", "dfs", "-mkdir", 'data'], stdin=PIPE, bufsize=-1)
# put.communicate()

# load consents file
client=storage.Client()
bucket=client.get_bucket('vf-polimi-batch-data')
blob=bucket.blob('consents/consents.csv')
file_content = blob.download_as_string().decode('utf-8').split('\n')[1:-1]

consents = dict()

for r in file_content:
    consents[hashlib.md5(r.split(',')[0].encode('utf-8')).hexdigest()] = r.split(',')[1]

# load list of consents in a dictionary from gcs
spark = SparkSession \
    .builder \
    .appName('compute-kpi-streaming') \
    .config('spark.sql.session.timeZone', 'Europe/Rome') \
    .getOrCreate()

# setup and start spark streaming pipeline from hdfs source folder

dpiSchema = StructType([ \
    StructField("number", StringType(), True), \
    StructField("signature", StringType(), True), \
    StructField("usage", DoubleType(), True), \
    StructField("timestamp", TimestampType(), True), \
    StructField("year", IntegerType(), True), \
    StructField("month", IntegerType(), True), \
    StructField("day", IntegerType(), True)
])

dpis = spark \
    .readStream \
    .schema(dpiSchema) \
    .option("timestampFormat","dd/MM/yyyy-HH:mm:ss") \
    .option("maxFilesPerTrigger", 2) \
    .csv("gs://vf-polimi-batch-data/dpi")

def map_consent(mapping):
    def translate_(col):
        return mapping.get(col)
    return udf(translate_, StringType())

# filter based on list of consents
dpis = dpis.withColumn("consent", map_consent(consents)("number")).filter(f.col('consent') == 'yes').drop('consent')

window_length = 20 # in seconds
slide_length = 10 # in seconds

# add filtering of old records
late_dpi_threshold = 5 # in seconds

late_dpis = dpis\
    .withColumn('delay', (f.unix_timestamp(f.current_timestamp()) - f.unix_timestamp(f.col('timestamp'))))

def late_dpi_to_topic(row):
    # Write row to pubsub output topic
    publisher = pubsub.PublisherClient()
    topic_url = 'projects/{project_id}/topics/{topic}'.format(
        project_id=PROJECT,
        topic=TOPIC_NAME_LATE_DPI,
    )
    publisher.publish(topic_url, ','.join([row.number, row.signature, str(row.usage), str(row.timestamp), str(row.delay)]).encode('utf-8'))

late_dpis.writeStream.foreach(late_dpi_to_topic).start().awaitTermination()

kpis = dpis.withWatermark("timestamp", str(late_dpi_threshold) + " seconds") \
    .groupBy(f.window(dpis.timestamp, str(window_length) + " seconds", str(slide_length) + " seconds"),
    dpis.number,
    dpis.signature,
    'timestamp'
).sum('usage')


# function to write output triggers to PubSub topic
def kpi_to_topic(row):
    # Write row to pubsub output topic
    publisher = pubsub.PublisherClient()
    topic_url = 'projects/{project_id}/topics/{topic}'.format(
        project_id=PROJECT,
        topic=TOPIC_NAME,
    )
    publisher.publish(topic_url, ','.join([str(row.window), row.number, row.signature, str(row['sum(usage)'])]).encode('utf-8'))

# add triggering logic: if higher than threshold trigger a campaign writing a row on bigquery
threshold = 10000
kpis.filter(f.col("sum(usage)") > 10000).writeStream.foreach(kpi_to_topic).start().awaitTermination()

# Alternative solution in which data is read directly from PubSub (instead of GCS) and is then uploaded to HDFS in chunks

# start building and sending micro batches to pubsub
# count = 0
# micro_batch_size = 20

# def callback(message):
#     print("Received message: {}".format(message.data.decode('UTF-8')))
#     with open('staging.csv', 'a') as file:
#         file.write(message.data.decode('UTF-8') + '\n')
#     message.ack()
#     global count
#     count += 1
#     if(count >= micro_batch_size):
#         # upload the file to the hdfs source monitored folder
#         count = 0
#         put = Popen(["hdfs", "dfs", "-put", 'staging.csv', 'data/micro-batch-' + datetime.now().strftime("%Y%m%d-%H-%M-%S") + '.csv'], stdin=PIPE, bufsize=-1)
#         put.communicate()
#         # remove staging.csv
#         os.remove('staging.csv')
#
# subscriber = pubsub.SubscriberClient()
# subscription_path = subscriber.subscription_path(PROJECT, 'dpi-subscription')
# streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)
# print("Listening for messages on {}..\n".format(subscription_path))

