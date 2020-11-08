from google.cloud import pubsub
from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql.types import StructType
from pyspark.sql.types import *
from google.cloud import storage
from pyspark.sql.functions import udf
import hashlib
import sys

PROJECT = sys.argv[1]

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

late_dpis = dpis\
    .withColumn('delay', (f.unix_timestamp(f.current_timestamp()) - f.unix_timestamp(f.col('timestamp'))))

def late_dpi_to_topic(row):
    # Write row to pubsub output topic
    publisher = pubsub.PublisherClient()
    topic_url = 'projects/{project_id}/topics/{topic}'.format(
        project_id='big-data-env',
        topic='late-dpi-topic',
    )
    publisher.publish(topic_url, ','.join([str(row.window.start), str(row.window.end), row.number[-4:], str(row['count'])]).encode('utf-8'))

late_dpi_threshold = 100 # in seconds
late_dpi_window_length = 60 # in seconds
late_dpi_slide_length = 60 # in seconds

late_dpis_console = late_dpis.where('delay > ' + str(late_dpi_threshold))\
    .groupBy(f.window(dpis.timestamp, str(late_dpi_window_length) + " seconds", str(late_dpi_window_length) + " seconds"),
             dpis.number) \
    .count()\
    .writeStream.outputMode("complete").format('console').trigger(processingTime=str(late_dpi_window_length) + " seconds").option('truncate','false').start()

late_dpis_pubsub = late_dpis.where('delay > ' + str(late_dpi_threshold)) \
    .groupBy(f.window(dpis.timestamp, str(late_dpi_window_length) + " seconds", str(late_dpi_window_length) + " seconds"),
             dpis.number) \
    .count() \
    .writeStream.outputMode("complete").trigger(processingTime=str(late_dpi_window_length) + " seconds").foreach(late_dpi_to_topic).start()

####################################################
####################################################


kpis_window_length = 30 # in seconds
kpis_slide_length = 30 # in seconds

kpis = dpis.withWatermark("timestamp", str(late_dpi_threshold) + " seconds") \
    .groupBy(f.window(dpis.timestamp, str(kpis_window_length) + " seconds", str(kpis_slide_length) + " seconds"),
    dpis.number,
    dpis.signature,
    'timestamp')\
    .sum('usage')


# function to write output triggers to PubSub topic
def kpi_to_topic(row):
    # Write row to pubsub output topic
    publisher = pubsub.PublisherClient()
    topic_url = 'projects/{project_id}/topics/{topic}'.format(
        project_id='big-data-env',
        topic='dpi-kpi-topic',
    )
    publisher.publish(topic_url, ','.join([str(row.window.start), str(row.window.end), row.number[-4:], row.signature, str(row['sum(usage)'])]).encode('utf-8'))

# add triggering logic: if higher than threshold trigger a campaign writing a row on bigquery
threshold = 8000

kpis_console = kpis.filter(f.col("sum(usage)") > threshold)\
    .writeStream.format('console').option('truncate','false').start()

kpis_pubsub = kpis.filter(f.col("sum(usage)") > threshold)\
    .writeStream.foreach(kpi_to_topic).start()

late_dpis_console.awaitTermination()
late_dpis_pubsub.awaitTermination()
kpis_console.awaitTermination()
kpis_pubsub.awaitTermination()
