from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from datetime import datetime
import pytz
from pyspark.sql.types import *

local_tz = pytz.timezone('Europe/Rome')

def utc_to_local(utc_dt):
    local_dt = utc_dt.replace(tzinfo=pytz.utc).astimezone(local_tz)
    return local_tz.normalize(local_dt)

spark = SparkSession \
          .builder \
          .appName('compute-kpi-batch') \
          .config('spark.sql.session.timeZone', 'Europe/Rome') \
          .getOrCreate()

now = utc_to_local(datetime.now())

df = spark.read.load("gs://vf-polimi-batch-data/dpi/year=%d/month=%d" % (now.year, now.month), \
                              format='com.databricks.spark.csv', \
                              header='true', \
                              inferSchema='true')

# number, signature, usage, timestamp

df = df.withColumn('timestamp', f.to_timestamp('timestamp','dd/MM/yyyy-HH:mm:ss'))

df = df.withColumn('hour', f.hour('timestamp'))

df = df\
    .groupBy('number', 'signature', 'day','hour')\
    .sum('usage')\
    .groupBy('number', 'signature', 'hour')\
    .agg(f.mean('sum(usage)'))\
    .withColumnRenamed('avg(sum(usage))','average_usage')

# need to create a dataset first "bq mk vf_polimi_demo_dataset"
df.write.format('bigquery') \
    .option('table', 'vf_polimi_demo_dataset.batch_kpi%d%d' % (now.year, now.month)) \
    .option("temporaryGcsBucket","vf-polimi-batch-data") \
    .mode('overwrite') \
    .save()

# alternative solution to write output on GCS partitioned by date
#df.write.partitionBy('hour').option('header', 'true').mode('overwrite').csv('gs://vf-polimi-batch-data/dpi-kpi/year=%d/month=%d' % (now.year, now.month))

