export PROJECT=$(gcloud info --format='value(config.project)')
export SPARK_PROPERTIES="spark.dynamicAllocation.enabled=false,spark.streaming.receiver.writeAheadLog.enabled=true"

start_date=$(date +%Y%m%d-%H%M%S)

gcloud dataproc jobs submit pyspark ../python/src/main/compute-kpi-batch.py \
  --id compute-kpi-batch-$start_date \
  --cluster=vf-polimi-demo \
  --jars gs://spark-lib/bigquery/spark-bigquery-latest.jar \
  --properties $SPARK_PROPERTIES \
  --region=europe-west1 \
  -- $PROJECT