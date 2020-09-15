export PROJECT=$(gcloud info --format='value(config.project)')
export SPARK_PROPERTIES="spark.dynamicAllocation.enabled=false,spark.streaming.receiver.writeAheadLog.enabled=true"

gcloud dataproc jobs submit pyspark compute-kpi-batch.py \
  --id compute-kpi-batch \
  --cluster=vf-polimi-demo \
  --jars gs://spark-lib/bigquery/spark-bigquery-latest.jar \
  --properties $SPARK_PROPERTIES \
  --region=europe-west1
