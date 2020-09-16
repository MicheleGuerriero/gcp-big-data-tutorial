export PROJECT=$(gcloud info --format='value(config.project)')
export SPARK_PROPERTIES="spark.dynamicAllocation.enabled=false,spark.streaming.receiver.writeAheadLog.enabled=true"

gcloud dataproc jobs submit pyspark compute-kpi-streaming-123.py \
  --id compute-kpi-streaming-123 \
  --cluster=vf-polimi-demo \
  --properties $SPARK_PROPERTIES \
  --region=europe-west1
