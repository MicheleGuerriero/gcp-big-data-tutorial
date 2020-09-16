export PROJECT=$(gcloud info --format='value(config.project)')
export SERVICE_ACCOUNT_NAME="dataproc-service-account"

gcloud beta dataproc clusters create vf-polimi-demo \
--bucket vf-polimi-batch-data \
--region europe-west1 \
--zone europe-west1-b \
--service-account="$SERVICE_ACCOUNT_NAME@$PROJECT.iam.gserviceaccount.com" \
--subnet projects/$PROJECT/regions/europe-west1/subnetworks/default \
--tags default-allow-internal,default-allow-ssh \
--project $PROJECT \
--master-machine-type n1-standard-2 \
--master-boot-disk-size 60 \
--worker-boot-disk-size 60 \
--num-workers 2 \
--worker-machine-type n1-standard-2 \
--image-version 1.4-debian9 \
--metadata enable-oslogin=true \
--metadata PIP_PACKAGES=google-cloud-pubsub \
--properties core:fs.gs.implicit.dir.repair.enable=false,core:fs.gs.status.parallel.enable=true,core:mapreduce.fileoutputcommitter.marksuccessfuljobs=false,core:spark.pyspark.python=python3,core:spark.pyspark.driver.python=python3 \
--optional-components=ANACONDA,JUPYTER,ZEPPELIN \
--enable-component-gateway \
--max-idle 1h \
--scopes=pubsub,bigquery \
--initialization-actions gs://goog-dataproc-initialization-actions-europe-west1/python/pip-install.sh
