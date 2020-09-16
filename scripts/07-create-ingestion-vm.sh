export PROJECT=$(gcloud info --format='value(config.project)')
export SERVICE_ACCOUNT_NAME="dataproc-service-account"

gcloud beta compute --project=$PROJECT instances create ingestion-vm \
--zone=europe-west1-b \
--machine-type=n1-standard-2 \
--service-account=$SERVICE_ACCOUNT_NAME@$PROJECT.iam.gserviceaccount.com \
--scopes=https://www.googleapis.com/auth/cloud-platform \
--image=debian-10-buster-v20200910 \
--image-project=debian-cloud \
--boot-disk-size=10GB \
--boot-disk-type=pd-standard \
--boot-disk-device-name=test