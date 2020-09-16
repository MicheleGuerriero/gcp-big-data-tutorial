export PROJECT=$(gcloud info --format='value(config.project)')
export SERVICE_ACCOUNT_NAME="dataproc-service-account"

gcloud beta pubsub subscriptions add-iam-policy-binding \
    dpi-subscription \
    --role roles/pubsub.subscriber \
    --member="serviceAccount:$SERVICE_ACCOUNT_NAME@$PROJECT.iam.gserviceaccount.com"

gcloud beta pubsub topics add-iam-policy-binding \
    dpi-topic \
    --role roles/pubsub.publisher \
    --member="serviceAccount:$SERVICE_ACCOUNT_NAME@$PROJECT.iam.gserviceaccount.com"
