gcloud pubsub topics create dpi-topic
gcloud pubsub subscription create dpi-subscription --topic=dpi-topic
gcloud pubsub subscription create dpi-ingestion-subscription --topic=dpi-topic

gcloud pubsub topics create dpi-cons-topic
gcloud pubsub subscriptions create dpi-cons-subscription --topic=dpi-cons-topic

gcloud pubsub topics create dpi-kpi-topic
gcloud pubsub subscriptions create dpi-kpi-subscription --topic=dpi-kpi-topic

gcloud pubsub topics create late-dpi-topic
gcloud pubsub subscriptions create late-dpi-subscription --topic=late-dpi-topic
