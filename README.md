##### Project creation and the Google Cloud shell

##### Setup the required topics and subscriptions for real time data processing. 
There will be 3 topics:

* dpi-topic: for storing raw dpi records
* dpi-kpi-topic: for storing real time KPIs based on DPIs
* late-dpi-topic: for storing raw dpi records that arrive late

and there will be 3 subscriptions:

* dpi-subscription: used by the streaming application to consumes messages from dpi-topic
* dpi-kpi-subscription: used to consume directly from the web UI or from any other client the real time KPIs produced by the streaming application
* late-dpi-subscription: used to consume from the web UI or any other client the dpi records that arrive too late

You can configure topics and subscriptions by running the setup-pubsub.sh script from the cloud shell.

##### Create bucket for storing historical data

Historical data are stored in Google Cloud Storage in the form of csv files, partitioned by date of arrival.
The only required setup is the creation of the necessary bucket, that can be achieved by running the create-gcs-bucket.sh script from the cloud shell.

##### Create big query dataset for storing analytical outputs of batch processing

The example Spark application will run periodically on the historical data to compute and update a relevant KPI.
This KPI will be stored for further processing and analysis into Google BigQuery. To this aim, the only necessary setup is 
the creation of a Big Query dataset, which can ne achieved by running the create-bq-dataset.sh script from the cloud shell.

##### Create a service account and gve the required permissions

We now need to create a Google Service Account.
This is effectively an account that we will use, in place of our own personal account,
to provision resources and run applications on GCP. 
In order to create the service account we can run the create-service-account.sh script,
which will create a service account named dataproc-service-account.
Then, it is necessary to grant this service account privileges in order
to operate with GCP resources. In particular, we need the service account
to be able to work with Dataproc, BigQuery and PubSub, specifically with the 
resources we have just created. In order to achieve this, we can run the
add-dataproc-worker-role-to-sa.sh and the add-pubsub-role-to-sa.sh scripts,
which contain the gcloud commands that are necessary to configure all the 
required roles and permissions.

##### Upload the consents file into the gcs bucket

According to many privacy regulations, a privacy preserving mechanism that is often used in practice is consent,
meaning that a customer's data can be processed only if the customer has provided an explicit consent.
To reproduce this scenario in our example, the DPIs processed by the streaming application must be filtered based on customers consent.
We simulate a one-shot uploading of raw data about customers consent; this data will be then used by the streaming application.
The provided file consents.csv contains the information about the consent given by each customer (for the example customers of our example).
The only necessary setup is the upload of the consents.csv file into an appropriate folder under the GCS bucket.
This can be achieved by running the ingest_consents.sh script from the cloud shell.

##### Create VM for running dpi generator and ingestion process of historical data

As we cannot use real DPI data coming from customer phones in this example, we will simulate the generation
of DPIs and their arrival on the configured PubSub topic (dpi-topic). In order to do so, we need to execute 
the dpi-generator.py application, that will be detailed later. Similarly, we need a process that periodically
moves DPIs from PubSub to GCS. This is because PubSub is the entry point of our architecture, where DPIs 
arrive in the first place. But PubSub is not meant for long term data storage, while GCS is. For this reason,
we will need to run the above process, which is enacted by the batch-ingestion.py application. 
These two applications will need to run on an appropriate infrastructure. In this case, a simple virtual machine 
is enough. In order to create the required virtual machine on Google Compute Engine, first execute
the create-ingestion-vm.sh script; once the VM is created, you can go to the Compute Engine tab and visualize
the VM instance; then you can look for the created VM and click on the SSH button, which will open an ssh session 
to the VM. Once you are logged in, you can finalize the VM configuration by executing the following commands:

    sudo apt-get update
    sudo apt-get install -y python3-pip
    pip3 install pytz google-cloud-storage google-cloud-pubsub ratelimit

##### From the VM ssh session, start in background ingestion process of historical data

Once the VM is configured, we can start in the background the ingestion process of historical data by running the batch-ingestion-py application:

    nohup python3 batch-ingestion.py > batch-ingestion.log 2>&1 &
    
The application will start in the background and will periodically pull new record from the dpi-topic PubSub topic.
The application appends the records into a temporaty file named staging.csv. Once the minimum number of records is reached,
the application uploads the content of staging.csv file into GCS under an appropriate partition. The staging.csv file is then cleaned up.

##### From the VM ssh session, start dpi generator (in background or interactively)

We can now start generating DPI records by running the dpi-generator.py application. The application requires as input
for how long it must run (in minutes) and how many records per minute it has to generate and send. We can start the application in the background for 
a sufficient amount of time by running the following command:

    nohup python3 dpi-generator.py big-data-env 60 30 > dpi-generator.log 2>&1 &
    
In this way the application will produce 100 records per minute, for 30 minutes. Once the application finishes,
 we can restart it with the same command if needed.
 
##### Create a dataproc cluster

Now that the ingestion process is up and running, we can go ahead and process our data. In order to cope with the high volume 
of DPIs, we need a distributed data processing platform, that's why we are going to use Spark and, more specifically, Dataproc,
which is the solution provided by Google managing Spark cluster on GCP. 
We can setup a suitable Spark cluster by running the create-dp-cluster.sh script from the cloud shell.

##### Submit to the data proc cluster the streaming application

At this point, we are ready to start processing our data. We can start by processing the real-time data we ingest into PubSub.
This can be done by executing the compute-kpi-streaming.py application. This will compute, for each customer
and for each category of traffic, the amount of data allowance consumed by the customer during the last 15 minutes;
if such amount is higher than a user-defined threshold, the application generates a trigger
and publishes it into an output PubSub topic, namely dpi-kpi-topic.
In doing this the application takes care of filtering out all records relative to customers who
have not given the consent for data processing. In order to do this, the application loads the consents.csv file
that we have previously uploaded into GCS.
Additionally, the application discards DPI records that arrive too late, meaning that the 
timestamp they carry is way too older, according to a user-defined threshold, than the time at which the application processes the records.
Late records are not considered in the computation of data consumption, but are retained and published
into a second output topic, namely late-dpi-topic, for monitoring and analytics purposes.

In order to submit the streaming application using spark, we can run the submit-streaming-job.sh script from the cloud shell.

##### Look at the various outputs of the streaming application from the pubsub topics web UI

As streaming applications are supposed to run forever, we are not going to wait to the application to terminate and we will just 
leave the application up and running. In order to see some application outputs, one can go to the PubSub pane and to the subscription
tabs; then select the subscription we have previusly created for the desired topic (late-dpi-subscription or dpi-kpi-subscription), 
click on View Messages and finally on the "pull" button.

##### Submit to the dataproc cluster the batch application and wait until it completes

We can now start by processing the historical data we have on GCS.
This can be done by executing the compute-kpi-batch.py application. This will compute, for each customer, for each month and for each hour of the day,
the amount of data allowance that customer consumes, averaged across all the days of the month. This KPI can be useful for example 
to detect when a customer deviates too much from her usual data usage pattern and to react accordingly. 
The application writes the output KPI into a Big Query table in the dataset created before.
In order to execute the batch application using spark, we can run the submit-job.sh script from the cloud shell.

##### Look at the output of the batch application from big query web UI and perform some queries

Once the application is finished, you can open the BigQuery console and have a look at the results.
You can also perform some simple SQL query on the output table.

##### Scheduled the composer dag for the batch application

Batch jobs are often supposed to be scheduled and to be executed periodically.
To this aim, GCP provides Cloud Composer, a service based on Apache Airflow that
simplifies the scheduling of Big Data workflows on GCP. 

As the DPI generator and the DPI batch ingestion processes are still running into our 
Compute Engine instance, we can schedule a Composer application to execute periodically
our batch application on the newly ingested data and we can see updated KPIs into the Big Query table.

We can create a Compose instance and submit the provided DAG (batch-kpi-composer-dag.py),
which will run the compute-kpi-batch.py application every 10 minutes:

- first, as the DAG needs to retrieve the application file from GCS, we need to upload this 
into our bucket. Go to the Cloud Storage pane and into the vf-polimi-batch-data bucket;
create a folder named "dev" and then upload the compute-kpi-batch.py file into the folder

- we then need to create a Composer instance, which will take care of scheduling different tasks.
Go to the composer tab and enable the Composer API. Then click on create an environment configure it
with 3 nodes, n1-standard-2 machine type, 40GB of disk and your region and zone of choice. Also,
select the service account created at the beginning; select the latest image version and python3 as the environment.
The Composer instance creation can take several minutes. Once it completes, you can click on DAG
to go into the folder containing Composer DAGs and upload the batch-kpi-composer-dag.py file. Instead,
by clicking on the "Airflow" button, the Aiflow web UI shows up, from which it is possible to trigger and monitor 
DAGs execution. The DAG for scheduling out batch application is configured to run every 10 minutes.
The various execution can be directly monitored from the Airflow UI, while from Big Query it is possible
to monitor the results that get updated every 10 minutes.
