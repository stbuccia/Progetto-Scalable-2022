#!/bin/bash

PROJECT_NAME=natural-axiom-390913
CLUSTER=cluster-apriori
REGION=us-east1
ZONE=us-east1-c
BUCKET=gs://b-apriori
MASTER_MACHINE_TYPE=n1-standard-4
JAR_NAME=armep.jar
DATASET_1=dataset_1.csv
IMAGE_VERSION=2.0-debian10
MAIN_CLASS=Main
MASTER=yarn
NUM_WORKER=4
WORKER_MACHINE_TYPE=n1-standard-4



# creazione bucket
gsutil mb -l $REGION $BUCKET

# copia del file .jar nel bucket
gsutil -m cp \
	demo_upload/$JAR_NAME \
	$BUCKET/$JAR_NAME

# copia dataset nel bucket
gsutil -m cp \
	demo_upload/$DATASET_1 \
	$BUCKET/input/$DATASET_1

 
# creazione cluster 
gcloud dataproc clusters create $CLUSTER \
    --enable-component-gateway \
    --region $REGION \
    --master-machine-type $MASTER_MACHINE_TYPE \
    --master-boot-disk-size 100 \
    --num-workers $NUM_WORKER \
    --worker-machine-type $WORKER_MACHINE_TYPE \
    --worker-boot-disk-size 100 \
    --image-version $IMAGE_VERSION \
    --project $PROJECT_NAME 


# submit job 
DIR=sim_yarn_w4_c4_1
gcloud dataproc jobs submit spark \
    --cluster=$CLUSTER \
    --class=$MAIN_CLASS \
    --jars=$BUCKET/$JAR_NAME \
    --region=$REGION \
    -- --master $MASTER --dataset $BUCKET/input/$DATASET_1 --sim true --output-folder $BUCKET/output/$DIR/


# eliminazione cluster
gcloud dataproc clusters delete $CLUSTER --region=$REGION --quiet
 

# download risultati
gsutil -m cp -r $BUCKET/output/* ./demo_output/


# crea grafico
python3 create_chart.py



