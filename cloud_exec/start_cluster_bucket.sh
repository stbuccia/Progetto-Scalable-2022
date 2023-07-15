#!/bin/bash

source my_custom.env

# creazione bucket
gsutil mb -l $GC_REGION $GC_BUCKET

# copia del file .jar nel bucket
gsutil cp \
	upload/$GC_JAR_NAME \
	$GC_BUCKET/$GC_JAR_NAME

# copia dataset nel bucket
gsutil cp \
    upload/$GC_DATASET_NAME \
    $GC_BUCKET/input/$GC_DATASET_NAME

# creazione cluster
gcloud dataproc clusters create $GC_CLUSTER \
    --enable-component-gateway \
    --region $GC_REGION \
    --zone $GC_ZONE \
    --master-machine-type $GC_MASTER_MACHINE_TYPE \
    --master-boot-disk-size 100 \
    --num-workers $GC_N_WORKER \
    --worker-machine-type $GC_WORKER_MACHINE_TYPE \
    --worker-boot-disk-size 100 \
    --image-version 2.0-debian10 \
    --project natural-axiom-390913










# download file
#gsutil cp \
#    $GC_BUCKET/$GC_JAR_NAME \
#   download/$GC_JAR_NAME