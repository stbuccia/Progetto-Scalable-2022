#!/bin/bash

source my_custom.env

#gcloud dataproc jobs submit spark     --cluster=example-word-count-cluster --class=Main --jars=gs://example_word_count_bucket/scala/Progetto-Scalable-2022-assembly-Progetto-Scalable-2022.jar --region=europe-west12 -- yarn gs://example_word_count_bucket/input/dataset_from_2010_01_to_2021_12.csv gs://example_word_count_bucket

# submit job
gcloud dataproc jobs submit spark \
    --cluster=$GC_CLUSTER \
    --class=$GC_MAIN_CLASS \
    --jars=$GC_BUCKET/$GC_JAR_NAME \
    --region=$GC_REGION \
    -- $GC_MASTER $GC_BUCKET/input/$GC_DATASET_NAME $GC_BUCKET/
