#!/bin/bash

source my_custom.env

gsutil rm -r $GC_BUCKET

gcloud dataproc clusters delete $GC_CLUSTER --region=$GC_REGION

gsutil -m rm -r gs://dataproc*