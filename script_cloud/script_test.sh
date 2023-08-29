#!/bin/bash

PROJECT_NAME=natural-axiom-390913
CLUSTER=cluster-apriori
REGION=us-east1
ZONE=us-east1-c
BUCKET=gs://bucket-apriori
MASTER_MACHINE_TYPE=n1-standard-4
JAR_NAME=armep.jar
DATASET_1=dataset_1.csv
DATASET_2=dataset_2.csv
DATASET_3=dataset_3.csv
DATASET_4=dataset_4.csv
IMAGE_VERSION=2.0-debian10
MAIN_CLASS=Main
MASTER=yarn


INIT=1
if [[ $INIT -eq 1 ]]
then
    # creazione bucket
    gsutil mb -l $REGION $BUCKET

    # copia del file .jar nel bucket
    gsutil -m cp \
        upload/$JAR_NAME \
        $BUCKET/$JAR_NAME

    # copia dataset nel bucket
    gsutil -m cp \
        upload/$DATASET_1 \
        $BUCKET/input/$DATASET_1

    gsutil -m cp \
        upload/$DATASET_2 \
        $BUCKET/input/$DATASET_2

    gsutil -m cp \
        upload/$DATASET_3 \
        $BUCKET/input/$DATASET_3

    gsutil -m cp \
        upload/$DATASET_4 \
        $BUCKET/input/$DATASET_4
fi


if [[ $INIT -eq 0 ]]
then
    # elimina file nella cartelle di output
    gsutil -m rm -r $BUCKET/output/
fi




# ESECUZIONE TEST - 4 worker n1-standard-4 ------------------------------------------------------------------------------------------
    NUM_WORKER=4
    WORKER_MACHINE_TYPE=n1-standard-4
    echo -e "\nTEST - 4 worker n1-standard-4\n"
    
    # creazione cluster con 4 worker n1-standard-4
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

    echo -e "\nCreato cluster con 4 worker n1-standard-4\n"

    # submit job con dataset 1
    DIR=sim_yarn_w4_c4_1
    gcloud dataproc jobs submit spark \
        --cluster=$CLUSTER \
        --class=$MAIN_CLASS \
        --jars=$BUCKET/$JAR_NAME \
        --region=$REGION \
        -- $MASTER $BUCKET/input/$DATASET_1 "sim=true" $BUCKET/output/$DIR/

    # submit job con dataset 2
    DIR=sim_yarn_w4_c4_2
    gcloud dataproc jobs submit spark \
        --cluster=$CLUSTER \
        --class=$MAIN_CLASS \
        --jars=$BUCKET/$JAR_NAME \
        --region=$REGION \
        -- $MASTER $BUCKET/input/$DATASET_2 "sim=true" $BUCKET/output/$DIR/

    # submit job con dataset 3
    DIR=sim_yarn_w4_c4_3
    gcloud dataproc jobs submit spark \
        --cluster=$CLUSTER \
        --class=$MAIN_CLASS \
        --jars=$BUCKET/$JAR_NAME \
        --region=$REGION \
        -- $MASTER $BUCKET/input/$DATASET_3 "sim=true" $BUCKET/output/$DIR/

     # submit job con dataset 4
    DIR=sim_yarn_w4_c4_4
    gcloud dataproc jobs submit spark \
        --cluster=$CLUSTER \
        --class=$MAIN_CLASS \
        --jars=$BUCKET/$JAR_NAME \
        --region=$REGION \
        -- $MASTER $BUCKET/input/$DATASET_4 "sim=true" $BUCKET/output/$DIR/


    gcloud dataproc clusters delete $CLUSTER --region=$REGION --quiet
    echo -e "\nEliminato cluster con 4 worker n1-standard-4\n"


# ESECUZIONE TEST - 4 worker n1-standard-2 ------------------------------------------------------------------------------------------
    NUM_WORKER=4
    WORKER_MACHINE_TYPE=n1-standard-2
    echo -e "\nTEST - 4 worker n1-standard-2\n"
    
    # creazione cluster con 4 worker n1-standard-2
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

    echo -e "\nCreato cluster con 4 worker n1-standard-2\n"

    # submit job con dataset 1
    DIR=sim_yarn_w4_c2_1
    gcloud dataproc jobs submit spark \
        --cluster=$CLUSTER \
        --class=$MAIN_CLASS \
        --jars=$BUCKET/$JAR_NAME \
        --region=$REGION \
        -- $MASTER $BUCKET/input/$DATASET_1 "sim=true" $BUCKET/output/$DIR/

    # submit job con dataset 2
    DIR=sim_yarn_w4_c2_2
    gcloud dataproc jobs submit spark \
        --cluster=$CLUSTER \
        --class=$MAIN_CLASS \
        --jars=$BUCKET/$JAR_NAME \
        --region=$REGION \
        -- $MASTER $BUCKET/input/$DATASET_2 "sim=true" $BUCKET/output/$DIR/

    # submit job con dataset 3
    DIR=sim_yarn_w4_c2_3
    gcloud dataproc jobs submit spark \
        --cluster=$CLUSTER \
        --class=$MAIN_CLASS \
        --jars=$BUCKET/$JAR_NAME \
        --region=$REGION \
        -- $MASTER $BUCKET/input/$DATASET_3 "sim=true" $BUCKET/output/$DIR/

     # submit job con dataset 4
    DIR=sim_yarn_w4_c2_4
    gcloud dataproc jobs submit spark \
        --cluster=$CLUSTER \
        --class=$MAIN_CLASS \
        --jars=$BUCKET/$JAR_NAME \
        --region=$REGION \
        -- $MASTER $BUCKET/input/$DATASET_4 "sim=true" $BUCKET/output/$DIR/


    gcloud dataproc clusters delete $CLUSTER --region=$REGION --quiet
    echo -e "\nEliminato cluster con 4 worker n1-standard-2\n"


# ESECUZIONE TEST - 2 worker n1-standard-2 ------------------------------------------------------------------------------------------
    NUM_WORKER=2 
    WORKER_MACHINE_TYPE=n1-standard-2
    echo -e "\nTEST - 4 worker n1-standard-2\n"
    
    # creazione cluster con 4 worker n1-standard-2
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

    echo -e "\nCreato cluster con 2 worker n1-standard-2\n"

    # submit job con dataset 1
    DIR=sim_yarn_w2_c2_1
    gcloud dataproc jobs submit spark \
        --cluster=$CLUSTER \
        --class=$MAIN_CLASS \
        --jars=$BUCKET/$JAR_NAME \
        --region=$REGION \
        -- $MASTER $BUCKET/input/$DATASET_1 "sim=true" $BUCKET/output/$DIR/

    # submit job con dataset 2
    DIR=sim_yarn_w2_c2_2
    gcloud dataproc jobs submit spark \
        --cluster=$CLUSTER \
        --class=$MAIN_CLASS \
        --jars=$BUCKET/$JAR_NAME \
        --region=$REGION \
        -- $MASTER $BUCKET/input/$DATASET_2 "sim=true" $BUCKET/output/$DIR/

    # submit job con dataset 3
    DIR=sim_yarn_w2_c2_3
    gcloud dataproc jobs submit spark \
        --cluster=$CLUSTER \
        --class=$MAIN_CLASS \
        --jars=$BUCKET/$JAR_NAME \
        --region=$REGION \
        -- $MASTER $BUCKET/input/$DATASET_3 "sim=true" $BUCKET/output/$DIR/

     # submit job con dataset 4
    DIR=sim_yarn_w2_c2_4
    gcloud dataproc jobs submit spark \
        --cluster=$CLUSTER \
        --class=$MAIN_CLASS \
        --jars=$BUCKET/$JAR_NAME \
        --region=$REGION \
        -- $MASTER $BUCKET/input/$DATASET_4 "sim=true" $BUCKET/output/$DIR/


    gcloud dataproc clusters delete $CLUSTER --region=$REGION --quiet
    echo -e "\nEliminato cluster con 2 worker n1-standard-2\n"


# ESECUZIONE TEST - 2 worker n1-standard-1 ------------------------------------------------------------------------------------------
    NUM_WORKER=2 
    WORKER_MACHINE_TYPE=custom-1-6656-ext
    echo -e "\nTEST - 4 worker n1-standard-1\n"
    
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

    echo -e "\nCreato cluster con 2 worker n1-standard-1\n"

    # submit job con dataset 1
    DIR=sim_yarn_w2_c1_1
    gcloud dataproc jobs submit spark \
        --cluster=$CLUSTER \
        --class=$MAIN_CLASS \
        --jars=$BUCKET/$JAR_NAME \
        --region=$REGION \
        -- $MASTER $BUCKET/input/$DATASET_1 "sim=true" $BUCKET/output/$DIR/

    # submit job con dataset 2
    DIR=sim_yarn_w2_c1_2
    gcloud dataproc jobs submit spark \
        --cluster=$CLUSTER \
        --class=$MAIN_CLASS \
        --jars=$BUCKET/$JAR_NAME \
        --region=$REGION \
        -- $MASTER $BUCKET/input/$DATASET_2 "sim=true" $BUCKET/output/$DIR/

    # submit job con dataset 3
    DIR=sim_yarn_w2_c1_3
    gcloud dataproc jobs submit spark \
        --cluster=$CLUSTER \
        --class=$MAIN_CLASS \
        --jars=$BUCKET/$JAR_NAME \
        --region=$REGION \
        -- $MASTER $BUCKET/input/$DATASET_3 "sim=true" $BUCKET/output/$DIR/

     # submit job con dataset 4
    DIR=sim_yarn_w2_c1_4
    gcloud dataproc jobs submit spark \
        --cluster=$CLUSTER \
        --class=$MAIN_CLASS \
        --jars=$BUCKET/$JAR_NAME \
        --region=$REGION \
        -- $MASTER $BUCKET/input/$DATASET_4 "sim=true" $BUCKET/output/$DIR/


    gcloud dataproc clusters delete $CLUSTER --region=$REGION --quiet
    echo -e "\nEliminato cluster con 2 worker n1-standard-1\n"


# DOWNLOAD ------------------------------------------------------------------------------------------
    echo -e "\nDownload risultati\n"

    # elimino file nella cartella di output
    #rm -r ./output/*

    # download risultati
    gsutil -m cp -r $BUCKET/output/* ./output/
    #gsutil -m cp -r gs://bucket-apriori/output/* ./output/
