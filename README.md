# Association Rule Mining for Earthquakes Prediction

Project for "Scalable and Cloud Programming" course of the University of Bologna.
The project aims at generating association rules for Earthquakes Dataset, following the cited paper approach. Various version of the Apriori algorithm have been used, exploiting the MapReduce paradigm.

## What does this application do?

The app runs two main phases, which are: **data pre-processing** and **association rule mining**.

### Data pre-processing

During this first part, data are (1) divided into clusters based on their magnitude, then (2) converted so that they assume [0,1] values. 

Clustering allows to search for frequent items inside each cluster, thus having more precise results. This is done by using [KMeans algorithm from Spark's MLib library](https://spark.apache.org/docs/latest/ml-clustering.html).

Once clustered, data are normalized (i.e. converted in [0,1] values). This is done by changing the attributes into cathegorical ones, so that instead of numerical values data will express an attribute with a 0 value if they do  not belong tho the cathegory, 1 instead.

Attributes are updated as follows:

![data-conv](https://github.com/stbuccia/Progetto-Scalable-2022/assets/26099356/be748b5e-2866-45d6-8376-9cd6defb8a54)

### Association Rule Mining

During this phase the choosen mining algorithm is executed for each cluster. The found rules are then grouped, and outputted together with their accuracy. The accuracy measures how likely is for that rule to be true.

The available mining algorithms are:

+ Sequential Apriori
+ MapRedure Apriori
+ MapReduce Apriori with Tail Recursion
+ FP-growth

The first three are directly implemented. FP-growth uses the [Spark's MLib library version](https://spark.apache.org/docs/latest/mllib-frequent-pattern-mining.html). 

Users can choose which one of the algorithms to use. Another option is to run the app in `simulation mode`, where all the four algorithms are executed. Further details on how to choose the mining algorithm can be found in the following section.

## How to run (local mode)

This is the list of arguments necessary for the run

+ `--master <yarn|local[*]|local[2]|...>`
+ `--dataset <dataset_path>`
+ `--<sim <true|false> | classifier <aprioritailrec|aprioriseq|apriorimapreduce|fpgrowth>>` 
+ `--compute-elbow-mode <true|false>`
+ `--output-folder <output_folder_path>`

When running on local mode libraries import from 'build.sbt' file must be changed: the version without 'provided' is the suitable one.

## How to run (cloud mode)

This project has been (mainly) deployed on **Google Cloud Platform (GCP)**. Here are the steps to follow in order to execute it in this mode.

### Configuring GCP

First step is to create a Google Cloud Project and to download the Google Cloud Command Line Interface (CLI). For this you can follow [this guide](https://cloud.google.com/dataproc/docs/guides/setup-project).

### Create a Cloud Storage Bucket

A Bucket is a storing unit you need to create in order to save your data on the cloud. In order to create a new bucket run the following command:

```
gsutil mb -l $REGION gs://$BUCKET_NAME
```
where `$REGION` is chosen from [the available ones](https://cloud.google.com/about/locations), and `$BUCKET_NAME` is chosen freely. Note that the name must be unique in the whole Google Cloud Platform.

### Create a Dataproc Cluster

In order to execute an app based on Apache Spark we need to provide a computing environment. This is done by Dataproc Clusters.

In order to [create a cluster](https://cloud.google.com/dataproc/docs/guides/create-cluster) run the following command:

```
gcloud dataproc clusters create $CLUSTER_NAME \
    --enable-component-gateway \
    --region $REGION \
    --master-machine-type $MASTER_MACHINE_TYPE \
    --master-boot-disk-size 100 \
    --num-workers $N_WORKER \
    --worker-machine-type $WORKER_MACHINE_TYPE \
    --worker-boot-disk-size 100 \
    --image-version 2.0-debian10 \
    --project $PROJECT_NAME
```
where the variables must be defined as follow:

+ `$CLUSTER_NAME` and `$REGION` can be chosen freely. For the region see the link above.
+ both `$MASTER_MACHINE_TYPE` and `$WORKER_MACHINE_TYPE` must be choosen from the [available ones](https://cloud.google.com/compute/docs/general-purpose-machines#n1_machines).
+ `$N_WORKER` is the number of workers available in addition to the master. They can be chosen freely (just check available resources first).
+ `$PROJECT_NAME` must correspond to the id set when creating the Google Cloud project.

### Build project and Bucket upload

In order to build the project run:

```
sbt assembly
gsutil cp target/scala-2.12/armep.jar gs://$BUCKET_NAME/armep.jar
```

where `$BUCKET_NAME` must correspond to the one chosen before.

At this step one may upload in the bucket the dataset as well:

```
gsutil cp dataset/$DATASET_NAME gs://$BUCKET_NAME/data.csv
```

where `$DATASET_NAME` must be substituted with the preferred dataset.


### Launch a Spark Job

This is the actual execution of the app on the cloud. For further details see https://cloud.google.com/dataproc/docs/guides/submit-job#dataproc-submit-job-gcloud.

Run the following command:

```
gcloud dataproc jobs submit spark \
    --cluster=$CLUSTER_NAME \
    --jars=$BUCKET_NAME/armep.jar \
    --region=$REGION \
    -- $MASTER_TYPE \
    $BUCKET_NAME/data.csv \
    $MODE \
    $COMPUTE_ELBOW \
    $BUCKET_NAME/$OUTPUT_FOLDER \
```
where:
 + `$CLUSTER_NAME`, `$BUCKET_NAME` and `$REGION` must correspond to the ones chosen early
 + `$MASTER_TYPE`, `$MODE` and `$COMPUTE_ELBOW` are the app's inputs, defined in the previous section. All the arguments must be wrapped in quotation marks.

### Download Results

To get the output results execute this command:

```
gsutil -m cp -r "gs://$BUCKET_NAME/$OUTPUT_FOLDER/associationrules" .
```
This downloads a folder containing a csv with association rules for each algorithm executed.

To get the time results execute this command: 
```
gsutil -m cp -r "gs://$BUCKET_NAME/$OUTPUT_FOLDER/times" .
```
This download the `times` folder containing a csv with execution time for each algorithm executed.

Again, `$BUCKET_NAME` is defined above and `$OUTPUT_FOLDER` is the same value of the argument `--output-folder` in command. 

## References

+ U. Nivedhitha and Krishna Anand. [Development of a Predictive
System for Anticipating Earthquakes using Data Mining Techniques](https://indjst.org/articles/development-of-a-predictive-system-for-anticipating-earthquakes-using-data-mining-techniques), 2016.
+ Ming-Yen Lin, Pei-Yu Lee, and Sue-Chen Hsueh. [Apriori-Based
Frequent Itemset Mining Algorithms on MapReduce](https://dl.acm.org/doi/10.1145/2184751.2184842), 2012.
