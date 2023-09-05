# Association Rule Mining for Earthquakes Prediction

Project for "Scalable and Cloud Programming" course of the University of Bologna.
The project aims at generating association rules for Earthquakes Dataset, following the cited paper approach. Various version of the Apriori algorithm have been used, exploiting the MapReduce paradigm.

##

The app runs two main phases, which are: **data pre-processing** and **association rule mining**.

### Data pre-processing

During this first part, data are (1) divided into clusters based on their magnitude, then (2) converted so that they assume [0,1] values. 

Clustering allows to search for frequent items inside each cluster, thus having more precise results. This is done by using [KMeans algorithm from Spark's MLib library](https://spark.apache.org/docs/latest/ml-clustering.html).

Once clustered, data are normalized (i.e. converted in [0,1] values). This is done by changing the attributes into cathegorical ones, so that instead of numerical values data will express an attribute with a 0 value if they do  not belong tho the cathegory, 1 instead.

Attributes are updated as follows:



### Association Rule Mining


## Usage

This is the list of arguments necessary for the run

+ `--master <yarn|local[*]|local[2]|...>`
+ `--dataset <dataset_path>`
+ `--sim <true|false>` 
+ `--classifier <aprioritailrec|aprioriseq|apriorimapreduce|fpgrowth>`
+ `--compute-elbow-mode <true|false>`
+ `--output-folder <output_folder_path>`

## Download Results

To get the output results execute this command: 
```
gsutil -m cp -r "gs://$BUCKET_NAME/$OUTPUT_FOLDER/associationrules" .
``` 
where `$BUCKET_NAME` is defined above and `$OUTPUT_FOLDER` is the same value of the argument `--output-folder` in command. This downloads a `apriori` folder containing a csv with association rules for each algorithm executed.

## Download Time Results

To get the time results execute this command: 
```
gsutil -m cp -r "gs://$BUCKET_NAME/$OUTPUT_FOLDER/times" .
``` 
where `$BUCKET_NAME` is defined above and `$OUTPUT_FOLDER` is the same value of the argument `--output-folder` in command. This download the `times` folder containing a csv with execution time for each algorithm executed.
