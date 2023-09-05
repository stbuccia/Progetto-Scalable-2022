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

## Usage

This is the list of arguments necessary for the run

+ `--master <yarn|local[*]|local[2]|...>`
+ `--dataset <dataset_path>`
+ `--sim <true|false>` 
+ `--classifier <aprioritailrec|aprioriseq|apriorimapreduce|fpgrowth>`
+ `--compute-elbow-mode <true|false>`
+ `--output-folder <output_folder_path>`

### Download Results

To get the output results execute this command: 
```
gsutil -m cp -r "gs://$BUCKET_NAME/$OUTPUT_FOLDER/associationrules" .
``` 

where `$BUCKET_NAME` is defined above and `$OUTPUT_FOLDER` is the same value of the argument `--output-folder` in command. This downloads a `apriori` folder containing a csv with association rules for each algorithm executed.

### Download Time Results

To get the time results execute this command: 
```
gsutil -m cp -r "gs://$BUCKET_NAME/$OUTPUT_FOLDER/times" .
``` 
where `$BUCKET_NAME` is defined above and `$OUTPUT_FOLDER` is the same value of the argument `--output-folder` in command. This download the `times` folder containing a csv with execution time for each algorithm executed.

## References

+ U. Nivedhitha and Krishna Anand. [Development of a Predictive
System for Anticipating Earthquakes using Data Mining Techniques](https://indjst.org/articles/development-of-a-predictive-system-for-anticipating-earthquakes-using-data-mining-techniques), 2016.
+ Ming-Yen Lin, Pei-Yu Lee, and Sue-Chen Hsueh. [Apriori-Based
Frequent Itemset Mining Algorithms on MapReduce](https://dl.acm.org/doi/10.1145/2184751.2184842), 2012.