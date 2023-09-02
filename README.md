# Association Rule Mining for Earthquakes Prediction

Project for "Scalable and Cloud Programming" course of the University of Bologna.
The project aims at generating association rules for Earthquakes Dataset, following the cited paper approach. Various version of the Apriori algorithm have been used, exploiting the MapReduce paradigm.

## Usage

This is the list of arguments necessary for the run

+ `--master` <yarn|local[*]|local[2]|...>
+ `--dataset` <dataset path>
+ `--sim`  <true|false>
+ `--classifier` <aprioritailrec|aprioriseq|apriorimapreduce|fpgrowth>
+ `--compute-elbow-mode` <true|false>
+ `--output-folder` <output folder path>

## Download Results

To get the output results execute this command: 
```
gsutil -m cp -r "gs://$BUCKET_NAME/$OUTPUT_FOLDER/associationrules" .
``` 
where $BUCKET_NAME is defined above and $OUTPUT_FOLDER is the same value of the argument `--output-folder` in command. This downloads a `apriori` folder containing a csv with association rules for each algorithm executed.

## Download Time Results

To get the time results execute this command: 
```
gsutil -m cp -r "gs://$BUCKET_NAME/$OUTPUT_FOLDER/times" .
``` 
where $BUCKET_NAME is defined above and $OUTPUT_FOLDER is the same value of the argument `--output-folder` in command. This download the `times` folder containing a csv with execution time for each algorithm executed.
