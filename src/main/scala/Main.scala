import associationrulelearning.runApriori.runAprioriSeq
import org.apache.spark.{SparkConf, SparkContext}
import clustering.EarthquakeKMeans.kMeansClustering
import org.apache.spark.sql.SparkSession

import java.io.File

object Main{


  def main(args: Array[String]): Unit = {


    // Check arguments
    if (args.length != 3) {
      println("Missing arguments!")
      System.exit(1)
    }

    val master = args(0)
    val datasetPath = args(1)
    val outputFolder = args(2)

    println("Configuration:")
    println(s"- master: $master")
    println(s"- dataset path: $datasetPath")
    println(s"- output folder: $outputFolder")


    // Initiate Spark Session/Context

    println("Started")

    val appName = "AssociationRuleLearning.Apriori"
    //val master = "local" // or "local[2]"
    val sparkSession = SparkSession.builder()
      .appName(appName)
      .master(master)
      .getOrCreate()
    val sc = sparkSession.sparkContext


    // Load dataset

    val datasetDF = sparkSession.read
      .option("delimiter",",")
      .option("header", value = true)
      .csv(datasetPath)

//    val datasetRDD = sc.textFile(datasetPath)
//      .map(_.split(","))
//      .cache()


    // Run clustering and normalize data

    val attributeForClustering = 3  // chose magnitude as dimension on which to perform clustering

//    val keyValuedDataset = datasetRDD.map(entry => (entry, entry(attributeForClustering)))  // transforms the RDD in a key-valued one in order to easily perform join

    val clusteredData = kMeansClustering(sc, datasetDF, attributeForClustering, 5, 20, "clusteredDataMag")


    // Update data with the clusters info


    // Run algorithms

    val folder = new File("src/main/resources/")
    if (folder.exists && folder.isDirectory)
      folder.listFiles
        .filter(file => file.toString.contains("label"))
        .toList
        .foreach(file => runAprioriSeq(sc, file.getPath))
  }

}
