import associationrulelearning.runApriori
import associationrulelearning.runApriori.runAprioriSeq
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import clustering.EarthquakeKMeans
import clustering.EarthquakeKMeans.kMeansClustering

import java.io.File

object Main{

  def main(args: Array[String]): Unit = {

    // Check arguments

    // Initiate Spark Session/Context

    println("Started")

    val appName = "AssociationRuleLearning.Apriori"
    val master = "local" // or "local[2]"
    val conf = new SparkConf()
      .setAppName(appName)
      .setMaster(master)
    val sc = new SparkContext(conf)

    // Run clustering and normalize data

    val datasetFilePath = "/dataset_from_2010_01_to_2021_12.csv"
    kMeansClustering(sc, datasetFilePath, 3, 5, 20, "clusteredDataMag")

    // Run algorithms

    val folder = new File("src/main/resources/")
    if (folder.exists && folder.isDirectory)
      folder.listFiles
        .filter(file => file.toString.contains("label"))
        .toList
        .foreach(file => runAprioriSeq(sc, file.getPath))
  }

}
