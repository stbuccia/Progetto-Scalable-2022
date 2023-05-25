import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.{SparkConf, SparkContext}


import scala.io.Source

// @TODO: learn how we parse data in spark context

/**
 *
 * Implements a KMeans clustering model for earthquake dataset
 *
 * The outputs shows, for each cluster, the center of each feature.
 * Recall that the features are: latitude, longitude, depth, mag, year
 *
 */

object KMeansCluster {

  def getResourceFile(filePath: String) = {
    val is = getClass.getResourceAsStream(filePath)
    Source.fromInputStream(is).getLines()
  }

  def main(args: Array[String]): Unit = {

    println("Started")

    val appName = "KMeans"
    val master = "local" // or "local[2]"
    val conf = new SparkConf()
      .setAppName(appName)
      .setMaster(master)
    val sc = new SparkContext(conf)


    println("Loading data...")

    val dataFilePath = "/dataset_from_2020_01_to_2021_12.csv"
    val src = getResourceFile(dataFilePath).filter(_.nonEmpty).toList
    val textData = sc.parallelize(src)
    val parsedData = textData
      .map(_.split(",").map(_.toDouble))
      .map(Vectors.dense)
      .cache()

    println("Clustering...")

    // Cluster the data into 3 classes using KMeans
    val numClusters = 3
    val numIterations = 200
    val clusters = KMeans.train(parsedData, numClusters, numIterations) // returns a KMeansModel obj


    println("Results:")

    // Evaluate clustering by computing Within Set Sum of Squared Errors
    val WSSSE = clusters.computeCost(parsedData)
    println(s"Within Set Sum of Squared Errors = $WSSSE")

    // Show significant output
    clusters.clusterCenters.zipWithIndex.foreach { case (center, idx) =>
      println(s"Cluster Center ${idx}: ${center}")
    }

    sc.stop()
  }

}

