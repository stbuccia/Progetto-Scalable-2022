import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.{SparkConf, SparkContext}

import scala.io.Source

// @TODO: learn how we parse data in spark context

/**
 *
 * Implements a KMeans clustering model for Iris dataset
 *
 * The outputs shows, for each cluster, the center of each feature.
 * Recall that the features are: sepal lenght, sepal width, petal lenght and petal width
 *
 */

object IrisKMeans {

  def getResourceFile(filePath: String) = {
    val is = getClass.getResourceAsStream(filePath)
    Source.fromInputStream(is).getLines()
  }

  def main(args: Array[String]): Unit = {

    println("Started")

    val appName = "IrisKMeans"
    val master = "local" // or "local[2]"
    val conf = new SparkConf()
      .setAppName(appName)
      .setMaster(master)
    val sc = new SparkContext(conf)


    println("Loading Iris data...")

    val dataFilePath = "/iris.data.txt"
    val src = getResourceFile(dataFilePath).filter(_.nonEmpty).toList
    val textData = sc.parallelize(src)
    val parsedData = textData
      .map(_.split(",").dropRight(1).map(_.toDouble))
      .map(Vectors.dense)
      .cache()


    println("Clustering...")

    // Cluster the data into two classes using KMeans
    val numClusters = 3
    val numIterations = 20
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

