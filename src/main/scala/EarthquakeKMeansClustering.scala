import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.rdd.RDD
import breeze.plot._

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

object EarthquakeKMeans {

  def getResourceFile(filePath: String) = {
    val is = getClass.getResourceAsStream(filePath)
    Source.fromInputStream(is).getLines()
  }

  def kMeansClustering(sc: SparkContext, dataFilePath: String, column: Int, numClusters: Int, numIterations: Int): RDD[(Double, Int)] = {
      val src = getResourceFile(dataFilePath).filter(_.nonEmpty).drop(1).toList
      val textData = sc.parallelize(src)
      val magColumn = 3
      val parsedData = textData
        .map(_.split(",")(column))
        .map(_.toDouble) 
        //.map(Vectors.dense)
        .cache()

      val vectors: RDD[org.apache.spark.mllib.linalg.Vector] = parsedData.map(value => Vectors.dense(value))

      println("Clustering...")

      // Cluster the data into two classes using KMeans
      val numClusters = 3
      val numIterations = 20
      val clusters = KMeans.train(vectors, numClusters, numIterations) // returns a KMeansModel obj


      println("Results:")

      // Evaluate clustering by computing Within Set Sum of Squared Errors
      val WSSSE = clusters.computeCost(vectors)
      println(s"Within Set Sum of Squared Errors = $WSSSE")

      // Show significant output
      clusters.clusterCenters.zipWithIndex.foreach { case (center, idx) =>
        println(s"Cluster Center ${idx}: ${center}")
      }

      // Get the cluster labels for each data point
      val clusterLabels: RDD[Int] = clusters.predict(vectors)

      // Count the number of data points in each cluster
      val clusterSizes: Array[Long] = clusterLabels.countByValue().toArray.sortBy(_._1).map(_._2)


      // Print the cluster sizes
      clusterSizes.zipWithIndex.foreach { case (size, clusterIndex) =>
        println(s"Cluster $clusterIndex size: $size")
      }
      
      // Discretize the extracted column using the trained K-means model
      val discretizedData: RDD[(Double, Int)] = parsedData.map(value => {
        val vector = Vectors.dense(value)
        val clusterIndex = clusters.predict(vector)
        (value, clusterIndex)
      })

      discretizedData
  }

  def main(args: Array[String]): Unit = {

    println("Started")

    val appName = "EarthquakeKMeans"
    val master = "local" // or "local[2]"
    val conf = new SparkConf()
      .setAppName(appName)
      .setMaster(master)
    val sc = new SparkContext(conf)

    println("Loading Earthquake data...")

    val discretizedDataMag: RDD[(Double, Int)] = kMeansClustering(sc, "/dataset_from_2010_01_to_2021_12.csv", 3, 3, 20)
    val discretizedDataDepth: RDD[(Double, Int)] = kMeansClustering(sc, "/dataset_from_2010_01_to_2021_12.csv", 2, 3, 20)
  }
}

