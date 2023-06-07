package Clustering

import com.cibo.evilplot.colors._
import com.cibo.evilplot.demo.DemoPlots.theme
import com.cibo.evilplot.numeric._
import com.cibo.evilplot.plot._
import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import java.io.File
import scala.io.Source


/**
 *
 * Implements a KMeans clustering model for eathquakes dataset
 *
 * The outputs shows, for each cluster, the center of each feature.
 * Recall that the features are: latitude, longitude, depth, mag and year
 *
 */

object EarthquakeKMeans {

  def getResourceFile(filePath: String) = {
    val is = getClass.getResourceAsStream(filePath)
    Source.fromInputStream(is).getLines()
  }

    /**
   * Creates a lineplot with the WSSSE obtained for different number of clusters
   * @param wss_list list of wss calculated for each numnber of clusters
   * @param clusters_range min - max number of cluster tested
   * @param filename output filename
   */
  def saveElbowLinePlot(wss_list: Seq[Double], clusters_range: Seq[Int], filename: String) = {
    println("Save elbow lineplot.......")
    val data = clusters_range.zip(wss_list).map(pair => Point(pair._1, pair._2))

    LinePlot.series(data, "WSSSE elbow", HSL(210, 100, 56))
      .xAxis().yAxis().frame()
      .xLabel("Number of cluster")
      .yLabel("WSSSE")
      .render()
      .write(new File(filename))
  }

  /**
   * Compute the elbow graph by calculating the WSSSE for each number of clusters
   * @param start min number of clusters
   * @param end max number of clusters
   * @param input_data_points RDD[points]
   * @param num_iteration maxim number of iteration for KMeans algorithm
   * @param filename png file in which the result will be saved
   */
  def computeElbow(start: Int, end: Int, input_data_points: RDD[org.apache.spark.mllib.linalg.Vector],
                   num_iteration: Int, filename: String = "kmeans_elbow_plot.png"): Any = {

    // (E) Elbow method to know the best number of clusters
    val clusters_range = start to end
    val wss_list = for{
      num_centroids <- clusters_range
      // compute kmeans
      clusters = KMeans.train(input_data_points, num_centroids, num_iteration)
      // compute the "error" measure
      wss = clusters.computeCost(input_data_points)
    } yield wss

    saveElbowLinePlot(wss_list, clusters_range, filename)
  }

  /**
   * Divides data in clusters
   * @param sc Spark Context
   * @param dataFilePath  input data file location
   * @param column  column index from where read the data
   * @param numClusters number of clusters we want to get
   * @param numIterations maximum number of iteration the algorithm can do
   * @param modelName name of the returned model
   * @return
   */
  def kMeansClustering(sc: SparkContext, dataFilePath: String, column: Int, numClusters: Int, numIterations: Int, modelName: String = "kMeansClusteredData"): RDD[(Double, Int)] = {
      val src = getResourceFile(dataFilePath).filter(_.nonEmpty).drop(1).toList
      val textData = sc.parallelize(src)
      val parsedData = textData
        .map(_.split(",")(column))
        .map(_.toDouble)
        .cache()

      val vectors: RDD[org.apache.spark.mllib.linalg.Vector] = parsedData.map(value => Vectors.dense(value))

      println("Clustering...")

      // Cluster the data using KMeans
      val clusters = KMeans.train(vectors, numClusters, numIterations) // returns a KMeansModel obj


      println("Computing elbow method....")

      // Elbow method computation
      computeElbow(2, 10, vectors, numIterations, modelName + "ElbowPlot.png")


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

    val appName = "Clustering.EarthquakeKMeans"
    val master = "local" // or "local[2]"
    val conf = new SparkConf()
      .setAppName(appName)
      .setMaster(master)
    val sc = new SparkContext(conf)

    println("Loading Earthquake data...")

    val discretizedDataMag: RDD[(Double, Int)] = kMeansClustering(sc, "/dataset_from_2010_01_to_2021_12.csv", 3, 5, 20, "clusteredDataMag")
  }
}

