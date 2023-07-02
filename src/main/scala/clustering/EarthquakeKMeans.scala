package clustering

import com.cibo.evilplot.colors._
import com.cibo.evilplot.demo.DemoPlots.theme
import com.cibo.evilplot.numeric._
import com.cibo.evilplot.plot._
import model.Event
import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, Row}

import java.io.{File}


/**
 *
 * Implements a KMeans clustering model for Earthquakes dataset
 *
 * The outputs shows, for each cluster, the center of each feature.
 * Recall that the features are: latitude, longitude, depth, mag and year
 *
 *
 */

object EarthquakeKMeans {


  /**
   * Divides data in clusters.
   * For each cluster it generates a subset of the given dataset, and save it in the /src/main/resources folder.
   *
   * @param sc Spark Context
   * @param datasetDF  dataset file in Spark DataFrame format
   * @param dimension  column index from where read the data
   * @param numClusters number of clusters we want to get
   * @param numIterations maximum number of iteration the algorithm can do
   * @param modelName name of the returned model
   * @return an RDD containing every data associated with its cluster, in the form (Value, Cluster_Index)
   */
  def kMeansClustering(sc: SparkContext, datasetDF: DataFrame, dimension: Int, numClusters: Int, numIterations: Int, modelName: String = "kMeansClusteredData"): RDD[(Int, Event)] = {

    // Loading dataset

//    val src = getResourceFile(dataFilePath).filter(_.nonEmpty).drop(1).toList
//    val textData = sc.parallelize(src)
//    val parsedData = textData
//      .map(_.split(","))
//      .cache()

//    val columnData: RDD[Double] = parsedData.map(fields => fields(column).toDouble)

    val datasetRDD = datasetDF.rdd.map(fromRowToRddEntry)

    val datasetColumn = datasetDF.rdd.map(row => row(dimension).toString.toDouble)
    val vectors: RDD[org.apache.spark.mllib.linalg.Vector] = datasetColumn.map(value => Vectors.dense(value))


    // Cluster the data using KMeans
    val clusters = KMeans.train(vectors, numClusters, numIterations) // returns a KMeansModel obj


    println("Computing elbow method...")

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
    val discretizedData: RDD[(Double, Int)] = datasetColumn.map(value => {
      val vector = Vectors.dense(value)
      val clusterIndex = clusters.predict(vector)
      (value, clusterIndex)
    })

    // Build and return the dataset together with cluster information
    val clusteredDataset: RDD[(Double, (Int, Event))] = discretizedData.join(datasetRDD)

    clusteredDataset.map(_._2)

//    // Discretize the vector represented data using the K-means model
//    val discretizedData2 = parsedData.map(row => {
//      val value = row(column).toDouble
//      val vector = Vectors.dense(value)
//      val clusterIndex = clusters.predict(vector)
//      (row.mkString(","), clusterIndex)
//    })//.map { case (fields, clusterIndex) => (fields.mkString(","), clusterIndex) }


//    // Write each cluster's data to separate files
//    for (clusterIndex <- 0 until numClusters) {
//      val clusterData = discretizedData2.filter { case (_, c) => c == clusterIndex }
//      //clusterData.saveAsTextFile(s"${modelName}_cluster$clusterIndex")
//      var filePath = "src/main/resources/dataset_2010_2021_cluster" + clusterIndex + ".csv"
//      writeRDDToCSV(clusterData, filePath);
//      dataconversion.mainDataConversion.normalizeDataset(sc, filePath)
//    }

  }

      /**
   * Creates a lineplot with the WSSSE obtained for different number of clusters
   * @param wss_list list of wss calculated for each numnber of clusters
   * @param clusters_range min - max number of cluster tested
   * @param filename output filename
   */
  private def saveElbowLinePlot(wss_list: Seq[Double], clusters_range: Seq[Int], filename: String) = {
    println("Save elbow lineplot...")
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
  private def computeElbow(start: Int, end: Int, input_data_points: RDD[org.apache.spark.mllib.linalg.Vector],
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

//  /**
//   * Writes a given RDD into a CSV file.
//   * @param rdd input data to be written
//   * @param filePath  of the CSV file
//   */
//  private def writeRDDToCSV(rdd: RDD[(String, Int)], filePath: String): Unit = {
//    val printWriter = new PrintWriter(new File(filePath))
//
//    // Write the data rows
//    rdd.collect().foreach { case (strValue, _) =>
//      val row = s"$strValue"
//      printWriter.write(row + "\n")
//    }
//
//    // Close the writer
//    printWriter.close()
//  }

  private def fromRowToRddEntry(row: Row): (Double, Event) = {
    (row(3).toString.toDouble, new Event((row(0).toString.toDouble, row(1).toString.toDouble),
      row(2).toString.toDouble,
      row(3).toString.toDouble,
      row(4).toString.toInt))
  }


//  def main(args: Array[String]): Unit = {
//
//    println("Started")
//
//    val appName = "Clustering.EarthquakeKMeans"
//    val master = "local" // or "local[2]"
//    val conf = new SparkConf()
//      .setAppName(appName)
//      .setMaster(master)
//    val sc = new SparkContext(conf)
//
//    println("Loading Earthquake data...")
//
//    val discretizedDataMag: RDD[(Double, Int)] = kMeansClustering(sc, "/dataset_from_2010_01_to_2021_12.csv", 3, 5, 20, "clusteredDataMag")
//  }

}

