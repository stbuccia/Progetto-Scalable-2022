
import associationrulelearning.{AprioriSeq, AprioriSparkSPC, FPGrowth}
import clustering.EarthquakeKMeans.kMeansClustering
import dataconversion.mainDataConversion.labelConversion
import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD

import scala.io.StdIn.readLine


object Main{

  // Spark UI: http://localhost:4040/jobs/

  def main(args: Array[String]): Unit = {

    // Check arguments
    if (args.length < 4) {
      println("Missing arguments!")
      System.exit(1)
    }

    val master = args(0)
    val datasetPath = args(1)
    val simulation = if (args(2) == "sim=true") true else false
    val outputFolder = args(3)

    var classifier = ""

    if (!simulation) {
        classifier = args(4)
    }

    println("Configuration:")
    println(s"- master: $master")
    println(s"- dataset path: $datasetPath")
    if (!simulation) {
        println(s"- classifier: $classifier")
    }
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
    sc.setLogLevel("WARN")


    // Load dataset

    val datasetDF = sparkSession.read
      .option("delimiter",",")
      .option("header", value = true)
      .csv(datasetPath)


    // Run clustering and update data with cluster info

    val attributeForClustering = 3  // chose magnitude as dimension on which to perform clustering
    val numClusters = 5
    val clusteredData = kMeansClustering(sc, datasetDF, attributeForClustering, numClusters, 20, "clusteredDataMag", computeElbowMode = false)

    // Normalize data

    val normalizedData: RDD[(Int, Set[String])] = clusteredData.map(entry => (entry._1,labelConversion(entry._2)))


    if (simulation) {
      time(s"[apriori sequential]", runAprioriForEachCluster(numClusters, normalizedData, "aprioriseq"))
      time(s"[apriori single pass count]", runAprioriForEachCluster(numClusters, normalizedData, "apriorispc"))
      time(s"[fpgrowth]", runAprioriForEachCluster(numClusters, normalizedData, "fpgrowth"))
    } else {
      classifier match {
        case "aprioriseq" =>
          time(s"[apriori sequential]", runAprioriForEachCluster(numClusters, normalizedData, "aprioriseq"))
        case "apriorispc" =>
          time(s"[apriori single pass count]", runAprioriForEachCluster(numClusters, normalizedData, "apriorispc"))
        case "fpgrowth" =>
          time(s"[fpgrowth]", runAprioriForEachCluster(numClusters, normalizedData, "fpgrowth"))
      }
    }

    sparkSession.stop()

    println("\nMain method completed")
//    readLine()

  }

   def runAprioriForEachCluster(numClusters: Int, dataset: RDD[(Int, Set[String])], model: String): Unit = {

    // Run algorithm for each cluster
    for (clusterIndex <- 0 until numClusters) {
      println()
      println(s"$model - computing cluster $clusterIndex...")
      val transactions: RDD[Set[String]] = dataset.filter(_._1 == clusterIndex).map(_._2)

      model match {
        case "aprioriseq" =>
          val collectedTransaction = transactions.collect().toSeq
          val seqInstance = new AprioriSeq(collectedTransaction)
          seqInstance.run()
        case "apriorispc" =>
          val spcInstance = new AprioriSparkSPC(transactions)
          spcInstance.run()
        case "fpgrowth" =>
          val fpgrowthInstance = new FPGrowth(transactions)
          fpgrowthInstance.run()
      }
    }
  }


  def time[R](label: String, block: => R): R = {
    val t0 = System.nanoTime()
    val result = block
    val t1 = System.nanoTime()
    println()
    println(s"$label - elapsed time: " + (t1 - t0) / 1000000 + "ms")
    result
  }

}
