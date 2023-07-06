//import associationrulelearning.runApriori.runAprioriSeq
import associationrulelearning.AprioriSparkSPC
import clustering.EarthquakeKMeans.kMeansClustering
import dataconversion.mainDataConversion.{RDDLabelConversion, labelConversion}
import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD

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
    sc.setLogLevel("WARN")


    // Load dataset

    val datasetDF = sparkSession.read
      .option("delimiter",",")
      .option("header", value = true)
      .csv(datasetPath)

    // Run clustering and update data with cluster info
    val attributeForClustering = 3  // chose magnitude as dimension on which to perform clustering
    val numClusters = 5
    val clusteredData = kMeansClustering(sc, datasetDF, attributeForClustering, numClusters, 20, "clusteredDataMag")

    // Normalize data
    val normalizedData: RDD[(Int, Set[String])]= clusteredData.map(entry => (entry._1,labelConversion(entry._2)))

    for (clusterIndex <- 0 until numClusters) {
      println()
      println(s"Computing cluster $clusterIndex...")
      val transactions: RDD[Set[String]] = normalizedData.filter(_._1 == clusterIndex).map(_._2)
      val alg = new AprioriSparkSPC(transactions, 0.6, 0.7)
      alg.run()
    }

  }

}
