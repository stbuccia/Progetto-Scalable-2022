import associationrulelearning.runApriori.runAprioriSeq
import clustering.EarthquakeKMeans.kMeansClustering
import dataconversion.mainDataConversion.{RDDLabelConversion, labelConversion}
import org.apache.spark.sql.SparkSession


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
    val clusteredData = kMeansClustering(sc, datasetDF, attributeForClustering, 5, 20, "clusteredDataMag")


    // Normalize data

    val normalizedData = clusteredData.map(entry => (entry._1, labelConversion(entry._2)))



    // Run algorithm for each cluster

//    val folder = new File("src/main/resources/")
//    if (folder.exists && folder.isDirectory)
//      folder.listFiles
//        .filter(file => file.toString.contains("label"))
//        .toList
//        .foreach(file => runAprioriSeq(sc, file.getPath))


  }

}
