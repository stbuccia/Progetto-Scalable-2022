
import associationrulelearning.VerifyAssociationRules.verify
import associationrulelearning.{AprioriMapReduce, AprioriSeq, AprioriSparkSPC, FPGrowth, VerifyAssociationRules}
import clustering.EarthquakeKMeans.kMeansClustering
import dataconversion.mainDataConversion.labelConversion
import org.apache.spark.{HashPartitioner, Partitioner, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD


object Main {

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
    val sparkSession = SparkSession.builder()
      .appName(appName)
      .master(master)
      .getOrCreate()
      val sc = sparkSession.sparkContext
      sc.setLogLevel("WARN")


    // Load dataset

    val datasetDF = sparkSession.read
      .option("delimiter", ",")
      .option("header", value = true)
      .csv(datasetPath)


    // Run clustering and update data with cluster info

    val attributeForClustering = 3 // chose magnitude as dimension on which to perform clustering
    val numClusters = 5
    val clusteredData = kMeansClustering(sc, datasetDF, attributeForClustering, numClusters, 20, "clusteredDataMag", computeElbowMode = false)

    // Normalize data

    val normalizedData: RDD[(Int, Set[String])] = clusteredData.map(entry => (entry._1, labelConversion(entry._2)))

    val algorithms = if (simulation) List("aprioriseq", "apriorispc", "apriorimapreduce", "fpgrowth") else List(classifier)

    val times = executeAlgorithms(algorithms, sparkSession, sc, numClusters, normalizedData, outputFolder)
    writeTimesToCSV(sparkSession, times, master, datasetPath, outputFolder + "/times")

    sparkSession.stop()

    println("\nMain method completed")
  }

  def executeAlgorithms(algorithms: List[String], sparkSession: SparkSession, sc: SparkContext, numClusters: Int, normalizedData: RDD[(Int, Set[String])], outputFolder: String) : List[(String, Long)] = {
    algorithms match {
      case alg :: tail => {
        val (rules, time_elapsed) = time(alg, runAprioriForEachCluster(sc, numClusters, normalizedData, alg))
        val res = rules.reduceLeft((a,b) => a.union(b))
          .map(rule => ((rule._1, rule._2), rule._3))
          .groupByKey()
          .map(rule => (rule._1._1, rule._1._2, rule._2.head ) )

        writeAssociationRulesToCSV(sparkSession, verify(res, normalizedData), outputFolder + "/associationrules/" + alg)
        (alg, time_elapsed) :: executeAlgorithms(tail, sparkSession, sc, numClusters, normalizedData, outputFolder)
      }
      case _ => List[(String, Long)]()
    }
  }


  def runAprioriForEachCluster(sc: SparkContext, numClusters: Int, dataset: RDD[(Int, Set[String])], model: String): List[RDD[(Set[String], Set[String], Double)]] = {

    var associationRules: List[RDD[(Set[String], Set[String], Double)]] = List()

    // Run algorithm for each cluster
    for (clusterIndex <- 0 until numClusters) {
      println()
      println(s"$model - computing cluster $clusterIndex...")
      val transactions: RDD[Set[String]] = dataset.filter(_._1 == clusterIndex).map(_._2)

      model match {
        case "aprioriseq" =>
          val collectedTransaction = transactions.collect().toSeq
          val seqInstance = new AprioriSeq(collectedTransaction)
          associationRules = associationRules :+ sc.parallelize(seqInstance.run())
          //val assRulesSubset = seqInstance.run()
        case "apriorispc" =>
          val spcInstance = new AprioriSparkSPC(transactions)
          associationRules = associationRules :+ spcInstance.run()
        case "apriorimapreduce" =>
          val mapreduceInstance = new AprioriMapReduce(transactions)
          associationRules = associationRules :+ mapreduceInstance.run()
        case "fpgrowth" =>
          val fpgrowthInstance = new FPGrowth(transactions)
          associationRules = associationRules :+ fpgrowthInstance.run()
      }
    }

    associationRules
  }

  def writeAssociationRulesToCSV(sparkSession: SparkSession, rules: RDD[(Set[String], Set[String], Double)], outputFolder: String) : Unit = {

    val associationRules = rules.map {
      case (lhs, rhs, accuracy) => (lhs.mkString(", "), rhs.mkString(", "), accuracy)
    }

    sparkSession.createDataFrame(associationRules)
      .toDF("antecedent", "consequent", "accuracy")
      .coalesce(1)
      .write
      .option("header", value = true)
      .mode("overwrite")
      .csv(outputFolder)
  }

  def writeTimesToCSV(sparkSession: SparkSession, times: List[(String, Long)], master: String, datasetPath: String, outputFolder: String) : Unit = {

    val data = times.map {
      case (alg, time) => (alg, master, datasetPath.split("/").last, time)
    }

    import sparkSession.implicits._
    data.toDF("algorithm", "master", "dataset", "time elapsed (ms)")
      .coalesce(1)
      .write
      .option("header", value = true)
      .mode("append")
      .csv(outputFolder)
  }


  def time[R](label: String, block: => R): (R, Long) = {
    val t0 = System.nanoTime()
    val result = block
    val t1 = System.nanoTime()
    val time_elapsed = (t1 - t0) / 1000000 
    println()
    println(s"$label - elapsed time: " + time_elapsed + "ms")
    (result, time_elapsed)
  }
}
