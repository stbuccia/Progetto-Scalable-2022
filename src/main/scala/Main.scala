
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


    if (simulation) {
      val aprioriSeqRules = time(s"[apriori sequential]", runAprioriForEachCluster(sc, numClusters, normalizedData, "aprioriseq"))
      println("generated association rules are: ")
      val aprioriSeqRes = aprioriSeqRules.reduceLeft((a,b) => a.union(b).distinct())
      aprioriSeqRes.sortBy(_._3).collect().foreach(println)
      writeAssociationRulesToCSV(sparkSession, verify(aprioriSeqRes, normalizedData), outputFolder + "/AssociationRules/AprioriSeq")


      val aprioriSpcRules = time(s"[apriori single pass count]", runAprioriForEachCluster(sc, numClusters, normalizedData, "apriorispc"))
      println("generated association rules are: ")
      val aprioriSpcRes = aprioriSpcRules.reduceLeft((a,b) => a.union(b).distinct())
      aprioriSpcRes.sortBy(_._3).collect().foreach(println)
      writeAssociationRulesToCSV(sparkSession, aprioriSpcRes, outputFolder + "/AssociationRules/AprioriSPC")

      val aprioriMapRedRules = time(s"[apriori map reduce]", runAprioriForEachCluster(sc, numClusters, normalizedData, "apriorimapreduce"))
      println("generated association rules are: ")
      val aprioriMapRedRes = aprioriMapRedRules.reduceLeft((a,b) => a.union(b).distinct())
      aprioriMapRedRes.sortBy(_._3).collect().foreach(println)
      writeAssociationRulesToCSV(sparkSession, aprioriMapRedRes, outputFolder + "/AssociationRules/AprioriMapRed")

      val fpgrowthRules = time(s"[fpgrowth]", runAprioriForEachCluster(sc, numClusters, normalizedData, "fpgrowth"))
      println("generated association rules are: ")
      val fpgrowthRes = fpgrowthRules.reduceLeft((a,b) => a.union(b).distinct())
      fpgrowthRes.sortBy(_._3).collect().foreach(println)
      writeAssociationRulesToCSV(sparkSession, fpgrowthRes, outputFolder + "/AssociationRules/FPGrowth")


    } else {
      classifier match {
        case "aprioriseq" =>
          val aprioriSeqRules = time(s"[apriori sequential]", runAprioriForEachCluster(sc, numClusters, normalizedData, "aprioriseq"))
          println("generated association rules are: ")
          val aprioriSeqRes = aprioriSeqRules.reduceLeft((a,b) => a.union(b).distinct())
          aprioriSeqRes.sortBy(_._3).collect().foreach(println)
        case "apriorispc" =>
          val aprioriSpcRules = time(s"[apriori single pass count]", runAprioriForEachCluster(sc, numClusters, normalizedData, "apriorispc"))
          println("generated association rules are: ")
          val aprioriSpcRes = aprioriSpcRules.reduceLeft((a,b) => a.union(b).distinct())
          aprioriSpcRes.sortBy(_._3).collect().foreach(println)
        case "apriorimapreduce" =>
          val aprioriMapRedRules = time(s"[apriori map reduce]", runAprioriForEachCluster(sc, numClusters, normalizedData, "apriorimapreduce"))
          println("generated association rules are: ")
          val aprioriMapRedRes = aprioriMapRedRules.reduceLeft((a,b) => a.union(b).distinct())
          aprioriMapRedRes.sortBy(_._3).collect().foreach(println)
        case "fpgrowth" =>
          val fpgrowthRules = time(s"[fpgrowth]", runAprioriForEachCluster(sc, numClusters, normalizedData, "fpgrowth"))
          println("generated association rules are: ")
          val fpgrowthRes = fpgrowthRules.reduceLeft((a,b) => a.union(b).distinct())
          fpgrowthRes.sortBy(_._3).collect().foreach(println)
      }
    }

    sparkSession.stop()

    println("\nMain method completed")
  }


  def writeAssociationRulesToCSV(sparkSession: SparkSession, rules: RDD[(Set[String], Set[String], Double)], outputFolder: String) : Unit = {

    val associationRules = rules.map {
      case (lhs, rhs, confidence) => (lhs.mkString(", "), rhs.mkString(", "), confidence)
    }

    sparkSession.createDataFrame(associationRules)
    .toDF("antecedent", "consequent", "confidence")
    .coalesce(1)
    .write
    .option("header", value = true)
    .mode("overwrite")
    .csv(outputFolder)
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


  def time[R](label: String, block: => R): R = {
    val t0 = System.nanoTime()
    val result = block
    val t1 = System.nanoTime()
    println()
    println(s"$label - elapsed time: " + (t1 - t0) / 1000000 + "ms")
    result
  }
}
