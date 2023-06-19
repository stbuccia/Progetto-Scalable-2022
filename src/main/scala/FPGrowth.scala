import org.apache.spark.{SparkConf, SparkContext}
//import org.apache.spark.mllib.clustering.KMeans
//import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.ml.fpm.{FPGrowth, FPGrowthModel}

import scala.io.Source


object FPGrowth {

    def main(args: Array[String]): Unit = {
    // Set up Spark session
    val spark = SparkSession.builder()
      .appName("FPGrowthExample")
      .master("local[1]")
      .getOrCreate()

    val csvPath = "/home/stefano/IdeaProjects/Progetto-Scalable-2022/src/main/resources/dataset_2010_2021_dataConversion_label.csv"
    val lines = Source.fromFile(csvPath).getLines()
    //val transactions: List[Set[String]] = lines.map(_.split(",").toSet).toList

    val df = spark.read.format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(csvPath)

    val transactions: List[Set[String]] = lines.map(_.split(",").toSet).toList
    //val transactions: List[Set[String]] = lines.map { line =>
      //line.split(",").zipWithIndex.flatMap { case (value, index) =>
        //if (value == "1")
          //index match {
            //case 0 => Some("nh")  
            //case 1 => Some("sh")  
            //case 2 => Some("quad1")  
            //case 3 => Some("quad2")  
            //case 4 => Some("quad3")  
            //case 5 => Some("quad4")  
            //case 6 => Some("low_mag")  
            //case 7 => Some("med_mag")  
            //case 8 => Some("high_mag")  
            //case 9 => Some("low_depth")  
            //case 10 => Some("med_depth")  
            //case 11 => Some("high_depth")  
          //}
        //else 
          //None
      //}.toSet
    //}.toList



    // Convert the RDD of transactions to DataFrame
    import spark.implicits._
    val transactionsDF: DataFrame = transactions.toDF("items")

    // Apply the Apriori algorithm
    val minSupport = 0.6 // Minimum support threshold
    val fpGrowth = new FPGrowth()
      .setItemsCol("items")
      .setMinSupport(minSupport)
    val model: FPGrowthModel = fpGrowth.fit(transactionsDF)

    // Get the frequent itemsets
    val frequentItemsets = model.freqItemsets

    // Get the association rules
    val minConfidence = 0.7 // Minimum confidence threshold
    val associationRules = model.associationRules
      .where($"confidence" >= minConfidence)

    // Show the frequent itemsets and association rules
    frequentItemsets.show(false)
    associationRules.show(false)

    // Stop the Spark session
    spark.stop()
  }

}

