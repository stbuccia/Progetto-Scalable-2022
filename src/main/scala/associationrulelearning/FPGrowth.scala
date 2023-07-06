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

     val transactions = List[String]()
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

