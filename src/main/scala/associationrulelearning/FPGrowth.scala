import org.apache.spark.{SparkConf, SparkContext}
//import org.apache.spark.mllib.clustering.KMeans
//import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession, DataFrame}
//import org.apache.spark.ml.fpm.{FPGrowth, FPGrowthModel}
import org.apache.spark.mllib.fpm.FPGrowth
import org.apache.spark.sql.types.{StringType, StructField, StructType}

import scala.io.Source


class FPGrowthAssociation( t: RDD[Set[String]], m: Double, n: Double) {

    var transactions: RDD[Set[String]] = t
    var minSupport: Double = m 
    var minConfidence: Double = n

    def run(): Unit = {
        // Set up Spark session
        //val spark = SparkSession.builder()
        //.appName("FPGrowthExample")
        //.master("local[1]")
        //.getOrCreate()

        // Convert the RDD of transactions to DataFrame
        //import spark.implicits._
        //val transactionsDF: DataFrame = transactions.toDF("items")

        // Define the schema for the DataFrame
        //val schema = StructType(Array(
            //StructField("hemisphere", StringType, nullable = false),
            //StructField("quadrant", StringType, nullable = false),
            //StructField("magnitude", StringType, nullable = false),
            //StructField("depth", StringType, nullable = false)
        //))

        //// Convert RDD[Set[String]] to RDD[Row]
        //val rowRDD: RDD[Row] = transactions.map(set => Row(set.mkString(",")))

        //// Create DataFrame from RDD[Row] and schema
        //val df = spark.createDataFrame(rowRDD, schema)


        //// Apply the Apriori algorithm
        //val fpGrowth = new FPGrowth().setItemsCol("items").setMinSupport(minSupport)

        //val model: FPGrowthModel = fpGrowth.fit(df)

        //// Get the frequent itemsets
        //val frequentItemsets = model.freqItemsets

        //// Get the association rules
        //val associationRules = model.associationRules.where($"confidence" >= minConfidence)

        //// Show the frequent itemsets and association rules
        //frequentItemsets.show(false)
        //associationRules.show(false)

        //// Stop the Spark session
        //spark.stop()

        //Convert RDD[Set[String]] in RDD[Array[String]]
        val transactionsRDD: RDD[Array[String]] = transactions.map(_.toArray)

        val fpg = new FPGrowth()
            .setMinSupport(minSupport)
            .setNumPartitions(10)

        val model = fpg.run(transactionsRDD)

        model.freqItemsets.collect().foreach { itemset =>
            println(itemset.items.mkString("[", ",", "]") + ", " + itemset.freq)
        }

        model.generateAssociationRules(minConfidence).collect().foreach { rule =>
            println(
                rule.antecedent.mkString("[", ",", "]")
                + " => " + rule.consequent .mkString("[", ",", "]")
                + ", " + rule.confidence)
        }
    }

}

