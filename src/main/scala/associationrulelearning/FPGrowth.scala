import org.apache.spark.{SparkConf, SparkContext}
//import org.apache.spark.mllib.clustering.KMeans
//import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession, DataFrame}
//import org.apache.spark.ml.fpm.{FPGrowth, FPGrowthModel}
import org.apache.spark.mllib.fpm.FPGrowth
import org.apache.spark.sql.types.{StringType, StructField, StructType}

import scala.io.Source


class FPGrowth( t: RDD[Set[String]], m: Double, n: Double) {

    var transactions: RDD[Set[String]] = t
    var minSupport: Double = m 
    var minConfidence: Double = n

    def run(): Unit = {
        //
        //Convert RDD[Set[String]] in RDD[Array[String]]
        val transactionsRDD: RDD[Array[String]] = transactions.map(_.toArray)

        val fpg = new org.apache.spark.mllib.fpm.FPGrowth()
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

