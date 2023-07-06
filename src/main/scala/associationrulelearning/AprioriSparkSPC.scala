package associationrulelearning

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.rdd.RDD
import scala.annotation.tailrec
import com.fasterxml.jackson.module.scala.deser.overrides

class AprioriSparkSPC(t: RDD[Set[String]], m: Double, n: Double) extends AprioriSpark(t, m, n) {

  @tailrec
  private def recursivePhase2(transactionsRdd: RDD[Set[String]], k: Int, setL: RDD[(Set[String], Int)]): RDD[(Set[String], Int)] = {
    val setL_k = phase2(transactionsRdd, k, setL)
    if (setL_k.count() == 0)
      setL
    else
      recursivePhase2(transactionsRdd, k + 1, setL.union(setL_k))
  }

  private def generateAssociationRules(frequentItemsets: RDD[(Set[String], Int)], minConfidence: Double): RDD[(Set[String], Set[String], Double)] = {
    val frequentItemsetsList = frequentItemsets.collect()

    val associationRules = frequentItemsets.flatMap { case (itemset, support) =>
      val subsets = itemset.subsets().toList.filter(_.nonEmpty)//.filter(_.size == itemset.size - 1)
      subsets.map { subset =>
        val remaining = itemset -- subset
        val confidence = support.toDouble / frequentItemsetsList.filter(_._1 == subset).map(_._2).head
        (subset, remaining, confidence)
      }
    }
    // Filter rules based on confidence
    associationRules.filter(_._2.nonEmpty).filter(_._3 >= minConfidence)
  }

  override def run() = {
    //TODO: Capire perché dà questi problemi se aumentiano a più nodi (controllare shuffling e partitioning sulle slide)

    //val conf = new SparkConf().setAppName("apriori-sequential").setMaster("local[1]")
    //conf.set("spark.driver.allowMultipleContexts","true");
    //val sc = new SparkContext(conf)
    //val transactionsRdd = sc.parallelize(transactions)
    val transactionsRdd = (transactions)

    val setL_1 = phase1(transactionsRdd)
    val setL_2 = setL_1.union(phase2(transactionsRdd, 2, setL_1))

    val out = recursivePhase2(transactionsRdd, 3, setL_2)
    out.collect().foreach(println)


    val frequentItemsets: RDD[(Set[String], Int)] = out // Your RDD of frequent itemsets
    val minConfidence: Double = 0.7 // Minimum confidence threshold

    val associationRules: RDD[(Set[String], Set[String], Double)] = generateAssociationRules(frequentItemsets, minConfidence)

    // Print the association rules
    associationRules.foreach { case (lhs, rhs, confidence) =>
      println(s"${lhs.mkString(", ")} => ${rhs.mkString(", ")} (Confidence: $confidence)")
    }

    //sc.stop()
  }
}
