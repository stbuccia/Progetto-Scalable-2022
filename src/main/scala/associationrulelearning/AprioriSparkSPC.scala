package associationrulelearning

import org.apache.spark.rdd.RDD
import scala.annotation.tailrec


class AprioriSparkSPC(dataset: RDD[Set[String]]) extends AprioriSpark(dataset) {

  @tailrec
  private def recursivePhase2(transactionsRdd: RDD[Set[String]], k: Int, setL: RDD[(Set[String], Int)]): RDD[(Set[String], Int)] = {
    val setL_k = phase2(transactionsRdd, k, setL)
    if (setL_k.isEmpty()) {
      setL
    } else {
      recursivePhase2(transactionsRdd, k + 1, setL.union(setL_k))
    }
  }


  private def generateAssociationRules(frequentItemsets: Set[(Set[String], Int)], minConfidence: Double): List[(Set[String], Set[String], Double)] = {

    val frequentItemsetsList = frequentItemsets.toList

    val associationRules = frequentItemsets.flatMap { case (itemset, support) =>
      val subsets = itemset.subsets().toList.filter(_.nonEmpty)//.filter(_.size == itemset.size - 1)
      subsets.map { subset =>
        val remaining = itemset -- subset
        val confidence = support.toDouble / frequentItemsetsList.filter(_._1 == subset).map(_._2).head
        (subset, remaining, confidence)
      }
    }
    // Filter rules based on confidence
    associationRules.filter(_._2.nonEmpty).filter(_._3 >= minConfidence).toList

  }

  override def run() = {
    //TODO: Capire perché dà questi problemi se aumentiano a più nodi (controllare shuffling e partitioning sulle slide)

    val transactionsRdd = (transactions)

    val setL_1 = phase1(transactionsRdd)  // returns a set with only minSupport entries
    val setL_2 = setL_1.union(phase2(transactionsRdd, 2, setL_1))

    frequentItemsets = recursivePhase2(transactionsRdd, 3, setL_2).collect().toSet
    associationRules = generateAssociationRules(frequentItemsets, minConfidence)

    printResults()

  }
}
