package associationrulelearning

import org.apache.spark.rdd.RDD
import scala.annotation.tailrec


class AprioriTailRec(dataset: RDD[Set[String]]) extends java.io.Serializable with Apriori[RDD[Set[String]]] {

  var transactions: RDD[Set[String]] = dataset
  var minSupportCount: Int = (minSupport * transactions.count()).toInt


  /**
   * Generates 1-dimensional frequent itemsets for transactionsRdd
   *
   * @param transactionsRdd dataset where to look for frequent itemsets
   * @return  an RDD of 1-dim frequent itemsets together with their support
   */
  protected def phase1(transactionsRdd: RDD[Set[String]]) = {
    transactionsRdd.flatMap(itemset => itemset.map(item => (Set(item), 1))).reduceByKey((x, y) => x + y).filter(item => item._2 > minSupportCount)
  }


  protected def phase2(transactionsRdd: RDD[Set[String]], k: Int, setL: RDD[(Set[String], Int)]): RDD[(Set[String], Int)] = {

    val setL_strings = setL.map(_._1)

    val setC_k = setL_strings.cartesian(setL_strings)
      .map(tuples => tuples._1 | tuples._2)
      .filter(_.size == k)
      .distinct()
      .collect()

    val setL_k = transactionsRdd
      .flatMap(transaction =>
        setC_k.filter(itemsetC => itemsetC.subsetOf(transaction))
          .map(itemsetC => (itemsetC,1))
      )
      .reduceByKey((x, y) => x + y)
      .filter(item => item._2 > minSupportCount)

    setL_k
  }

  @tailrec
  private def recursivePhase2(transactionsRdd: RDD[Set[String]], k: Int, setL: RDD[(Set[String], Int)]): RDD[(Set[String], Int)] = {
    if (k > 4)
      return setL

    val setL_k = phase2(transactionsRdd, k, setL)
    if (setL_k.isEmpty()) {
      setL
    } else {
      recursivePhase2(transactionsRdd, k + 1, setL.union(setL_k))
    }
  }

  private def generateAssociationRules(frequentItemsets: RDD[(Set[String], Int)], minConfidence: Double): RDD[(Set[String], Set[String], Double)] = {

    val frequentItemsetsList = frequentItemsets.collect().toList

    val associationRules = frequentItemsets.flatMap { case (itemset, support) =>
      val subsets = itemset.subsets().toList.filter(_.nonEmpty)
      subsets.map { subset =>
        val remaining = itemset -- subset
        val confidence = support.toDouble / frequentItemsetsList.filter(_._1 == subset).map(_._2).head
        (subset, remaining, confidence)
      }
    }
    // Filter rules based on confidence
    associationRules.filter(_._2.nonEmpty).filter(_._3 >= minConfidence)

  }

  override def run(): RDD[(Set[String], Set[String], Double)] = {
    val transactionsRdd = (transactions)

    val setL_1 = phase1(transactionsRdd) 

    val frequentItemsets: RDD[(Set[String], Int)] = recursivePhase2(transactionsRdd, 2, setL_1)
    val associationRules: RDD[(Set[String], Set[String], Double)] = generateAssociationRules(frequentItemsets, minConfidence)

    associationRules

  }
}
