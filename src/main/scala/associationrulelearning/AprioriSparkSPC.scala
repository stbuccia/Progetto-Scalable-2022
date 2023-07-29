package associationrulelearning

import org.apache.spark.rdd.RDD
import scala.annotation.tailrec


class AprioriSparkSPC(dataset: RDD[Set[String]]) extends java.io.Serializable with Apriori[RDD[Set[String]]] {

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


  def candidateExistsInTransaction(candidate: Set[String], transaction: Set[String]): Boolean = {
    // all elements in candidate exist in transaction
    var result = true
    for (elem <- candidate) {
      if (!transaction.contains(elem))
        result = false
    }
    result
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

  override def run(): RDD[(Set[String], Set[String], Double)] = {
    //TODO: Capire perché dà questi problemi se aumentiano a più nodi (controllare shuffling e partitioning sulle slide)


    val transactionsRdd = (transactions)

    val setL_1 = phase1(transactionsRdd)  // returns a set with only minSupport entries
    val setL_2 = setL_1.union(phase2(transactionsRdd, 2, setL_1))

    val frequentItemsets: RDD[(Set[String], Int)] = recursivePhase2(transactionsRdd, 3, setL_2)
    val associationRules: RDD[(Set[String], Set[String], Double)] = generateAssociationRules(frequentItemsets, minConfidence)

    println("===Frequent Itemsets===")
    frequentItemsets.collect().sortBy(_._1.size).foreach(itemset => println(itemset._1.mkString("(", ", ", ")") + "," + itemset._2))

    println("===Association Rules===")
    associationRules.foreach { case (lhs, rhs, confidence) =>
      println(s"${lhs.mkString(", ")} => ${rhs.mkString(", ")} (Confidence: $confidence)")
    }

    associationRules

  }
}
