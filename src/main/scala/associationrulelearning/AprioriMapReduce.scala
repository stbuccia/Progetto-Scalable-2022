package associationrulelearning

import org.apache.spark.rdd.RDD

class AprioriMapReduce(dataset: RDD[Set[String]]) extends Serializable with Apriori[RDD[Set[String]]] {

  override var transactions: RDD[Set[String]] = dataset
  var rdd_size: Double = transactions.count().toDouble


  override def run(): RDD[(Set[String], Set[String], Double)] = {

    var rdd_itemsets = countItemsetsSize1(transactions, rdd_size, minSupport)

    var i = 2
    var stop = false
    while (i < 5 && !stop) {
      val rdd_itemsets_N = generateAndCountItemset(transactions, rdd_itemsets.filter(_._1.size == i - 1), itemSet, rdd_size, minSupport, i)
      if (rdd_itemsets_N.isEmpty) {
        stop = true
      }
      else
        rdd_itemsets = rdd_itemsets.union(rdd_itemsets_N)
      i = i + 1
    }

    val frequentItemsets: RDD[(Set[String], Int)] = rdd_itemsets
    val associationRules: RDD[(Set[String], Set[String], Double)] = generateAssociationRules(rdd_itemsets, minConfidence, transactions)

    associationRules
  }


  def countItemsetsSize1(rdd: RDD[Set[String]], rdd_size: Double, min_sup: Double) = {
    rdd
      .flatMap(x => x.map(y => (Set(y), 1)))
      .reduceByKey((x, y) => x + y)
      .filter(x => (x._2.toDouble / rdd_size) >= min_sup)
  }


  def generateAndCountItemset(rdd: RDD[Set[String]],
                              rdd_itemsets: RDD[(Set[String], Int)],
                              labelSet: Set[String],
                              rdd_size: Double,
                              min_support: Double,
                              i: Int) = {

    val array_itemsetsSizeN = rdd_itemsets
      .map(_._1)
      .flatMap(x => generateItemsetsSizeN(x, labelSet))
      .collect().toSet
    val output = countItemsetsSizeN(rdd, array_itemsetsSizeN, rdd_size, min_support)

    output
  }


  def generateItemsetsSizeN(itemset: Set[String], labelSet: Set[String]) = {
      var output = labelSet
      for (item <- itemset) {
        item match {
          case "NH" | "SH" => output = output -- Set("NH", "SH")
          case "Q1" | "Q2" | "Q3" | "Q4" => output = output -- Set("Q1", "Q2", "Q3", "Q4")
          case "LOW_MAG" | "MED_MAG" | "HIGH_MAG" => output = output -- Set("LOW_MAG", "MED_MAG", "HIGH_MAG")
          case "LOW_DEPTH" | "MED_DEPTH" | "HIGH_DEPTH" => output = output -- Set("LOW_DEPTH", "MED_DEPTH", "HIGH_DEPTH")
        }
      }
      output.map(x => itemset + x)
  }


  def countItemsetsSizeN(rdd: RDD[Set[String]], itemsets: Set[Set[String]], rdd_size: Double, min_sup: Double) = {
    rdd
      .flatMap(rdd_row =>
        itemsets
          .filter(itemset => itemset.subsetOf(rdd_row))
          .map(itemset => (itemset, 1))
      )
      .reduceByKey((x, y) => x + y)
      .filter(x => (x._2.toDouble / rdd_size) >= min_sup)
  }



  def generateAssociationRules(rdd_itemsets: RDD[(Set[String], Int)],
                               confidence: Double,
                               rdd: RDD[Set[String]]) = {

    val array_subsets = rdd_itemsets
      .flatMap(itemset => itemset._1.subsets().toSet.filter(_.nonEmpty) )
      .distinct()
      .collect()

    val array_subsets_pair = rdd
      .flatMap(rdd_row => {
        array_subsets
          .filter(subset => subset.subsetOf(rdd_row))
          .map(subset => (subset, 1))
      })
      .reduceByKey((x, y) => x + y)
      .collect()

    val associationRules = rdd_itemsets
      .flatMap(itemset => {
        array_subsets_pair
          .filter(subset_pair => subset_pair._1.subsetOf(itemset._1) && subset_pair._1.size < itemset._1.size)
          .map(subset_pair => (subset_pair._1, itemset._1 -- subset_pair._1, itemset._2.toDouble / subset_pair._2.toDouble))
          .filter(rule => rule._3 >= confidence)
      })

    associationRules
  }

}
