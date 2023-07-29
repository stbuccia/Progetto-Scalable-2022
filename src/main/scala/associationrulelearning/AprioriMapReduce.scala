package associationrulelearning

import org.apache.spark.rdd.RDD

class AprioriMapReduce(dataset: RDD[Set[String]]) extends Serializable with Apriori[RDD[Set[String]]] {

  override var transactions: RDD[Set[String]] = dataset
  var rdd_size: Double = transactions.count().toDouble


  override def run(): RDD[(Set[String], Set[String], Double)] = {


    val rdd_itemsets_1 = countItemsetsSize1(transactions, rdd_size, minSupport)
    var rdd_itemsets = generateAndCountItemset(transactions, rdd_itemsets_1, itemSet, rdd_size, minSupport, 2)

    var i = 3
    var stop = false
    while (i < 5 && !stop) {
      val rdd_itemsets_N = generateAndCountItemset(transactions, rdd_itemsets.filter(_._1.size == i - 1), itemSet, rdd_size, minSupport, i)
      if (rdd_itemsets_N.isEmpty) {
        stop = true
        //println("    - Empty")
      }
      else
        rdd_itemsets = rdd_itemsets.union(rdd_itemsets_N)
      i = i + 1
    }

    val frequentItemsets: RDD[(Set[String], Int)] = rdd_itemsets
    val associationRules: RDD[(Set[String], Set[String], Double)] = generateAssociationRules(rdd_itemsets, minConfidence, transactions)

    println("===Frequent Itemsets===")
    frequentItemsets.collect().sortBy(_._1.size).foreach(itemset => println(itemset._1.mkString("(", ", ", ")") + "," + itemset._2))

    println("===Association Rules===")
    associationRules.sortBy(_._3).foreach { case (lhs, rhs, confidence) =>
      println(s"${lhs.mkString(", ")} => ${rhs.mkString(", ")} (Confidence: $confidence)")
    }

    associationRules
  }


  def countItemsetsSize1(rdd: RDD[Set[String]], rdd_size: Double, min_sup: Double) = {
    //println("\nGenerate itemsets with size 1...")
    val output = rdd
      .flatMap(x => x.map(y => (Set(y), 1)))
      .reduceByKey((x, y) => x + y)
      //TODO questa count() è necessaria? si, perchè viene passato come parametro a tutte le fasi, lo calcolo solo una volta
      .filter(x => (x._2.toDouble / rdd_size) >= min_sup)
    //.collect()
    //printItemsets(output, rdd_size)
    output
  }


  def generateAndCountItemset(rdd: RDD[Set[String]],
                              rdd_itemsets: RDD[(Set[String], Int)],
                              labelSet: Set[String],
                              rdd_size: Double,
                              min_support: Double,
                              i: Int) = {
    //println("\nGenerate itemsets with size " + i + "...")

    //TODO collect() dopo countItemsetsSizeN?
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

/*
  def generateAssociationRulesFromSubset(rdd: RDD[Set[String]],
                                         itemset: Set[String],
                                         itemset_support: Int,
                                         subItemset: Set[String],
                                         confidence: Double) = {
    val rules = rdd
      .filter(x => subItemset.subsetOf(x))
      .map(x => (subItemset, 1))
      .reduceByKey((x, y) => x + y)
      .map(x => (x._1, itemset -- subItemset, itemset_support.toDouble / x._2.toDouble))
      .filter(x => x._3 >= confidence)

    //TODO è necessaria la reverseRules?
    /*
        val reverseRules = rdd
          .filter(x => (itemset -- subItemset).subsetOf(x))
          .map(x => ((itemset -- subItemset), 1))
          .reduceByKey((x, y) => x + y)
          .map(x => (x._1, itemset -- (itemset -- subItemset), itemset_support / x._2.toDouble))
          .filter(x => x._3 >= confidence)

        rules ++ reverseRules

     */
    rules
  }
*/

  def generateAssociationRules(rdd_itemsets: RDD[(Set[String], Int)],
                               confidence: Double,
                               rdd: RDD[Set[String]]) = {

    val array_subsets = rdd_itemsets
      .flatMap(itemset => {
        val rangeIncl = Range.inclusive(1, itemset._1.toArray.length - 1)
        rangeIncl.flatMap(y => itemset._1.subsets(y).toSet)
      }).collect()

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
          .sortBy(_._3)
        //.sortBy(_._3, ascending = false)
        //TODO sort
      })

    associationRules
  }

}
