package associationrulelearning

import org.apache.spark.rdd.RDD

/**
 * Class to be used as superclass for the three version of Apriori defined in the paper
 * @param dataset
 *
 * todo: togliere abstract e implementare correttamente nel caso si realizzino anche gli altri due apriori
 */
abstract class AprioriSpark(dataset: RDD[Set[String]]) extends java.io.Serializable with Apriori[RDD[Set[String]]] {

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
    val setC_k = setL.flatMap(pair => generateItemsetsSizeN(pair._1)).distinct().collect()
    val setL_k = transactionsRdd
      .flatMap(transaction =>
        setC_k.filter(itemsetC => itemsetC.subsetOf(transaction))
          .map(itemsetC => (itemsetC, 1))
      )
      .reduceByKey((x, y) => x + y)
      .filter(item => item._2 > minSupport)

    setL_k
  }

  def generateItemsetsSizeN(candidateItemset: Set[String]) = {
    //val labelSet : Set[String] = Set("SH", "NH", "Q1", "Q2", "Q3", "Q4", "LOW_MAG", "MED_MAG", "HIGH_MAG", "LOW_DEPTH", "MED_DEPTH", "HIGH_DEPTH")
    var output: Set[String] = itemSet
    for (item <- candidateItemset) {
      item match {
        case "NH" | "SH" => output = output -- Set("NH", "SH")
        case "Q1" | "Q2" | "Q3" | "Q4" => output = output -- Set("Q1", "Q2", "Q3", "Q4")
        case "LOW_MAG" | "MED_MAG" | "HIGH_MAG" => output = output -- Set("LOW_MAG", "MED_MAG", "HIGH_MAG")
        case "LOW_DEPTH" | "MED_DEPTH" | "HIGH_DEPTH" => output = output -- Set("LOW_DEPTH", "MED_DEPTH", "HIGH_DEPTH")
      }
    }
    output.map(x => candidateItemset + x)
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


  //def run() = {
    /*
    //TODO: Capire perché dà questi problemi se aumentiano a più nodi (controllare shuffling e partitioning sulle slide)
    val conf = new SparkConf().setAppName("apriori-sequential").setMaster("local[2]")
    conf.set("spark.driver.allowMultipleContexts","true");
    val sc = new SparkContext(conf)
    val transactionsRdd = sc.parallelize(transactions)

    val setL_1 = phase1(transactionsRdd)

    val out = phase2(transactionsRdd, 2, setL_1)
    out.collect().foreach(println)
    */

  //}
}
