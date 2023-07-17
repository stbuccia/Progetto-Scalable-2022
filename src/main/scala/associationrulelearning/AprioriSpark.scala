package associationrulelearning

import org.apache.spark.rdd.RDD


class AprioriSpark(dataset: RDD[Set[String]]) extends java.io.Serializable with Apriori[RDD[Set[String]]] {

  var transactions: RDD[Set[String]] = dataset
  var minSupportCount: Int = (minSupport * transactions.count()).toInt


  protected def phase1(transactionsRdd: RDD[Set[String]]) = {
    transactionsRdd.flatMap(itemset => itemset.map(item => (Set(item), 1))).reduceByKey((x, y) => x + y).filter(item => item._2 > minSupport)
  }


  protected def phase2(transactionsRdd: RDD[Set[String]], k: Int, setL: RDD[(Set[String], Int)]): RDD[(Set[String], Int)] = {
    val setL_strings = setL.map(_._1)

    val setC_k = setL_strings.cartesian(setL_strings)
      .map(tuples => tuples._1 | tuples._2)
      .filter(_.size == k)
      .distinct()

    val setL_k = setC_k.cartesian(transactionsRdd)
      .filter(tuple => tuple._1.subsetOf(tuple._2))
      .map(tuple => (tuple._1, 1))
      .reduceByKey((x, y) => x + y)
      .filter(item => item._2 > minSupport) 

    setL_k
  }


  def run() = {
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
  }
}
