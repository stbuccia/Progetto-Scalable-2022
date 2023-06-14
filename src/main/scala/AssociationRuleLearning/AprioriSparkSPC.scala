package AssociationRuleLearning

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.annotation.tailrec

/**
 *
 * @param t
 * @param m
 * @param n
 *
 * todo: togliere abstract e implementare correttamente il trait
 */
abstract class AprioriSparkSPC(t: List[Set[String]], m: Int, n: Double) extends AprioriSpark(t, m, n) {

  @tailrec
  private def recursivePhase2(transactionsRdd: RDD[Set[String]], k: Int, setL: RDD[(Set[String], Int)]): RDD[(Set[String], Int)] = {
    val setL_k = phase2(transactionsRdd, k, setL)
    if (setL_k.count() == 0)
      setL
    else 
      recursivePhase2(transactionsRdd, k + 1, setL_k)
  }

  override def run() = {
    //TODO: Capire perché dà questi problemi se aumentiano a più nodi (controllare shuffling e partitioning sulle slide)
    val conf = new SparkConf().setAppName("apriori-sequential").setMaster("local[1]")
    conf.set("spark.driver.allowMultipleContexts","true");
    val sc = new SparkContext(conf)
    val transactionsRdd = sc.parallelize(transactions)

    val setL_1 = phase1(transactionsRdd)
    val setL_2 = phase2(transactionsRdd, 2, setL_1)

    val out = recursivePhase2(transactionsRdd, 3, setL_2)
    out.collect().foreach(println)
  }
}
