import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.rdd.RDD
import scala.annotation.tailrec
import com.fasterxml.jackson.module.scala.deser.overrides

class AprioriSparkSPC(t: List[Set[String]], m: Int) extends AprioriSpark(t, m) {

  @tailrec
  private def recursivePhase2(transactionsRdd: RDD[Set[String]], k: Int, setL: RDD[(Set[String], Int)]): RDD[(Set[String], Int)] = {
    val setL_k = phase2(transactionsRdd, k, setL)
    if (setL_k.count() == 0)
      setL
    else 
      recursivePhase2(transactionsRdd, k + 1, setL.union(setL_k))
  }

  override def run() = {
    //TODO: Capire perché dà questi problemi se aumentiano a più nodi (controllare shuffling e partitioning sulle slide)
    val conf = new SparkConf().setAppName("apriori-sequential").setMaster("local[1]")
    conf.set("spark.driver.allowMultipleContexts","true");
    val sc = new SparkContext(conf)
    val transactionsRdd = sc.parallelize(transactions)

    val setL_1 = phase1(transactionsRdd)
    val setL_2 = setL_1.union(phase2(transactionsRdd, 2, setL_1))

    val out = recursivePhase2(transactionsRdd, 3, setL_2)
    out.collect().foreach(println)
    sc.stop()
  }
}
