
package ammonite
package $file.src
import _root_.ammonite.interp.api.InterpBridge.{
  value => interp
}
import _root_.ammonite.interp.api.InterpBridge.value.{
  exit,
  scalaVersion
}
import _root_.ammonite.interp.api.IvyConstructor.{
  ArtifactIdExt,
  GroupIdExt
}
import _root_.ammonite.compiler.CompilerExtensions.{
  CompilerInterpAPIExtensions,
  CompilerReplAPIExtensions
}
import _root_.ammonite.runtime.tools.{
  browse,
  grep,
  time,
  tail
}
import _root_.ammonite.compiler.tools.{
  desugar,
  source
}
import _root_.mainargs.{
  arg,
  main
}
import _root_.ammonite.repl.tools.Util.{
  PathRead
}


object `apriori-sequential`{
/*<script>*/import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

import scala.annotation.tailrec

val transactions = List(
  Set("A", "B", "C", "D"),
  Set("A" ,"B", "D"), 
  Set("A", "B"), 
  Set("B", "C", "D"), 
  Set("C", "D"), 
  Set("B", "D"), 
)

val conf = new SparkConf().setAppName("apriori-sequential").setMaster("local[6]")
/*<amm>*/val res_5 = /*</amm>*/conf.set("spark.driver.allowMultipleContexts","true");
val sc = new SparkContext(conf)


def phase(
  transactionsRdd: org.apache.spark.rdd.RDD[Set[String]],
  k: Int,
  minimum: Int,
  setL: org.apache.spark.rdd.RDD[(Set[String], Int)],
): org.apache.spark.rdd.RDD[Set[String]] = {

  val setL_strings = setL.map( _._1)
  val setC_k = setL_strings.cartesian(setL_strings).map(tuples => tuples._1 | tuples._2).filter(_.size == k).distinct()
  val setL_k = setC_k.cartesian(transactionsRdd).filter(tuple => tuple._1.subsetOf(tuple._2)).map(tuple => (tuple._1, 1)).reduceByKey((x, y) => x + y).filter(item => item._2 > 2) //TODO metti minimum

  //if (k == 2)
    //setC_k
    ////setL_k.collect().foreach(println)
  if (setL_k.count() == 0)
    setC_k
  else 
    phase(transactionsRdd, k + 1, minimum, setL_k)
  
}
//val itemsWithOne = transactionsRdd.map(t => (t, 1))

val transactionsRdd = sc.parallelize(transactions)

val setL_1 = transactionsRdd.flatMap(itemset => itemset.map(item => (Set(item), 1))).reduceByKey((x, y) => x + y).filter(item => item._2 > 2)
val setL_1_strings = setL_1.map( _._1)
val setC_2 = setL_1_strings.cartesian(setL_1_strings).map(tuples => tuples._1 | tuples._2).filter(_.size == 2).distinct()
val setL_2 = setC_2.cartesian(transactionsRdd).filter(tuple => tuple._1.subsetOf(tuple._2)).map(tuple => (tuple._1, 1)).reduceByKey((x, y) => x + y).filter(item => item._2 > 2)

val setL_2_strings = setL_2.map(_._1)
val setC_3 = setL_2_strings.cartesian(setL_2_strings)//.map(tuples => tuples._1 | tuples._2).filter(_.size == 2).distinct()
//val setL_3 = setC_3.cartesian(transactionsRdd).filter(tuple => tuple._1.subsetOf(tuple._2)).map(tuple => (tuple._1, 1)).reduceByKey((x, y) => x + y).filter(item => item._2 > 2)

//setC_3.collect().foreach(println)
//val l_2 = transactionsRdd.map(itemset => c_k.filter(tuple => tuple.subsetOf(itemset)).map(tuple => (tuple, 1))).reduceByKey((x, y) => x + y).filter(item => item._2 > 2)

val out = phase(transactionsRdd, 2, 2, setL_1)
//out.collect().foreach(println)
  /*
val itemsWithOne = transactionsRdd.map(t => t.map(s => (s,1))).reduce((counted, itemset) => itemset)
itemsWithOne.take(15).foreach(println)*/
//itemsWithOne.reduce
/*</script>*/ /*<generated>*/
def $main() = { scala.Iterator[String]() }
  override def toString = "apriori$minussequential"
  /*</generated>*/
}
