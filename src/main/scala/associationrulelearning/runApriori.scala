package associationrulelearning

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext


/**
 * This object runs the choosen intance of Apriori algorithm on the given dataset.
 * The dataset has to be saved in a file, given as input to this object.
 * When running, dataset, minimum support and minimum confidence must be provided following this order.
 *
 * todo: fai la conversione in Int usando un Option Type
 */
object runApriori {

  def runAprioriSeq(sc: SparkContext, dataset: RDD[Set[String]]) : Unit = {

    // Creates an algorithm instance
    val alg = new AprioriSeq(dataset, 0.6, 0.7)

    println("Algorithm instance created. Going to run")
    alg.run()

    // Prints Frequent Itemsets and Association Rules generated by the algorithm
    println("===Frequent Itemsets===")
    alg.frequentItemsets.toArray.sortBy(_.size).foreach(println)
    println("===Association Rules===")
    alg.associationRules.foreach{case(lhs, rhs, confidence) =>
      println(s"${lhs.mkString(", ")} => ${rhs.mkString(", ")} (Confidence: $confidence)")
    }

  }

}
