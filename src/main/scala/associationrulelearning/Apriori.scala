package associationrulelearning
import org.apache.spark.rdd.RDD

trait Apriori {

  var itemSet : Set[String] = Set("SH", "NH", "Q1", "Q2", "Q3", "Q4", "LOW_MAG", "MED_MAG", "HIGH_MAG", "LOW_DEPTH", "MED_DEPTH", "HIGH_DEPTH")
  var transactions: RDD[Set[String]]
  var minSupport: Int
  var minConfidence: Double

  //var generatedItemsets: Map[Set[String], Int]

  //var frequentItemsets: Set[Set[String]]
  //var associationRules : List[(Set[String], Set[String], Double)]

  def run(): Any
}
