package associationrulelearning

trait Apriori {

  var itemSet : Set[String] = Set("SH", "NH", "Q1", "Q2", "Q3", "Q4", "LOW_MAG", "MED_MAG", "HIGH_MAG", "LOW_DEPTH", "MED_DEPTH", "HIGH_DEPTH")

  var transactions: Seq[Set[String]]
  var minSupport: Int
  var minConfidence: Double

  var frequentItemsets: Set[Set[String]]
  var associationRules : List[(Set[String], Set[String], Double)]

  def run(): Any
}
