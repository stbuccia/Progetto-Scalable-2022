package associationrulelearning

trait Apriori[T] {

  var itemSet : Set[String] = Set("SH", "NH", "Q1", "Q2", "Q3", "Q4", "LOW_MAG", "MED_MAG", "HIGH_MAG", "LOW_DEPTH", "MED_DEPTH", "HIGH_DEPTH")

  var transactions: T
  var minSupport: Int
  var minConfidence: Double


  def run(): Any
}
