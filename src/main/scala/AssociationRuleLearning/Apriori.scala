package AssociationRuleLearning

trait Apriori {
  var transactions: Seq[Set[String]]
  var minSupport: Int
  var minConfidence: Int

  def run(): Any
}
