package AssociationRuleLearning

trait Apriori {
  var transactions: Seq[Set[String]]
  var minSupport: Int
  var minConfidence: Double

  var generatedItemsets: Map[Set[String], Int]

  var frequentItemsets: Set[Set[String]]
  var associationRules : List[(Set[String], Set[String], Double)]

  def run(): Any
}
