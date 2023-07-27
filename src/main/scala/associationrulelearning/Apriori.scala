package associationrulelearning

trait Apriori[T] {

  var itemSet : Set[String] = Set("SH", "NH", "Q1", "Q2", "Q3", "Q4", "LOW_MAG", "MED_MAG", "HIGH_MAG", "LOW_DEPTH", "MED_DEPTH", "HIGH_DEPTH")

  var transactions: T
  var minSupport: Double = 0.6
  var minConfidence: Double = 0.7

  var frequentItemsets: Set[(Set[String], Int)] = Set()
  var associationRules: List[(Set[String], Set[String], Double)] = List()

  def run(): Any

  def printResults(): Unit = {

    println("===Frequent Itemsets===")
    frequentItemsets.toArray.sortBy(_._1.size).foreach(itemset => println(itemset._1.mkString("(", ", ", ")") + "," + itemset._2))

    println("===Association Rules===")
    associationRules.foreach { case (lhs, rhs, confidence) =>
      println(s"${lhs.mkString(", ")} => ${rhs.mkString(", ")} (Confidence: $confidence)")
    }
  }
}
