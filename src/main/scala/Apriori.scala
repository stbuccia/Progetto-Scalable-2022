trait Apriori {
  var transactions: Seq[Set[String]]
  var minSupport: Int

  def run(): Any 
}
