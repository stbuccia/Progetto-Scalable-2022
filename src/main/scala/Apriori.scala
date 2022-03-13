class Apriori {

  /* Implementation of Sequential Apriori
    Inspired by wikipedia page and "Apriori-Map/Reduce Algorithm", J. Woo paper
   */

  private def joinSort(setL: Set[Set[String]], k: Int) = {
    setL.map(itemset1 => setL.map(itemset2 => itemset1 | itemset2))
      .reduce(_ | _)
      .filter(_.size == k)
  }

  private def prune(setC: Set[Set[String]], transactions: List[Set[String]], minSupport: Double): Set[Set[String]] = {
    setC.filter(set => transactions.count(set.subsetOf(_))  >=  minSupport)
  }

  def apriori_sequential(setL: Set[Set[String]], transactions: List[Set[String]], k: Int, minSupport: Int): Any = {
    val frequentElements = prune(joinSort(setL, k), transactions, minSupport)
    if (frequentElements.isEmpty)
      setL
    else
      apriori_sequential(frequentElements, transactions, k + 1, minSupport)
  }

}
