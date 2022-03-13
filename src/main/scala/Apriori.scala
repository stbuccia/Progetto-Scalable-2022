/* Implementation of Sequential Apriori
  Inspired by wikipedia page and "Apriori-Map/Reduce Algorithm", J. Woo paper
 */

class Apriori(t: List[Set[String]], m: Int) {

  var transactions: Seq[Set[String]] = t
  var minSupport: Int = m

  private def joinSort(setL: Set[Set[String]], k: Int) = {
    setL.map(itemset1 => setL.map(itemset2 => itemset1 | itemset2))
      .reduce(_ | _)
      .filter(_.size == k)
  }

  private def prune(setC: Set[Set[String]]): Set[Set[String]] = {
    setC.filter(set => transactions.count(set.subsetOf(_))  >=  minSupport)
  }

  def apriori_sequential(setL: Set[Set[String]], k: Int): Any = {
    val frequentElements = prune(joinSort(setL, k))
    if (frequentElements.isEmpty)
      setL
    else
      apriori_sequential(frequentElements, k + 1)
  }

  def run(apriori_fun: (Set[Set[String]], Int) => Any): Any = {
    val setC = transactions.reduce(_ | _).map(Set(_))
    apriori_fun(setC, 1)
  }
}
