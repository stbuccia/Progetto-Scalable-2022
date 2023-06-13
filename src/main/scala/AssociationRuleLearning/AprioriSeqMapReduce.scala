package AssociationRuleLearning


/* Implementation of Sequential AssociationRuleLearning.Apriori
  Inspired by wikipedia page and "AssociationRuleLearning.Apriori-Map/Reduce Algorithm", J. Woo paper
 */

class AprioriSeqMapReduce(t: List[Set[String]], m: Int, n: Int) extends java.io.Serializable with Apriori {

  var transactions: Seq[Set[String]] = t
  var minSupport: Int = m
  var minConfidence: Int = n

  // joinSort(HashSet(Set(1), Set(2), Set(3), Set(4)), 1)
  //    => HashSet(Set(3), Set(1), Set(4), Set(2))
  //
  // joinSort(HashSet(Set(3), Set(1), Set(4), Set(2)), 2) 
  //    => HashSet(Set(4, 2), Set(1, 3), Set(4, 3), Set(2, 3), Set(1, 4), Set(1, 2))
  //
  // joinSort(HashSet(Set(4, 2), Set(4, 3), Set(2, 3), Set(1, 4), Set(1, 2), 3) 
  //    => HashSet(Set(1, 2, 4), Set(1, 2, 3), Set(1, 4, 3), Set(4, 3, 2))
  //
  // joinSort(HashSet(Set(1, 2, 4), Set(4, 3, 2), 4) 
  //    => HashSet(Set(4, 3, 2, 1))

  private def joinSort(setL: Set[Set[String]], k: Int) = {

    setL.map(itemset1 => setL.map(itemset2 => itemset1 | itemset2)) //In itemset2 there is also empty set
      .reduce(_ | _) // Convert set of set in a single set where equal are taken once
      .filter(_.size == k) //remove results of unions with empty set
  }

  private def prune(setC: Set[Set[String]]): Set[Set[String]] = {
    setC.filter(set => transactions.count(set.subsetOf(_)) >= minSupport)
  }
  
  def apriori_sequential(setL: Set[Set[String]], k: Int): Set[Set[String]] = {
    val frequentElements = prune(joinSort(setL, k))
    if (frequentElements.isEmpty)
      setL
    else
      apriori_sequential(frequentElements, k + 1)
  }

  def run() = {
    val setC = transactions.reduce(_ | _).map(Set(_))
    apriori_sequential(setC, 1)
  }
}
