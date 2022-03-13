import scala.annotation.tailrec

/* Implementation of Sequential Apriori
  Inspired by wikipedia page and "Apriori-Map/Reduce Algorithm", J. Woo paper
 */

val transactions = List(Set("A", "B", "C"), Set("A" ,"C"), Set("B", "C", "D", "E"), Set("A", "D"), Set("E"), Set("C", "D", "E", "F", "G" ))
val setC = Set(Set("A"), Set("B"), Set("C"), Set("D"), Set("E"), Set("F"), Set("G"))


def joinSort(setL: Set[Set[String]], k: Int) = {
  setL.map(itemset1 => setL.map(itemset2 => itemset1 | itemset2))
    .reduce(_ | _) //riporta i set al livello precedente (siamo dentro due map)
    .filter(_.size == k)
}

def prune(setC: Set[Set[String]], transactions: List[Set[String]], minSupport: Double): Set[Set[String]] = {
  setC.filter(set => transactions.count(set.subsetOf(_))  >=  minSupport)
}

@tailrec
def apriori(setL: Set[Set[String]], transactions: List[Set[String]], k: Int, minSupport: Int): Any = {
  val frequentElements = prune(joinSort(setL, k), transactions, minSupport)
  if (frequentElements.isEmpty)
    setL
  else
    apriori(frequentElements, transactions, k + 1, minSupport)
}

apriori(setC, transactions, 1, 2)


