package associationrulelearning

import scala.util.control.Breaks.{break, breakable}

/**
 * Class for sequential version of Apriori algorithm
 *
 */
class AprioriSeq(dataset: Seq[Set[String]]) extends Serializable with Apriori[Seq[Set[String]]] {

  override var transactions: Seq[Set[String]] = dataset
  var minSupportCount: Int = (minSupport * transactions.length).toInt

  var generatedItemsets : Map[Set[String], Int] = Map()

  var frequentItemsets: Set[(Set[String], Int)] = Set()
  var associationRules: List[(Set[String], Set[String], Double)] = List()


  /**
   * Counts the occurences of the given itemset inside the dataset
   * @param itemset set of items appearing inside the dataset
   * @return  number of times the given itemset appears
   */
  def getSupport(itemset : Set[String]) : Int = {
    transactions.count(transaction => itemset.subsetOf(transaction))
  }


  /**
   * Prunes a given Candidates Set by checking if its subsets satisfy the minimum support.
   * @param candidatesSet set of itemsets, counting the support for each one of them
   * @return  subset of candidatesSet where only the ones whose subsets satisfy the minimum support are left
   */
  private def prune(candidatesSet: Set[(Set[String],Int)]): Set[(Set[String],Int)] = {
    candidatesSet.filter(pair => transactions.count(transaction => pair._1.subsetOf(transaction)) >= minSupport)
  }


  /**
   * Generates Association Rules from Frequent Itemsets. Every rule must have at least minConfidence confidence value.
   */
  private def generateAssociationRules(): Unit = {
    frequentItemsets.foreach(itemset =>
      itemset._1.subsets.filter(subset => (subset.nonEmpty & subset.size < itemset._1.size))
        .foreach(subset => {associationRules = associationRules :+ (subset, itemset._1 diff subset,
          generatedItemsets(itemset._1).toDouble/generatedItemsets(subset).toDouble)}
        ))
    associationRules = associationRules.filter( rule => rule._3>=minConfidence)
  }


  def run(): List[(Set[String], Set[String], Double)] = {

    // Initialize 1 dimensional frequent itemsets
    val singletonSet: Set[Set[String]] = itemSet.subsets().filter(_.size == 1).toSet
    singletonSet.foreach(singleton => {
      val singletonSupport = getSupport(singleton)
      if (singletonSupport >= minSupportCount) generatedItemsets += (singleton->singletonSupport)})


    // Find frequent itemsets
    var k = 2
    breakable {
      while (true) {

        // Creating a set of all the possible subsets of dimension k
        val joinSet = itemSet.subsets(k).toSet

        // Deleting itemsets which do not satisfy minimum support
        var candidatesSet = joinSet.map(itemset => (itemset, getSupport(itemset)))
          .filter(pair => pair._2 >= minSupportCount)

        // Deleting itemsets whose subsets do not satisfy minimum support
        candidatesSet = prune(candidatesSet)

        // Stop if there are no other itemsets
        if (candidatesSet.isEmpty) break

        // Save the found itemsets and keep looking for others
        for(itemset<-candidatesSet) {
          generatedItemsets += (itemset._1->itemset._2)
        }
        k += 1
      }
    }

    frequentItemsets = generatedItemsets.map{ case(k,v) => (k,v)}(collection.breakOut) : Set[(Set[String], Int)]

    generateAssociationRules()

    associationRules
  }

}
