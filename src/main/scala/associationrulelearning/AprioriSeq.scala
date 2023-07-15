package associationrulelearning

import org.apache.spark.rdd.RDD
import scala.util.control.Breaks.{break, breakable}

/**
 * Class for sequential version of Apriori algorithm
 *
 */
class AprioriSeq(dataset: RDD[Set[String]], threshold: Double, confidence: Double) extends Serializable with Apriori[Seq[Set[String]]] {

  override var transactions: Seq[Set[String]] = dataset.collect().toSeq

  // Set minimum support and minimum confidence
  override var minSupport: Int = (threshold * transactions.length).toInt
  override var minConfidence: Double = confidence

  // Define local vars
  var generatedItemsets : Map[Set[String], Int] = Map()

  var frequentItemsets: Set[Set[String]] = Set()
  var associationRules : List[(Set[String], Set[String], Double)] = List()


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
      itemset.subsets.filter(subset => (subset.nonEmpty & subset.size < itemset.size))
        .foreach(subset => {associationRules = associationRules :+ (subset, itemset diff subset,
                                                                       generatedItemsets(itemset).toDouble/generatedItemsets(subset).toDouble)}
    ))
    associationRules = associationRules.filter( rule => rule._3>=minConfidence)
  }


  def run(): Unit = {

    // Initialize 1 dimensional frequent itemsets
    val singletonSet: Set[Set[String]] = itemSet.subsets().filter(_.size == 1).toSet
    singletonSet.foreach(singleton => {
      val singletonSupport = getSupport(singleton)
      if (singletonSupport >= minSupport) generatedItemsets += (singleton->singletonSupport)})


    // Find frequent itemsets
    var k = 2
    breakable {
      while (true) {
        println("Searching for " + k + " dimensional frequent itemsets")

        // Creating a set of all the possible subsets of dimension k
        val joinSet = itemSet.subsets().filter(_.size == k).toSet

        // Deleting itemsets which do not satisfy minimum support
        var candidatesSet = joinSet.map(itemset => (itemset, getSupport(itemset)))
          .filter(pair => pair._2 >= minSupport)

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

    // Save frequent itemsets
    frequentItemsets = generatedItemsets.keySet

    // Generate association rules
    generateAssociationRules()
  }

}
