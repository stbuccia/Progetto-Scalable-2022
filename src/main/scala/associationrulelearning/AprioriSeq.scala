package associationrulelearning

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import java.io.File
import scala.io.Source
import scala.util.control.Breaks
import scala.util.control.Breaks.{break, breakable}

/**
 * Class for sequential version of Apriori algorithm
 * @param dataFilePath path of the file that contains Data - i.e. list of itemset, transactions
 *
 * todo: capire perché per usare il trait dobbiamo anche estendere qualcosa...
 * todo: siccome la lettura dal file viene fatta in modo ridondante sia qua che nel Kmeans, creare una classe di utility col metodo
 */
class AprioriSeq(dataFilePath: String, threshold: Double, confidence: Double, context: SparkContext) extends Serializable with Apriori {

  // Set up transactions list from Input File
  val rdd: RDD[Set[String]] = context.textFile(dataFilePath)
      .map( x => x.split(",") )
      .map(_.toSet)
  override var transactions: Seq[Set[String]] = rdd.collect().toSeq

  // Set minimum support and minimum confidence
  override var minSupport: Int = (threshold * transactions.length).toInt
  override var minConfidence: Double = confidence

  // Define global vars
  var generatedItemsets : Map[Set[String], Int] = Map()
  var singletonSet: Set[Set[String]] = itemSet.subsets().filter(_.size == 1).toSet
  for(singleton<-singletonSet) {
    generatedItemsets += (singleton->getSupport(singleton))
  }

  var frequentItemsets: Set[Set[String]] = Set()
  var associationRules : List[(Set[String], Set[String], Double)] = List()


  /**
   * Counts the occurences of the given itemset inside the dataset
   * @param itemset set of items appearing inside the dataset
   * @return  number of times the given itemset appears
   */
  def getSupport(itemset : Set[String]) : Int = {
    transactions.count(transaction => itemset.subsetOf(transaction))
    // count.toDouble / transactions.size.toDouble
  }


  /**
   * Prunes a given Candidates Set by checking if its subsets satisfy the minimum support.
   * @param candidatesSet set of itemsets, counting the support for each one of them
   * @return  subset of candidatesSet where only the ones whose subsets satisfy the minimum support are left
   */
  private def prune(candidatesSet: Set[(Set[String],Int)]): Set[(Set[String],Int)] = {
    candidatesSet.filter(pair => transactions.count(pair._1.subsetOf(_)) >= minSupport)
  }

  private def generateAssociationRules(): Unit = {
    frequentItemsets.foreach(itemset =>
      itemset.subsets.filter(subset => (subset.nonEmpty & subset.size < itemset.size))
        .foreach(subset => {associationRules = associationRules :+ (subset, itemset diff subset,
                                                                       generatedItemsets(itemset).toDouble/generatedItemsets(subset).toDouble)}
    ))
    associationRules = associationRules.filter( rule => rule._3>=minConfidence)
  }

  def run(): Unit = {

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
    frequentItemsets = generatedItemsets.keySet.filter(itemset => itemset.size == k-1)

    // Generate association rules
    generateAssociationRules()
  }

}
