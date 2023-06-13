package AssociationRuleLearning

import java.io.File
import scala.io.Source
import scala.util.control.Breaks
import scala.util.control.Breaks.{break, breakable}

/**
 * Class for sequantial version of Apriori algorithm
 * @param inputDataFile file that contains Data - i.e. list of itemset, transactions
 *
 * todo: capire perché per usare il trait dobbiamo anche estendere qualcosa...
 * todo: siccome la lettura dal file viene fatta in modo ridondante sia qua che nel Kmeans, creare una classe di utility col metodo
 */
class AprioriSeq(inputDataFile: File, support: Int, confidence: Int) extends java.io.Serializable with Apriori{

  // Set up transactions list from Input File
  override var transactions: Seq[Set[String]] = List()

  var itemSet1 : Set[String] = Set()   // creating 1 dimensional itemset
  val src: List[String] = Source.fromFile(inputDataFile).getLines().filter(_.nonEmpty).drop(1).toList
  for (line<-src) {
    val lineSet = line.trim.split(',').toSet
    if (lineSet.nonEmpty) {
      transactions = transactions :+ lineSet
      itemSet1 = itemSet1 ++ lineSet  // it won't have duplicates because of Scala Set class implementation
    }
  }

  // Set minimum support and minimum confidence
  override var minSupport: Int = support
  override var minConfidence: Int = confidence


  /**
   * Prunes a given Candidates Set by checking if its subsets satisfy the minimum support.
   * @param candidatesSet
   * @return
   */
//  private def prune(candidatesSet: Set[Set[String]]): Set[Set[String]] = {
//
//  }

  def run(): Unit = {
    var k = 2
    var currentLSet = itemSet1
    var candidatesSet = currentLSet.subsets().filter(_.size == k).toSet
    println(candidatesSet)
//    breakable {
//      while (true) {
//        var candidatesSet = currentLSet.subsets().filter(_.size == k).toSet
//        println(candidatesSet)
//        val currentItemCombs : Set[(Set[String], Double)] = currentCSet.map( wordSet => (wordSet, getSupport(wordSet)))
//                                          .filter( wordSetSupportPair => (wordSetSupportPair._2 > minSupport))
//        val currentLSet = currentItemCombs.map( wordSetSupportPair => wordSetSupportPair._1).toSet
//        if (currentLSet.isEmpty) break
//        currentCSet = currentLSet.map( wordSet => currentLSet.map(wordSet1 => wordSet | wordSet1))
//                                                            .reduceRight( (set1, set2) => set1 | set2)
//                                                            .filter( wordSet => (wordSet.size==k))
//        itemCombs = itemCombs | currentItemCombs
//        k += 1
//      }
//  }
  }

}
