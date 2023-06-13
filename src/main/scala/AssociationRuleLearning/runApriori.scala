package AssociationRuleLearning

import java.io.File

/**
 * This object runs the choosen intance of Apriori algorithm on the given dataset.
 * The dataset has to be saved in a file, given as input to this object.
 * When running, dataset, minimum support and minimum confidence must be provided following this order.
 *
 * todo: fai la conversione in Int usando un Option Type
 */
object runApriori /*extends App*/ {

  val transactions = List(
//    Set("A", "B", "C", "D"),
//    Set("A" ,"B", "D"),
//    Set("A", "B"),
//    Set("B", "C", "D"),
//    Set("C", "D"),
//    Set("B", "D"),
//  )
//
//  //TODO: Creare un'interfaccia Apriori e ogni algoritmo di fatto diventa una classe
//  //val alg1 = new AprioriSeq(transactions, 2)
//  val alg2 = new AprioriSparkSPC(transactions, 1)
//
//  def main(args: Array[String]) = {
//    //System.out.println(alg1.run())
//    alg2.run()
//  }

  def main(args: Array[String]) = {
    if (args.size != 1) {
      println("Dataset file must be provided in order to run Apriori")
      System.exit(1)
    }

    // Create an instance of the algorithm you want to run, then run it
    val alg = new AprioriSeq(new File(args(0)), args(1).toInt, args(2).toInt)
    alg.run()

    // Prints Frequent Itemsets and Association Rules generated by the algorithm
    println("===Frequent Itemsets===")
    //alg.toRetItems.foreach(println)
    println("===Association Rules===")
    //alg.associationRules.foreach(println)
  }
}
