package AssociationRuleLearning

object runApriori /*extends App*/ {

  val transactions = List(
    Set("A", "B", "C", "D"),
    Set("A" ,"B", "D"),
    Set("A", "B"),
    Set("B", "C", "D"),
    Set("C", "D"),
    Set("B", "D"),
  )

  //TODO: Creare un'interfaccia Apriori e ogni algoritmo di fatto diventa una classe
  //val alg1 = new AprioriSeq(transactions, 2)
  val alg2 = new AprioriSparkSPC(transactions, 1)

  def main(args: Array[String]) = {
    //System.out.println(alg1.run())
    alg2.run()
  }
}
