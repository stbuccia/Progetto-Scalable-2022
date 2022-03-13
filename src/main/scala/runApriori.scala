object runApriori extends App {
  val alg = new Apriori()
  val setC = Set(Set("A"), Set("B"), Set("C"), Set("D"), Set("E"), Set("F"), Set("G"))
  val transactions = List(Set("A", "B", "C"), Set("A" ,"C"), Set("B", "C", "D", "E"), Set("A", "D"), Set("E"), Set("C", "D", "E", "F", "G" ))

  System.out.println(alg.apriori_sequential(setC, transactions, 1, 2))
}
