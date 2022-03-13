object runApriori extends App {
  val transactions = List(Set("A", "B", "C"), Set("A" ,"C"), Set("B", "C", "D", "E"), Set("A", "D"), Set("E"), Set("C", "D", "E", "F", "G" ))

  val alg = new Apriori(transactions, 2)
  System.out.println(alg.run(alg.apriori_sequential))
}
