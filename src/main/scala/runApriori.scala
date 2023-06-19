object runApriori /*extends App*/ {

  import scala.io.Source

  // Read the CSV file
  //val csvPath = "/home/stefano/IdeaProjects/Progetto-Scalable-2022/src/main/resources/output.csv"
  val csvPath = "src/main/resources/dataset_2010_2021_dataConversion_cluster0_label.csv"
  val lines = Source.fromFile(csvPath).getLines()

  //// Convert the CSV lines to transactions (sets)
  //val transactions: List[Set[String]] = lines.map { line =>
    //line.split(",").zipWithIndex.flatMap { case (value, index) =>
      //Some(value)
    //}.toSet
  //}.toList

  //val transactions: List[Set[String]] = lines.map(_.split(",").toSet).toList.filter(set => set.contains("HIGH_MAG"))
  val transactions: List[Set[String]] = lines.map(_.split(",").toSet).toList

  val threshold = 0.6
  val minSupport = (threshold * transactions.length).toInt
  val alg2 = new AprioriSparkSPC(transactions, minSupport)

  def main(args: Array[String]) = {
    System.out.println("Relative minSupport: " + threshold)
    System.out.println("Absolute minSupport: " + minSupport)
    alg2.run()
  }
}
