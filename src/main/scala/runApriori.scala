object runApriori /*extends App*/ {

  import scala.io.Source

  // Read the CSV file
  val csvPath = "/home/stefano/IdeaProjects/Progetto-Scalable-2022/src/main/resources/output.csv"
  val lines = Source.fromFile(csvPath).getLines()

  // Convert the CSV lines to transactions (sets)
  val transactions: List[Set[String]] = lines.map { line =>
    line.split(",").zipWithIndex.flatMap { case (value, index) =>
      if (value == "1")
        index match {
          case 0 => Some("nh")  
          case 1 => Some("sh")  
          case 2 => Some("quad1")  
          case 3 => Some("quad2")  
          case 4 => Some("quad3")  
          case 5 => Some("quad4")  
          case 6 => Some("low_mag")  
          case 7 => Some("med_mag")  
          case 8 => Some("high_mag")  
          case 9 => Some("low_depth")  
          case 10 => Some("med_depth")  
          case 11 => Some("high_depth")  
        }
      else 
        None
    }.toSet
  }.toList

  val threshold = 0.1
  val minSupport = (threshold * transactions.length).toInt
  val alg2 = new AprioriSparkSPC(transactions, minSupport)

  def main(args: Array[String]) = {
    System.out.println("Relative minSupport: " + minSupport)
    System.out.println("Absolute minSupport: " + threshold)
    alg2.run()
  }
}
