import org.apache.spark.{SparkConf, SparkContext}

object Main {
  def main(args: Array[String]): Unit = {

    val appName = "EarthquakeBasicAprioriMapReduce"
    val master = "local" //local or "local[2]"
    val conf = new SparkConf()
      .setAppName(appName)
      .setMaster(master)
    val sc = new SparkContext(conf)

    val inputRulesFilePath = "src/main/resources/rules_support_6.csv"
    //val inputRulesFilePath = "src/main/resources/rules_support_60.csv"
    val inputDatasetFilePath = "src/main/resources/dataset_2010_2021_dataConversion_label.csv"


    println("Read dataset...")
    val rdd_dataset = time(readCSV(sc, inputDatasetFilePath))
    val dataset_size = rdd_dataset.count()

    println("\nRead association rules...")
    val rdd_rules = time(readAssociationRules(sc, inputRulesFilePath).collect())



    val result = time(rdd_rules
      .map(rules => (rules, rdd_dataset
                              .filter(line => rules._1.subsetOf(line))
                              .filter(line => rules._2.subsetOf(line))
                              .count())

      )
    )
    result.map(x => printAssociationRules(x, dataset_size))



  }

  def readCSV(sc: SparkContext, path: String) = {
    val rdd = sc.textFile(path)
      .map(x => x.split(","))
      .map(_.toSet)
    println("   - Read " + rdd.count() + " lines")
    rdd
  }


  def readAssociationRules(sc: SparkContext, path: String) = {
    val rdd = sc.textFile(path)
      .map(x => x.split(","))
      .map(x =>
        (
          x(0).split("&").toSet,
          x(1).split("&").toSet,
          x(2).toDouble
        )
      )
      //.collect()

    println("   - Read " + rdd.count() + " lines")
    rdd
  }

  def printAssociationRules(rule: ((Set[String], Set[String], Double), Long), dataset_size: Long) = {
    println("\n    - Rule: " + rule._1._1.mkString(",")
      + " --> " + rule._1._2.mkString(",")
      + "\n    conf: " + rule._1._3
      + "  occ: " + rule._2 + "/" + dataset_size + "   " + (rule._2/dataset_size.toDouble*100) )
  }

  def time[R](block: => R): R = {
    val t0 = System.nanoTime()
    val result = block // call-by-name
    val t1 = System.nanoTime()
    println("Elapsed time: " + (t1 - t0) / 1000000 + "ms")
    result
  }
}

//Main.main()