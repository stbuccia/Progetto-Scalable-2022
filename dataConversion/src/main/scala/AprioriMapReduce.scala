import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import java.io.{File, PrintWriter}

object AprioriMapReduce {

  def main(args: Array[String]): Unit = {

    val appName = "EarthquakeAprioriMapReduce"
    val master = "local[2]" //local or "local[2]"
    val conf = new SparkConf()
      .setAppName(appName)
      .setMaster(master)
    val sc = new SparkContext(conf)

    val min_support = 0.6
    val confidence = 0.7
    val labelSet: Set[String] = Set("SH", "NH", "Q1", "Q2", "Q3", "Q4", "LOW_MAG", "MED_MAG", "HIGH_MAG", "LOW_DEPTH", "MED_DEPTH", "HIGH_DEPTH")
    //val inputFilePath = "src/main/resources/dataset_1990_2022_dataConversion_label.csv"
    val inputFilePath = "src/main/resources/dataset_2010_2021_dataConversion_label.csv"

    println("Read CSV file...")
    val rdd = time(readCSV(sc, inputFilePath))//.filter(x => Set("MED_MAG").subsetOf(x))
    val rdd_size = rdd.count().toDouble


    val rdd_itemsets_1 = time(countItemsetsSize1(rdd, rdd_size, min_support))
    val rdd_itemsets_2 = time(generateAndCountItemset(rdd, rdd_itemsets_1, labelSet, rdd_size, min_support, 2))

    var rdd_itemsets = rdd_itemsets_2.clone()
    var i = 3
    var stop = false
    while (i<5 && !stop) {
      var rdd_itemsets_N = time(generateAndCountItemset(rdd, rdd_itemsets, labelSet, rdd_size, min_support, i))

      if(rdd_itemsets_N.isEmpty) {
        stop = true
        println("    - Empty")
      }
      else
        rdd_itemsets = rdd_itemsets_N.clone()
      i = i + 1
    }

    println("\nAssociation Rules:")
    //val associationRules = sc.parallelize(rdd_itemsets)
    val associationRules = rdd_itemsets
      .map( x => {
        val rangeIncl = Range.inclusive(1, x._1.toArray.length - 1)
        //val arr = sc.parallelize(rangeIncl.flatMap(y => x._1.subsets(y).toSet))
        val arr = rangeIncl.flatMap(y => x._1.subsets(y).toSet)
        val output = arr.map(y => generateAssociationRulesFromSubset(
          rdd,
          x._1,
          x._2,
          y,
          confidence
        ))
        .reduce((x, y) => x ++ y)//.collect()

        output
        //generateAssociationRules(rdd, x._1, x._2, confidence, sc)
      })
      .reduce((x,y) => x ++ y)
    associationRules
      .sortBy(_._3, ascending = false).collect().foreach(printAssociationRules)
  }

  def readCSV(sc: SparkContext, path: String) = {
    val rdd = sc.textFile(path)
      .map( x => x.split(",") )
      .map( _.toSet )
    println("   - Read " + rdd.count() + " lines")
    rdd
  }

  def countItemsetsSize1(rdd: RDD[Set[String]], rdd_size: Double, min_sup: Double) = {
    println("\nGenerate itemsets with size 1...")
    val output = rdd
      .flatMap( x => x.map( y => (Set(y),1) ) )
      .reduceByKey( (x,y) => x + y)
      .filter( x =>  (x._2.toDouble / rdd_size) >= min_sup )
      .collect()
    printItemsets(output, rdd_size)
    output
  }

  def generateAndCountItemset(rdd: RDD[Set[String]],
                              rdd_itemsets:  Array[(Set[String], Int)],
                              labelSet: Set[String],
                              rdd_size: Double,
                              min_support: Double,
                              i: Int) = {
    println("\nGenerate itemsets with size " + i + "...")
    val output = rdd_itemsets
      .map(_._1)
      .flatMap(x => generateItemsetsSizeN(x, labelSet))
      .map(x => countItemsetsSizeN(rdd, x, rdd_size, min_support))
      .reduce((x, y) => x ++ y)
      .distinct
      .collect()
    printItemsets(output, rdd_size)
    output
  }

  def generateItemsetsSizeN(itemset: Set[String], labelSet: Set[String])= {
    labelSet
      .filter( x => !itemset.contains(x) )
      .map( x => itemset + x )
  }

  def countItemsetsSizeN(rdd: RDD[Set[String]], itemset: Set[String], rdd_size: Double, min_sup: Double) = {
    rdd
      .filter( x => itemset.subsetOf(x) )
      .map( x => (itemset, 1) )
      .reduceByKey( (x, y) => x + y )
      .filter( x => (x._2.toDouble / rdd_size) >= min_sup )
      //.collect()
  }
/*
  def generateAssociationRules(rdd: RDD[Set[String]],
                               itemset: Set[String],
                               itemset_support: Int,
                               confidence: Double,
                               sc: SparkContext) = {
    // confidence = support_totale / support_antecedente
    /*
    val rules = rdd
      .filter(x => Set(itemset.head).subsetOf(x))
      .map(x => (Set(itemset.head), 1))
      .reduceByKey((x, y) => x + y)
      .map(x => (x._1, itemset -- x._1, itemset_support / x._2.toDouble))
      .filter(x => x._3 >= confidence)
      //.collect()

    val rules1 = rdd
      .filter(x => itemset.tail.subsetOf(x))
      .map(x => (itemset.tail, 1))
      .map(x => (itemset.tail, 1))
      .reduceByKey((x, y) => x + y)
      .map(x => (x._1, itemset -- x._1, itemset_support / x._2.toDouble))
      .filter(x => x._3 >= confidence)
      //.collect()


    //rules.collect()
    (rules ++ rules1).collect()

     */



    output

  }
  */

  def generateAssociationRulesFromSubset(rdd: RDD[Set[String]],
                                         itemset: Set[String],
                                         itemset_support: Int,
                                         subItemset: Set[String],
                                         confidence: Double) = {
    val rules = rdd
      .filter(x => subItemset.subsetOf(x))
      .map(x => (subItemset, 1))
      .reduceByKey((x, y) => x + y)
      .map(x => (x._1, itemset -- subItemset, itemset_support.toDouble / x._2.toDouble))
      .filter(x => x._3 >= confidence)
/*
    val reverseRules = rdd
      .filter(x => (itemset -- subItemset).subsetOf(x))
      .map(x => ((itemset -- subItemset), 1))
      .reduceByKey((x, y) => x + y)
      .map(x => (x._1, itemset -- (itemset -- subItemset), itemset_support / x._2.toDouble))
      .filter(x => x._3 >= confidence)

    rules ++ reverseRules

 */
    rules
  }

  def printItemsets(rdd_itemsets:  Array[(Set[String], Int)], rdd_size: Double) = {
    rdd_itemsets.zipWithIndex
      .foreach(x => println("    - Itemset " + x._2 + ": " + x._1._1.mkString(",") + "  " + (x._1._2 / rdd_size) * 100))
  }

  def printAssociationRules(rule: (Set[String], Set[String], Double)) = {
    println("    - Rule: " + rule._1.mkString(",") + " --> " + rule._2.mkString(",")
          + "  confidence: " + rule._3)
  }

  def time[R](block: => R): R = {
    val t0 = System.nanoTime()
    val result = block // call-by-name
    val t1 = System.nanoTime()
    println("Elapsed time: " + (t1 - t0)/1000000 + "ms")
    result
  }



}


object LabelEnum extends Enumeration {
  type LabelEnum = Value

  // Assigning Values
  val SH = Value("SH") // ID = 0
  val NH = Value("NH") // ID = 1
  val Q1 = Value("Q1") // ID = 2
  val Q2 = Value("Q2") // ID = 3
  val Q3 = Value("Q3") // ID = 4
  val Q4 = Value("Q4") // ID = 5
  val LOW_MAG = Value("LOW_MAG") // ID = 6
  val MED_MAG = Value("MED_MAG") // ID = 7
  val HIGH_MAG = Value("HIGH_MAG") // ID = 8
  val LOW_DEPTH = Value("LOW_DEPTH") // ID = 9
  val MED_DEPTH = Value("MED_DEPTH") // ID = 10
  val HIGH_DEPTH = Value("HIGH_DEPTH") // ID = 11

  //println(s"ID of third = ${LabelEnum.Q3}")
  //println(s"ID of third = ${LabelEnum.Q3.id}")
}
