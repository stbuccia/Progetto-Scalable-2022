import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import java.io.{File, PrintWriter}
import scala.io.StdIn.readLine

import scala.reflect.io.Directory
import java.io.File



//http://localhost:4040/jobs/
/*
Argomenti main:
  master:
    local[2]
  inputFilePath:
    "src/main/resources/dataset_1990_2022_dataConversion_label.csv"
    "src/main/resources/dataset_2010_2021_dataConversion_label.csv"
  outputFilePath:
    "src/main/resources/output.csv"
 */


object newBasicAprioriMapReduce {

  var times = new Array[Int](0)

  def main(args: Array[String]): Unit = {

    if (args.length != 3) {
      println("Missing arguments!")
      System.exit(1)
    }

    val master = args(0)
    val inputFilePath = args(1)
    val outputFilePath = args(2)

    println("newBasicAprioriMapReduce")
    println("Configuration:")
    println(s"- master: $master")
    println(s"- dataset path: $inputFilePath")
    println(s"- output folder: $outputFilePath")

    val appName = "EarthquakeBasicAprioriMapReduce"
    val sparkSession = SparkSession.builder()
      .appName(appName)
      .master(master)
      .getOrCreate()
    val sc = sparkSession.sparkContext
    sc.setLogLevel("WARN")

    val min_support = 0.2
    val confidence = 0.7
    val labelSet: Set[String] = Set("SH", "NH", "Q1", "Q2", "Q3", "Q4", "LOW_MAG", "MED_MAG", "HIGH_MAG", "LOW_DEPTH", "MED_DEPTH", "HIGH_DEPTH")


    println("Read CSV file...")
    val rdd = time(readCSV(sc, inputFilePath)).filter(x => Set("MED_MAG").subsetOf(x))
    val rdd_size = rdd.count().toDouble


    val rdd_itemsets_1 = time(countItemsetsSize1(rdd, rdd_size, min_support))

    var rdd_itemsets = time(generateAndCountItemset(rdd, rdd_itemsets_1, labelSet, rdd_size, min_support, 2))

    var i = 3
    var stop = false
    while (i<5 && !stop) {
      var rdd_itemsets_N = time(generateAndCountItemset(rdd, rdd_itemsets.filter(_._1.size==i-1), labelSet, rdd_size, min_support, i))


      if(rdd_itemsets_N.isEmpty) {
        stop = true
        println("    - Empty")
      }
      else
        rdd_itemsets = rdd_itemsets.union(rdd_itemsets_N)

      i = i + 1
    }


    println("\nAssociation Rules:")
    val associationRules = time(generateAssociationRules(rdd_itemsets, confidence, rdd))


    val dir = new Directory(new File(outputFilePath))
    if(dir.exists) {
      dir.deleteRecursively()
    }

    println("\nWrite association rules on CSV file...")
    time(writeCSV(associationRules, outputFilePath))


    println("\n\n-------------------------------------------")
    println("Times: "+times.mkString("\t"))
    println("Total time: "+times.sum)
    println("\n\n-------------------------------------------")


    // ELIMINARE per esecuzioni su cloud
    // servono solo per poter visualizzare i dati su SparkUI nel caso di esecuzione in locale
    if(master.contains("local")) {
      println("\nMain method complete. Press Enter.")
      readLine()
    }

  }

  def writeCSV(rdd: RDD[(Set[String], Set[String], Double)], outputPath: String) = {
    rdd
      .map(x => toCSVLine(x))
      .coalesce(1)
      .saveAsTextFile(outputPath)
  }

  def readCSV(sc: SparkContext, path: String) = {
    val rdd = sc.textFile(path)
      .map( x => x.split(",") )
      .map( _.toSet )
      .persist()
    //TODO questa count() non è necessaria
    //println("   - Read " + rdd.count() + " lines")
    rdd
  }

  def countItemsetsSize1(rdd: RDD[Set[String]], rdd_size: Double, min_sup: Double) = {
    println("\nGenerate itemsets with size 1...")
    val output = rdd
      .flatMap( x => x.map( y => (Set(y),1) ) )
      .reduceByKey( (x,y) => x + y)
    //TODO questa count() è necessaria? si, perchè viene passato come parametro a tutte le fasi, lo calcolo solo una volta
      .filter( x =>  (x._2.toDouble / rdd_size) >= min_sup )
      //.collect()
    //printItemsets(output, rdd_size)
    output
  }

  def generateAndCountItemset(rdd: RDD[Set[String]],
                              rdd_itemsets: RDD[(Set[String], Int)],
                              labelSet: Set[String],
                              rdd_size: Double,
                              min_support: Double,
                              i: Int) = {
    println("\nGenerate itemsets with size " + i + "...")

    //TODO collect() dopo countItemsetsSizeN?
    val array_itemsetsSizeN = rdd_itemsets
      .map(_._1)
      .flatMap(x => generateItemsetsSizeN(x, labelSet))
      .collect().toSet
    val output = countItemsetsSizeN(rdd, array_itemsetsSizeN, rdd_size, min_support)


    output
  }

  def generateItemsetsSizeN(itemset: Set[String], labelSet: Set[String])= {
    labelSet
      .filter( x => !itemset.contains(x) )
      .map( x => itemset + x )
  }

  def countItemsetsSizeN(rdd: RDD[Set[String]], itemsets: Set[Set[String]], rdd_size: Double, min_sup: Double) = {
    rdd
      .flatMap(rdd_row =>
        itemsets
          .filter(itemset => itemset.subsetOf(rdd_row) )
          .map(itemset => (itemset,1))
      )
      .reduceByKey((x, y) => x + y)
      .filter(x => (x._2.toDouble / rdd_size) >= min_sup)
  }



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

    //TODO è necessaria la reverseRules?
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

  def printItemsets(rdd_itemsets:  RDD[(Set[String], Int)], rdd_size: Double) = {
    rdd_itemsets.zipWithIndex
      .foreach(x => println("    - Itemset " + x._2 + ": " + x._1._1.mkString(",") + "  " + (x._1._2 / rdd_size) * 100))
  }

  

  def generateAssociationRules(rdd_itemsets: RDD[(Set[String], Int)],
                               confidence: Double,
                               rdd: RDD[Set[String]] ) = {

    val array_subsets = rdd_itemsets
      .flatMap(itemset => {
        val rangeIncl = Range.inclusive(1, itemset._1.toArray.length - 1)
        rangeIncl.flatMap(y => itemset._1.subsets(y).toSet)
      }).collect()

    val array_subsets_pair = rdd
      .flatMap(rdd_row => {
        array_subsets
          .filter(subset => subset.subsetOf(rdd_row))
          .map(subset => (subset,1))
      })
      .reduceByKey((x, y) => x + y)
      .collect()

    val associationRules = rdd_itemsets
      .flatMap(itemset => {
        array_subsets_pair
          .filter(subset_pair => subset_pair._1.subsetOf(itemset._1) && subset_pair._1.size < itemset._1.size)
          .map(subset_pair => (subset_pair._1, itemset._1 -- subset_pair._1, itemset._2.toDouble / subset_pair._2.toDouble))
          .filter(rule => rule._3 >= confidence)
          .sortBy(_._3)
        //TODO sort
      })

    associationRules
  }

  def printAssociationRules(rddRules: Array[(Set[String], Set[String], Double)]) = {
    rddRules.foreach(rule =>
      println("    - Rule: " + rule._1.mkString(",") + " --> " + rule._2.mkString(",")
        + "  confidence: " + rule._3)
    )
  }


  def toCSVLine(x: (Set[String], Set[String], Double)): String = {
    x._1.mkString("--") + ",  " + x._2.mkString("--") + ",   " + x._3
  }

  //def saveAsCSVFile(rdd: RDD[(Set[String], Set[String], Double)], output_file_path: String) = {
  def saveAsCSVFile(rdd: RDD[(Set[String], Set[String], Double)], output_file_path: String) = {
    val pwCSV = new PrintWriter(
      new File(output_file_path)
    )

    rdd
      .collect()
      .foreach(x => {
        pwCSV.write(toCSVLine(x) + "\n")
    })

    pwCSV.close()
  }


  def time[R](block: => R): R = {
    val t0 = System.nanoTime()
    val result = block // call-by-name
    val t1 = System.nanoTime()
    var t = (t1 - t0)/1000000
    println("Elapsed time: " + (t1 - t0)/1000000 + "ms")
    times = times :+ t.toInt
    result
  }



}




