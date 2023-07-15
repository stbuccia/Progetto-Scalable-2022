import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext

import java.io.{File, PrintWriter}
import scala.io.StdIn.readLine

import scala.reflect.io.Directory

//http://localhost:4040/jobs/
object BasicAprioriMapReduce {

  def main(args: Array[String]): Unit = {

    if (args.length != 3) {
      println("Missing arguments!")
      System.exit(1)
    }

    val master = args(0)
    val inputFilePath = args(1)
    val outputFilePath = args(2)

    println("BasicAprioriMapReduce")
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

    // inputFilePath passato come parametro al main
    //val inputFilePath = "src/main/resources/dataset_1990_2022_dataConversion_label.csv"
    //val inputFilePath = "src/main/resources/dataset_2010_2021_dataConversion_label.csv"

    println("Read CSV file...")
    val rdd = time(readCSV(sc, inputFilePath))//.filter(x => Set("MED_MAG").subsetOf(x))
    //TODO questa count() è necessaria? si, perchè viene passato come parametro a tutte le fasi, lo calcolo solo una volta
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
    val associationRules = time(generateAssociationRules(rdd_itemsets, confidence, rdd))


    val dir = new Directory(new File(outputFilePath))
    if (dir.exists) {
      dir.deleteRecursively()
    }

    println("\nWrite association rules on CSV file...")
    //printAssociationRules(associationRules)

    time(writeCSV(associationRules, outputFilePath))
/*
    import sparkSession.implicits._
    associationRules//.map(x => toCSVLine(x))
      .map(x => (
        x._1.mkString(","),
        x._2.mkString(","),
        x._3
      ))
      .toDF("Premessa", "Conseguenza", "Confidence")
      .coalesce(1)
      .write
      .option("header", value = true)
      .mode("overwrite")
      .csv(outputFilePath)

 */

    //time(saveAsCSVFile(associationRules, outputFilePath))
/*   sparkSession.createDataFrame(associationRules.map(x => toCSVLine(x)))
      .toDF("Premessa", "Conseguenza", "Confidence")
      .coalesce(1)
      .write
      //.option("header", value = true)
      .mode("overwrite")
      .csv(outputFilePath)
*/
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
      .filter( x =>  (x._2.toDouble / rdd_size) >= min_sup )
      .collect()
    //printItemsets(output, rdd_size)
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
    //printItemsets(output, rdd_size)
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

  /*
  def printAssociationRules(rule: (Set[String], Set[String], Double)) = {
    println("    - Rule: " + rule._1.mkString(",") + " --> " + rule._2.mkString(",")
          + "  confidence: " + rule._3)
  }*/

  def generateAssociationRules(rdd_itemsets: Array[(Set[String], Int)],
                               confidence: Double,
                               rdd: RDD[Set[String]] ) = {
    val associationRules = rdd_itemsets
      .map(x => {
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
          .reduce((x, y) => x ++ y) //.collect()

        output
        //generateAssociationRules(rdd, x._1, x._2, confidence, sc)
      })
      .reduce((x, y) => x ++ y)
      .sortBy(_._3, ascending = false)
      //.collect()

    //printAssociationRules(associationRules)

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
  def saveAsCSVFile(rdd: Array[(Set[String], Set[String], Double)], output_file_path: String) = {
    val pwCSV = new PrintWriter(
      new File(output_file_path)
    )

    rdd
      //.collect()
      .foreach(x => {
        pwCSV.write(toCSVLine(x) + "\n")
    })

    pwCSV.close()
    //println("   - Write " + rdd.count() + " lines")
    println("   - Write " + rdd.length + " lines")
  }


  def time[R](block: => R): R = {
    val t0 = System.nanoTime()
    val result = block // call-by-name
    val t1 = System.nanoTime()
    println("Elapsed time: " + (t1 - t0)/1000000 + "ms")
    result
  }



}




