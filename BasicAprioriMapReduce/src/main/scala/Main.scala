import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import java.io.File
import scala.reflect.io.Directory

object Main {
  def main(args: Array[String]): Unit = {

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

    println("Read CSV file...")
    val rdd = readCSV(sc, inputFilePath)

    val basicApriori = new newBasicAprioriMapReduce(rdd)
    val associationRules = time("BasicAprioriMapReduce", basicApriori.run())

    val dir = new Directory(new File(outputFilePath))
    if (dir.exists) {
      dir.deleteRecursively()
    }

    println("\nWrite association rules on CSV file...")
    writeCSV(associationRules, outputFilePath)



    // ELIMINARE per esecuzioni su cloud
    // servono solo per poter visualizzare i dati su SparkUI nel caso di esecuzione in locale
    if (master.contains("local")) {
      println("\nMain method complete. Press Enter.")
      readLine()
    }


  }


  def readCSV(sc: SparkContext, path: String) = {
    val rdd = sc.textFile(path)
      .map(x => x.split(","))
      .map(_.toSet)
      .persist()
    //TODO questa count() non è necessaria
    //println("   - Read " + rdd.count() + " lines")
    rdd
  }


  def writeCSV(rdd: RDD[(Set[String], Set[String], Double)], outputPath: String) = {
    rdd
      .map(x => toCSVLine(x))
      .coalesce(1)
      .saveAsTextFile(outputPath)
  }

  def toCSVLine(x: (Set[String], Set[String], Double)): String = {
    x._1.mkString("--") + ",  " + x._2.mkString("--") + ",   " + x._3
  }


  def time[R](msg: String, block: => R): R = {
    val t0 = System.nanoTime()
    val result = block // call-by-name
    val t1 = System.nanoTime()
    println(msg + "\n\tElapsed time: " + (t1 - t0) / 1000000 + "ms")
    result
  }

}
