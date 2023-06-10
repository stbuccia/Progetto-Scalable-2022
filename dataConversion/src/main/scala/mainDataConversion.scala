import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import java.io.{File, PrintWriter}

object mainDataConversion {


  def main(args: Array[String]): Unit = {

    val appName = "EarthquakeDataConversion"
    val master = "local" // or "local[2]"
    val conf = new SparkConf()
      .setAppName(appName)
      .setMaster(master)
    val sc = new SparkContext(conf)

    val inputFilePath = "src/main/resources/dataset_from_1990_01_to_2022_12.csv"

    println("Read CSV file...")
    var rdd = readCSV(sc, inputFilePath)

    println("Check missing values...")
    rdd = handlerMissingValues(rdd)

    val startYear = rdd.map(_._5.toInt).min()
    val endYear = rdd.map(_._5.toInt).max()

    println("Data conversion...")
    val rdd_output = rdd.map(x => dataConversion(x))

    println("Write CSV file with binary data...")
    saveAsCSVFile(rdd_output, startYear, endYear, false)

    println("Write CSV file with text data...")
    saveAsCSVFile(rdd_output, startYear, endYear, true)


  }

  def readCSV(sc: SparkContext, path: String): RDD[(String,String,String,String,String )] = {
    val rdd = sc.textFile(path)
      .map(f => {
        f.split(",")
      })
      .mapPartitionsWithIndex {
        (idx, row) => if (idx == 0) row.drop(1) else row
      }
      .map{ case Array(x1,x2,x3,x4,x5) => (x1,x2,x3,x4,x5)}
    println("   - Read " +rdd.count() + " lines")
    return rdd
  }


  def dataConversion(x: (String, String, String, String, String)) : Tuple12[Int,Int,Int,Int,Int,Int,Int,Int,Int,Int,Int,Int] = {
    var myArray = Array(0,0,0,0,0,0,0,0,0,0,0,0);

    if(x._1.toDouble >= 0){
      myArray(1) = 1
      if(x._2.toDouble >= 0)
        myArray(2) = 1
      else
        myArray(3) = 1
    }
    else {
      myArray(0) = 1
      if (x._2.toDouble >= 0)
        myArray(5) = 1
      else
        myArray(4) = 1
    }

    //MAG   <5    5-6    >7
    if (x._4.toDouble >= 6)
      myArray(8) = 1
    else if (x._4.toDouble < 5)
      myArray(6) = 1
    else
      myArray(7) = 1

    //DEPTH   0 and 70    70 - 300    300 - 700
    if(x._3.toDouble >= 300)
      myArray(11) = 1
    else
      if(x._3.toDouble < 70)
        myArray(9) = 1
      else
        myArray(10) = 1

    (myArray(0), myArray(1), myArray(2), myArray(3), myArray(4), myArray(5),
      myArray(6), myArray(7), myArray(8), myArray(9), myArray(10), myArray(11))
  }


  def toCSVLineBinary(x: Tuple12[Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int]): String = {
    x.productIterator.mkString(",")
  }

  def toCSVLineLabel(x: Tuple12[Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int]): String = {
    var line = new Array[String](0)
    val labelArray: Array[String] = Array("SH", "NH", "Q1", "Q2", "Q3", "Q4", "LOW_MAG", "MED_MAG", "HIGH_MAG", "LOW_DEPTH", "MED_DEPTH", "HIGH_DEPTH")
    val lineArray: Array[Int] = x.productIterator.toArray.map(_.toString.toInt)
    var i = 0;
    for (n <- lineArray) {
      if (n == 1)
        line :+= labelArray(i)
      i = i + 1
    }
    line.mkString(",")
  }

  def saveAsCSVFile(rdd: RDD[(Int,Int,Int,Int,Int,Int,Int,Int,Int,Int,Int,Int)],
                          startYear: Int,
                          endYear: Int,
                          label: Boolean) = {
    var output_file_path = "src/main/resources/dataset_"+startYear+"_"+endYear+"_dataConversion_"
    if(label)
      output_file_path = output_file_path +"label.csv"
    else
      output_file_path = output_file_path +"binary.csv"

    val pwCSV = new PrintWriter(
      new File(output_file_path)
    )
    if(label) {
      rdd.collect().foreach(x => {
        pwCSV.write(toCSVLineLabel(x) + "\n")
      })
    }
    else {
      pwCSV.write("SH,NH,Q1,Q2,Q3,Q4,LOW_MAG,MED_MAG,HIGH_MAG,LOW_DEPTH,MED_DEPTH,HIGH_DEPTH" + "\n")
      rdd.collect().foreach(x => {
        pwCSV.write(toCSVLineBinary(x) + "\n")
      })
    }
    pwCSV.close()
    println("   - Write " +rdd.count() + " lines")
  }


/*
  def saveAsCSVFileLabel(rdd: RDD[(Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int)],
                         startYear: Int,
                         endYear: Int
                        ) = {
    val pwCSV = new PrintWriter(
      new File("src/main/resources/dataset_"+startYear+"_"+endYear+"_dataConversion_label.csv")
    )
    rdd.collect().foreach(x => {
      pwCSV.write(toCSVLineLabel(x) + "\n")
    })
    pwCSV.close()
  }
*/
  def countWrongValues(rdd: RDD[(String,String,String,String,String )], threshold: Double): Unit = {

    val rddWrongValues = rdd.zipWithIndex().filter(_._1._4.toDouble < threshold)
    println("Numero valore errati sul totale: " + rddWrongValues.count() + "/" + rdd.count())
    rddWrongValues.collect().foreach(println)
  }

  def checkMissingValues(x: (String, String, String, String, String)): Boolean = {
    val missingValues = x.productIterator.toArray.filter(_.toString.isEmpty)
    missingValues.length > 0
  }

  def handlerMissingValues(rdd: RDD[(String,String,String,String,String )]): RDD[(String, String, String, String, String)] = {
    val rdd_missingValues = rdd.filter(checkMissingValues)
    val numberMissingValues = rdd_missingValues.count()
    if (numberMissingValues > 0) {
      val rdd_size = rdd.count()
      println("   - Number of missing values: " + numberMissingValues + "/" + rdd_size)
      val rdd_without_missing_values = rdd.filter(y => !checkMissingValues(y))
      println("   - RDD size from " + rdd_size + " to " + rdd_without_missing_values.count())
      return rdd_without_missing_values
    }
    return rdd
  }

}
