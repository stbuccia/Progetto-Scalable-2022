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

    println("Read CSV file...")
    val rdd = readCSV(sc, "src/main/resources/dataset_from_2010_01_to_2021_12.csv")
    println("Data conversion...")
    val rdd_output = rdd.map(x => dataConversion(x))
    println("Write CSV file...")
    saveAsCSVFile(rdd_output)
  }

  def readCSV(sc: SparkContext, path: String): RDD[(String,String,String,String,String )] = {
    sc.textFile(path)
      .map(f => {
        f.split(",")
      })
      .mapPartitionsWithIndex {
        (idx, row) => if (idx == 0) row.drop(1) else row
      }
      .map{ case Array(x1,x2,x3,x4,x5) => (x1,x2,x3,x4,x5)}
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


  def toCSVLine(x: Tuple12[Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int]): String = {
    x.productIterator.mkString(",")
  }

  def saveAsCSVFile(rdd: RDD[(Int,Int,Int,Int,Int,Int,Int,Int,Int,Int,Int,Int)]) = {
    val pwCSV = new PrintWriter(
      new File("src/main/resources/dataset_dataConversion.csv")
    )
    pwCSV.write("SH,NH,Q1,Q2,Q3,Q4,LOW_MAG,MED_MAG,HIGH_MAG,LOW_DEPTH,MED_DEPTH,HIGH_DEPTH" + "\n")
    rdd.collect().foreach(x => {
      pwCSV.write(toCSVLine(x) + "\n")
    })
    pwCSV.close()
  }

}
