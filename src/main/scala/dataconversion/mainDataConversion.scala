package dataconversion


import model.{Event, Hemisphere, Magnitude, Quadrant, Depth, Transaction}

object mainDataConversion {

  //def labelConversion(event: Event): Transaction = {
  def labelConversion(event: Event): Set[String] = {
 
    //fromTupleToTransaction(fromEventToTuple(event))
    val line = new Array[String](4)

    if (event.location._1 >= 0) {
      line(0) = "NH"
      if (event.location._2 >= 0)
        line(1) = "Q1"
      else
        line(1) = "Q2"
    }
    else {
      line(0) = "SH"
      if (event.location._2 >= 0)
        line(1) = "Q4"
      else
        line(1) = "Q3"
    }

    //MAG   <5    5-6    >7
    if (event.magnitude >= 6)
      line(2) = "HIGH_MAG"
    else if (event.magnitude < 5)
      line(2) = "LOW_MAG"
    else
      line(2) = "MED_MAG"

    //DEPTH   0 and 70    70 - 300    300 - 700
    if (event.depth >= 300)
      line(3) = "HIGH_DEPTH"
    else if (event.depth < 70)
      line(3) = "LOW_DEPTH"
    else
      line(3) = "MED_DEPTH"


    //new Transaction(line(0), line(1), line(2), line(3))
    Set(line(0), line(1), line(2), line(3))
  }

  /*
  def labelConversionEnum(event: Event): Transaction = {

    var mag = Magnitude.low
    var depth = Depth.low
    var quadrant = Quadrant.q1
    var hemisphere = Hemisphere.north

    if (event.location._1 >= 0) {
      hemisphere = Hemisphere.north
      if (event.location._2 >= 0)
        quadrant = Quadrant.q1
      else
        quadrant = Quadrant.q2
    }
    else {
      hemisphere = Hemisphere.south
      if (event.location._2 >= 0)
        quadrant = Quadrant.q4
      else
        quadrant = Quadrant.q3
    }

    //MAG   <5    5-6    >7
    if (event.magnitude >= 6)
      mag = Magnitude.high
    else if (event.magnitude < 5)
      mag = Magnitude.low
    else
      mag = Magnitude.med

    //DEPTH   0 and 70    70 - 300    300 - 700
    if (event.depth >= 300)
      depth = Depth.high
    else if (event.depth < 70)
      depth = Depth.low
    else
      depth = Depth.med

    new Transaction(hemisphere, quadrant, mag, depth)
  }
*/

  /*
    def main(args: Array[String]): Unit = {

      val appName = "EarthquakeDataConversion"
      val master = "local" // or "local[2]"
      val conf = new SparkConf()
        .setAppName(appName)
        .setMaster(master)
      val sc = new SparkContext(conf)

      val inputFilePath = "src/main/resources/dataset_from_2010_01_to_2021_12_cluster1.csv"

      println("Read CSV file...")
      var rdd = readCSV(sc, inputFilePath)

      println("Check missing values...")
      rdd = handlerMissingValues(rdd)

      //val startYear = rdd.map(_._5.toInt).min()
      //val endYear = rdd.map(_._5.toInt).max()

      //println("Data conversion...")
      //val rdd_output = rdd.map(x => dataConversion(x))

      RDDLabelConversion(rdd).collect().foreach(println)
      //println("Write CSV file with binary data...")
      //saveAsCSVFile(rdd_output, startYear, endYear, false)

      //println("Write CSV file with text data...")
      //saveAsCSVFile(rdd_output, startYear, endYear, true)


    }

  def RDDLabelConversion(transactions: RDD[(String,String,String,String,String)]) : RDD[(String, String, String, String)] = {
    transactions.map(dataConversion).map(toTupleLineLabel)
  }

<<<<<<< HEAD
  def labelConversion(event: Event): Set[String] = {
    fromTupleToTransaction(fromEventToTuple(event))
  }
=======
>>>>>>> 6c30a32f49cf9ab922c69ac8bace5bd0dd12be74

  private def fromEventToTuple(x: Event): Tuple12[Int,Int,Int,Int,Int,Int,Int,Int,Int,Int,Int,Int] = {
    val myArray = Array(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0);

    if(x.location._1 >= 0){
      myArray(1) = 1
      if(x.location._2 >= 0)
        myArray(2) = 1
      else
        myArray(3) = 1
    }
    else {
      myArray(0) = 1
      if (x.location._2 >= 0)
        myArray(5) = 1
      else
        myArray(4) = 1
    }

    //MAG   <5    5-6    >7
    if (x.magnitude >= 6)
      myArray(8) = 1
    else if (x.magnitude < 5)
      myArray(6) = 1
    else
      myArray(7) = 1

    //DEPTH   0 and 70    70 - 300    300 - 700
    if(x.depth >= 300)
      myArray(11) = 1
    else
      if(x.depth < 70)
        myArray(9) = 1
      else
        myArray(10) = 1

    (myArray(0), myArray(1), myArray(2), myArray(3), myArray(4), myArray(5),
      myArray(6), myArray(7), myArray(8), myArray(9), myArray(10), myArray(11))

  }

  private def fromTupleToTransaction(tuple: Tuple12[Int,Int,Int,Int,Int,Int,Int,Int,Int,Int,Int,Int]): Set[String] = {
    var line = new Array[String](0)
    val labelArray: Array[String] = Array("SH", "NH", "Q1", "Q2", "Q3", "Q4", "LOW_MAG", "MED_MAG", "HIGH_MAG", "LOW_DEPTH", "MED_DEPTH", "HIGH_DEPTH")
    val lineArray: Array[Int] = tuple.productIterator.toArray.map(_.toString.toInt)
    var i = 0;
    for (n <- lineArray) {
      if (n == 1) {
        line :+= labelArray(i)
      }
      i = i + 1
    }
    //new Transaction(line(0), line(1), line(2), line(3))
     Set(line(0), line(1), line(2), line(3))
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

  def toTupleLineLabel(x: Tuple12[Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int]): (String, String, String, String) = {
    var line = new Array[String](0)
    val labelArray: Array[String] = Array("SH", "NH", "Q1", "Q2", "Q3", "Q4", "LOW_MAG", "MED_MAG", "HIGH_MAG", "LOW_DEPTH", "MED_DEPTH", "HIGH_DEPTH")
    val lineArray: Array[Int] = x.productIterator.toArray.map(_.toString.toInt)
    var i = 0;
    for (n <- lineArray) {
      if (n == 1) {
        line :+= labelArray(i)
      }
      i = i + 1
    }
    (line(0), line(1), line(2), line(3))
  }

  def saveAsCSVFile(rdd: RDD[(Int,Int,Int,Int,Int,Int,Int,Int,Int,Int,Int,Int)],
                    startYear: Int,
                    endYear: Int,
                    label: Boolean) = {
    var output_file_path = "src/main/resources/dataset_"+startYear+"_"+endYear+"_dataConversion_"
    if(label)
      output_file_path = output_file_path +"cluster1_label.csv"
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

<<<<<<< HEAD
}
=======

   */

}

