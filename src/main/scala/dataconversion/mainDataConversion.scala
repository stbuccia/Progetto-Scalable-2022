package dataconversion

import model.Event


object mainDataConversion {

  def labelConversion(event: Event): Set[String] = {

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

    Set(line(0), line(1), line(2), line(3))
  }

}
