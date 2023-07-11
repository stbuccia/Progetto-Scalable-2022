package model

class Event(val location: (Double, Double),
            val depth: Double,
            val magnitude: Double,
            val year: Int) extends Serializable{

  override def toString: String = {
    location._1.toString + ", " + location._2 + ", " + depth + ", " + magnitude + ", " + year + '\n'
  }
}
