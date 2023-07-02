package model

class Event(val location: (Double, Double),
            val depth: Double,
            val magnitude: Double,
            val year: Int) extends Serializable{
}
