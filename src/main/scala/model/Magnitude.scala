package model

object Magnitude extends Enumeration {
  type Magnitude = Value

  val low = Value("LOW_MAG")
  val med = Value("MED_MAG")
  val high = Value("HIGH_MAG")
}
