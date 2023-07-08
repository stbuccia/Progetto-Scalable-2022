package model

object Depth extends Enumeration {
  type Depth = Value

  val low = Value("LOW_DEPTH")
  val med = Value("MED_DEPTH")
  val high = Value("HIGH_DEPTH")
}
