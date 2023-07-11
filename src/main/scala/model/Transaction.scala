package model

class Transaction(val hemisphere: String,
                  val quadrant: String,
                  val magnitude: String,
                  val depth: String) extends Serializable{

  override def toString: String = {
    " "+ hemisphere + ", " + quadrant + ", " + magnitude + ", " + depth
  }

  def toSet(): Set[String] = {
    Set(hemisphere, quadrant, magnitude, depth)
  }
}
