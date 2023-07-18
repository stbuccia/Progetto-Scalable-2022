object Utils {

  def time[R](msg: String, block: => R): R = {
    val t0 = System.nanoTime()
    val result = block
    val t1 = System.nanoTime()
    println(msg + " - elapsed time: " + (t1 - t0) / 1000000 + "ms")
    result
  }

}
