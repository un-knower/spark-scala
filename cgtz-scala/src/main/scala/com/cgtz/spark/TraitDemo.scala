package com.cgtz.spark

import org.hamcrest.core.IsEqual

/**
 * 与接口不同的是，它还可以定义属性和方法的实现。
 */
object TraitDemo {
  def main(args: Array[String]) {
    val p1 = new Points(2, 3)
    val p2 = new Points(2, 4)
    val p3 = new Points(3, 3)

    println(p1.isNotEqual(p2))
    println(p1.isNotEqual(p3))
    println(p1.isNotEqual(2))
  }
}
trait Equal {
  def isEqual(x: Any): Boolean
  def isNotEqual(x: Any): Boolean = !isEqual(x)
}

class Points(xc: Int, yc: Int) extends Equal {
  var x: Int = xc
  var y: Int = yc
  
  def isEqual(obj: Any) =
    obj.isInstanceOf[Points] &&
      obj.asInstanceOf[Points].x == x
}

