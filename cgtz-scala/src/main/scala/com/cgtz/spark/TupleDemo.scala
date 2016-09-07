package com.cgtz.spark

object TupleDemo {
  val t = (1,3.14,"fred")
  var t2 = new Tuple3(1,3.14,"fred")
  
  def main(args: Array[String]): Unit = {
    //one
    //two
    //three
    four
  }
  def one():Unit={
    val t = (4,3,2,1)
    
    val sum = t._1+t._2+t._3+t._4
    println(sum)
  }
  def two():Unit={
    val t = (4,3,2,1)
    t.productIterator.foreach { i=> println(i) }
  }
  def three():Unit={
    val t = new Tuple3(1,"hello",Console)
    println(t.toString())
  }
  def four():Unit={
    val t = new Tuple2("www","aaaaa")
    println(t.swap)
  }
}