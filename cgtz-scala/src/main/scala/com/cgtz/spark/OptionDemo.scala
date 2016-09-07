package com.cgtz.spark

object OptionDemo {
  def main(args: Array[String]): Unit = {
    one
    two
    three
    four
  }
  def one():Unit={
    val myMap = Map("key1"->"value")
    val value1 = myMap.get("key1")
    val value2 = myMap.get("key2")
    
    println(value1)
    println(value2)
  }
  def two():Unit={
    val sites = Map("runoob" -> "www.runoob.com", "google" -> "www.google.com")

    println("sites.get( \"runoob\" ) : " + sites.get("runoob")) // Some(www.runoob.com)
    println("sites.get( \"baidu\" ) : " + sites.get("baidu")) //  None
  }
  def three():Unit={
    val sites = Map("runoob" -> "www.runoob.com", "google" -> "www.google.com")

    println(show(sites.get("runoob")))
    println(show(sites.get("goosgle")))
  }
  def show(x:Option[String]) : String= {
    x match{
      case Some(s) => s
      case None => "?"
    }
  }
  def four():Unit={
    val a = Some(5)
    val b = None
    
    println(a.getOrElse(6461))
    println(b.getOrElse(10))
    println(a.isEmpty)
    println(b.isEmpty)
  }
}