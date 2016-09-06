package com.cgtz.spark

import scala.util.matching.Regex

/**
 * 正则表达式
 */
object RegexDemo {
  def main(args: Array[String]): Unit = {
    pattentOne
    pattentTwo()
    pattentThree
    pattentFour
  }
  def pattentOne(): Unit = {
    //实例中使用 String 类的 r() 方法构造了一个Regex对象。
    val pattern = "Scala".r
    val str = "Scala is Scalable and cool"
    //然后使用 findFirstIn 方法找到首个匹配项。
    println(pattern findFirstIn str)
  }
  def pattentTwo():Unit = {
    val pattern = new Regex("(S|s)cala")
    val str = "Scala is scalable and cool"
    println(pattern.findAllIn(str).mkString(","))
  }
  def pattentThree():Unit={
    val pattern = "(S|s)cala".r
    val str = "Scala is scalable and cool"
    println(pattern replaceFirstIn(str , "JAVA"))
  }
  def pattentFour():Unit={
    val pattern = new Regex("abl[ae]\\d+")
    val str = "ablaw is able1 and cool"
    println((pattern findAllIn str).mkString(","))
  }
}