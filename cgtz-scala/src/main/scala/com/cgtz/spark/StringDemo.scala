package com.cgtz.spark

object StringDemo {
  val greeting = "hello scala"
  val greetingss = "greetingss"
  val buf = new StringBuilder
  buf+='a'
  buf ++="adfadfasd"
  buf.append("ddddddddddddddd")
  
  def main(args: Array[String]): Unit = {
    println(greeting.length())
    println(greeting)
    println(buf.toString())
    
    println(greetingss+greeting)
  }
}