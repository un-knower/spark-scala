package com.cgtz.spark

import java.io.PrintWriter
import java.io.File
import scala.io.Source

object IODemo {
  def main(args: Array[String]): Unit = {
    one
    two
    three
  }
  def one():Unit = {
		  val writer = new PrintWriter(new File("data/test.txt"))
		  writer.write("nihao")
		  writer.close()
  }
  def two():Unit={
    println("请输入：")
    val line = Console.readLine()
    println("3q:"+line)
  }
  def three():Unit={
    println("文件内容：")
    Source.fromFile("data/test.txt").foreach { 
      print
    }
  }
}