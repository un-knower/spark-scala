package com.cgtz.spark

object MapDemo {
  def main(args: Array[String]): Unit = {
//    one()
//    two
//    three()
//    four
    five
  }
  def one():Unit={
    var a  = Map[Char,Int]()
    val colors = Map("red"->"111","blue"->"222")
    a += ('a'->12)
    a += ('b'->13)
    a += ('c'->14)
    println(a.isEmpty)
    println(a.keys)
    println(colors.values)
  }
  def two():Unit={
    val colors = Map("red" -> "#FF0000",
      "azure" -> "#F0FFFF",
      "peru" -> "#CD853F")

    val nums = Map()
    println("colors 中的键为 : " + colors.keys)
    println("colors 中的值为 : " + colors.values)
    println("检测 colors 是否为空 : " + colors.isEmpty)
    println("检测 nums 是否为空 : " + nums.isEmpty)
  }
  def three():Unit={
    val colors1 = Map("red" -> "#FF0000",
      "azure" -> "#F0FFFF",
      "peru" -> "#CD853F")
    val colors2 = Map("blue" -> "#0033FF",
      "yellow" -> "#FFFF00",
      "red" -> "#FF0000")
      
    println(colors1++colors2)
  }
  def four():Unit={
    val sites = Map("runoob" -> "http://www.runoob.com",
      "baidu" -> "http://www.baidu.com",
      "taobao" -> "http://www.taobao.com")
      
     sites.keys.foreach {
      i=>print(i)
      println(sites(i)) 
     }
  }
  def five():Unit={
    val sites = Map("runoob" -> "http://www.runoob.com",
      "baidu" -> "http://www.baidu.com",
      "taobao" -> "http://www.taobao.com")

    if (sites.contains("runoob")) {
      println("runoob 键存在，对应的值为 :" + sites("runoob"))
    } else {
      println("runoob 键不存在")
    }
    
    if (sites.contains("baidu")) {
      println("baidu 键存在，对应的值为 :" + sites("baidu"))
    } else {
      println("baidu 键不存在")
    }
    
    if (sites.contains("google")) {
      println("google 键存在，对应的值为 :" + sites("google"))
    } else {
      println("google 键不存在")
    }
  }
}