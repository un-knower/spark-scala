package com.cgtz.spark

import scala.util.control.Breaks

object FuHao {
  def main(args: Array[String]): Unit = {
    var a = 0
    var b = 0
    
    val numList = List(1,2,3,3,4,465,5,34,3456)
    
    var retVal = for(a<-numList if a!=3 if a>100)yield a
    for(a <- retVal)
      println(a)
    
    for(a<-numList if a!=3 if a<100)
      println(a)
    
    for(a<- 1 to 3 ; b<- 1 to 4)
      println(a)
    
    val numList1 = List(1, 2, 3, 4, 5)
    val numList2 = List(11, 12, 13)

    val outer = new Breaks
    val inner = new Breaks

    outer.breakable {
      for (a <- numList1)
        println(a)
      inner.breakable {
        for (b <- numList2) {
          println(b)
          if (b == 12)
            inner.break
        }
      }
    }
  }
}