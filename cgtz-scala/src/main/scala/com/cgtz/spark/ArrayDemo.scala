package com.cgtz.spark


object ArrayDemo {
  //如果后面没有默认的数组值，则需要[String]来制定类型
  //因为不指定则无法知道数组类型
  //所以函数的形式参数是需要类型的
  var z = new Array[String](3)
  z(0) = "1111"
  z(1) = "2222"
  z(2) = "3333"
  
  var arr = Array("nihao","bitch","noshi")
  
  def main(args: Array[String]): Unit = {
//    oneArray()
//    conArary()
    rangeArray()
  }
  def oneArray():Unit={
    var myList = Array(1.9, 2.9, 3.4, 3.5)
    
    for(x<-myList)
      println(x)
      
    var total = 0.0
    for(i<- 0 to myList.length-1)
      total+=myList(i)
    println(total)
    
    var max = myList(0)
    for(i<-1 to myList.length-1){
      if(myList(i)>max)
        max = myList(i)
    }
    println(max)
  }
  def conArary(): Unit = {
    var myList1 = Array(1.9, 2.9, 3.4, 3.5)
    var myList2 = Array(8.9, 7.9, 0.4, 1.5)
    
    var myList3 =Array.concat(myList1,myList2)
    
    for(x<-myList3)
      println(x)
  }
  def rangeArray():Unit={
    var myList1 = Array.range(10, 20, 2)
    var myList2 = Array.range(10, 20)

    // 输出所有数组元素
    for (x <- myList1) {
      print(" " + x)
    }
    println()
    for (x <- myList2) {
      print(" " + x)
    }
  }
}