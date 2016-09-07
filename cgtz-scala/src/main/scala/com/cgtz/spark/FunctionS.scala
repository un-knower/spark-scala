package com.cgtz.spark

import java.util.Date

object FunctionS {
  def main(args: Array[String]): Unit = {
    println(addInt(23,54))
    //没有参数,则可不写
    printMe
    printMeX
    delayed(time)
    println(printlnInt(b=5, a=7))
    printlnStrings("aaaaa","bbbbbbbbbb","cccccccccc")
    println(factorial(10))
    println( factorial(3) )

    val date = new Date
    val logWithDateBound = log(date, _: String)
    logWithDateBound("message1")
    Thread.sleep(1000)
    logWithDateBound("message2")
    Thread.sleep(1000)
    logWithDateBound("message3")
    
    val str1="hello"
    val str2="scala"
    println(stracat(str1)(str2))
  }
  def addInt(a:Int,b:Int):Int={
    a+b
  }
  def printMe():Unit={
    println("hello scala!")
  }
  //省略参数、返回值写法
  def printMeX  = println("hello scala!!!!!!!!!!")
  
  def time()={
    println("get time")
    System.nanoTime
  }
  def delayed(t:Long)={
    println("在 delayed 方法内")
    println("参数： " + t)
    t
  }
  
  def printlnInt(a:Int,b:Int){
    a+b
  }
  
  def printlnStrings(args:String*):Unit={
    var i = 0
    for(a<-args){
      println(a)
    }
  }
  def factorial(n:BigInt) :BigInt={
    if(n<=1)
      1
    else
      n*factorial(n-1)
  }
  /**
   * 高阶函数:高阶函数可以使用其他函数作为参数，或者使用函数作为输出结果
   */
   /**
   * 函数嵌套
   */
   def factorial(i:Int):Int = {
    def fact(i:Int,accumulator:Int):Int={
      if(i<=1)
        accumulator
      else
        fact(i-1,i*accumulator)
    }
    fact(i,1)
  }
  /**
   * 偏函数
   */
   def log(date:Date,message:String):Unit = {
     println(date + "----" + message)
   }
   /**
    * 柯里化
    */
    //def add(x:Int,y:Int)=x+y
   def add(x:Int)(y:Int)=x+y
   def stracat(s1:String)(s2:String)={
     s1+s2
   }
}