package com.cgtz.spark

object SetDemo {
  def main(args: Array[String]): Unit = {
    //one()
    //two()
    //three()
    //four()
    five()
  }
  def one():Unit={
    import scala.collection.mutable.Set
    val set = Set(1, 2, 3)
    println(set.getClass.getName)
    
    set.add(4)
    set.remove(1)
    set+=5
    set-=2
    
    println(set.exists { _ % 2 ==0 })
    println(set)
    val another = set.toSet
    println(another.getClass.getName)
  }
  def two():Unit={
    val site = Set("Runoob", "Google", "Baidu")
    val nums  = Set()

    println("第一网站是 : " + site.head)
    println("最后一个网站是 : " + site.tail)
    println("查看列表 site 是否为空 : " + site.isEmpty)
    println("查看 nums 是否为空 : " + nums.isEmpty)
  }
  def three():Unit={
    val site1 = Set("Runoob", "Google", "Baidu")
    val site2 = Set("Faceboook", "Taobao")
    
    // ++ 作为运算符使用
    var site = site1 ++ site2
    println("site1 ++ site2 : " + site)

    //  ++ 作为方法使用
    site = site1.++(site2)
    println("site1.++(site2) : " + site)
  }
  def four():Unit={
    val num = Set(13,3,5,5,234,56)
    
    println(num)
    println(num.max)
    println(num.min)
  }
  def five():Unit={
    val  num1 = Set(5,6,9,20,30,45)
    val num2 = Set(50,60,9,20,35,55)
    
    println(num1.&(num2))
    println(num1.intersect(num2))
  }
}