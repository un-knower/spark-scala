package com.cgtz.spark
/**
 * Scala 列表类似于数组，它们所有元素的类型都相同，
 * 但是它们也有所不同：列表是不可变的，值一旦被定义了就不能改变，
 * 其次列表 具有递归的结构（也就是链接表结构）而数组不是。
 */
object ListDemo {
   val site = List("nihao","woshi","liuxing")
   
   val nums = List(12,34,56)
   
   val empty = List()
   
   val dim = List(
     List(1,2,3),
     List(4,5,6),
     List(7,8,9)
   )
   
   val siteS = "aaa"::"bbb"::"ccc"::Nil
   
   val numsI = 1::2::3::Nil
   
   val emptyE = Nil
   
   def main(args: Array[String]): Unit = {
     println(site.head)
     println(site.tail)
     println(site.isEmpty)
     println(empty.isEmpty)
     
     println(site:::nums)
     println(List.concat(site,nums).reverse)
   }
}