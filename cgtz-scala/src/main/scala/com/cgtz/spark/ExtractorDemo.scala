package com.cgtz.spark
/**
 * Scala 提取器
 * 提取器是从传递给它的对象中提取出构造该对象的参数
 */
object ExtractorDemo {
  def main(args: Array[String]): Unit = {
    //    println("Apply 方法 : " + ExtractorDemo("Zara", "gmail.com"));
    //    println("Unapply 方法 : " + unapply("Zara@gmail.com"));
    //    println("Unapply 方法 : " + unapply("Zara Ali"));

    val x = 5
    println(x)
    x match {
      case ExtractorDemo(num) => println(x + " 是 " + num + " 的两倍！")
      //unapply 被调用
      case _                  => println("无法计算")
    }
  }
  def apply(x: Int) = x * 2
  def unapply(z: Int): Option[Int] = if (z % 2 == 0) Some(z / 2) else None

  //  //注入方法(可选)
  //  def apply(user: String, domain: String) = {
  //    user + "@" + domain
  //  }
  //  //提取方法
  //  def unapply(str: String): Option[(String, String)] = {
  //    val parts = str.split("@")
  //    if (parts.length == 2) {
  //      Some(parts(0), parts(1))
  //    } else {
  //      None
  //    }
  //  }

}