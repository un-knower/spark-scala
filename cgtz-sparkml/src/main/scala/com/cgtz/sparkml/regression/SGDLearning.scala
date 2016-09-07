package com.cgtz.sparkml.regression

import scala.collection.mutable.HashMap
/**
 * Spark中组件Mllib的学习23之随机梯度下降（SGD）
 * sgd解决了梯度下降的两个问题： 收敛速度慢和陷入局部最优。
 * 具体的介绍请见【4】、【5】和【6】
 *
 * 背景：
 * 梯度下降法的缺点是：
 * 靠近极小值时速度减慢。
 * 直线搜索可能会产生一些问题。
 * 可能会’之字型’地下降。
 *
 * 随机梯度下降法stochastic gradient descent，也叫增量梯度下降
 * 由于梯度下降法收敛速度慢，而随机梯度下降法会快很多
 *
 * –根据某个单独样例的误差增量计算权值更新，得到近似的梯度下降搜索（随机取一个样例）
 *
 * –可以看作为每个单独的训练样例定义不同的误差函数
 *
 * –在迭代所有训练样例时，这些权值更新的序列给出了对于原来误差函数的梯度下降的一个合理近似
 *
 * –通过使下降速率的值足够小，可以使随机梯度下降以任意程度接近于真实梯度下降
 *
 * •标准梯度下降和随机梯度下降之间的关键区别
 *
 * –标准梯度下降是在权值更新前对所有样例汇总误差，而随机梯度下降的权值是通过考查某个训练样例来更新的
 *
 * –在标准梯度下降中，权值更新的每一步对多个样例求和，需要更多的计算
 *
 * –标准梯度下降，由于使用真正的梯度，标准梯度下降对于每一次权值更新经常使用比随机梯度下降大的步长
 *
 * –如果标准误差曲面有多个局部极小值，随机梯度下降有时可能避免陷入这些局部极小值中
 */
object SGDLearning {
  val data = HashMap[Int,Int]()
  //创建数据集
  def getData():HashMap[Int,Int] ={
    //生成数据集内容
    for(i<-1 to 50){
      //创建50个数据
      data+=(i->(20*i))//写入公式y=2x
    }
    data//返回数据集
  }
  
  var θ: Double = 0
  //第一步假设θ为0
  var α: Double = 0.5 //设置步进系数

  def sgd(x: Double, y: Double) = {
    //设置迭代公式
    θ = θ - α * ((θ * x) - y) //迭代公式
  }

  def main(args: Array[String]) {
    val dataSource = getData() //获取数据集
    println("data:")
    dataSource.foreach(each => print(each + " "))
    println("\nresult:")
    var num = 1;
    dataSource.foreach(myMap => {
      //开始迭代
      println(num + ":" + θ+" ("+myMap._1+","+myMap._2+")")
      sgd(myMap._1, myMap._2) //输入数据
      num = num + 1;
    })
    println("最终结果θ值为 " + θ) //显示结果
  }
}