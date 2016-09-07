package com.cgtz.sparkml.dataType
import org.apache.spark.mllib.linalg.{ Matrices, Vectors }
import org.apache.spark.mllib.stat.Statistics

/**
 * Spark中组件Mllib的学习20之假设检验-卡方检验
 * 分别对Vector和Matrix进行卡方检验
 * 定义:卡方检验就是统计样本的实际观测值与理论推断值之间的偏离程度，
 * 实际观测值与理论推断值之间的偏离程度就决定卡方值的大小，卡方值越大，
 * 越不符合；卡方值越小，偏差越小，越趋于符合，若两个值完全相等时，卡方值就为0，表明理论值完全符合。
 */
object ChiSqLearning {
  def main(args: Array[String]): Unit = {
    val vd = Vectors.dense(1, 2, 3, 4, 5)
    val vdResult = Statistics.chiSqTest(vd)
    println(vdResult)
    println
    val mtx = Matrices.dense(3, 2, Array(1, 3, 5, 2, 4, 6))
    val mtxResult = Statistics.chiSqTest(mtx)
    println(mtxResult)
    //print :方法、自由度、方法的统计量、p值
  }
}