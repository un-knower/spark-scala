package com.cgtz.sparkml.dataType

import org.apache.spark.mllib.linalg.Matrices

/**
 * Spark中组件Mllib的学习15之创建分布式矩阵
 */
object MatrixLearning {
  def main(args: Array[String]): Unit = {
    val mx = Matrices.dense(2, 3, Array(1, 2, 3, 4, 5, 6)) ////创建一个分布式矩阵
    println(mx) //打印结果

    val arr = (1 to 6).toArray.map(_.toDouble)
    val mx2 = Matrices.dense(2, 3, arr) //创建一个分布式矩阵
    println(mx2) //打印结果

    val arr3 = (1 to 20).toArray.map(_.toDouble)
    val mx3 = Matrices.dense(4, 5, arr3) //创建一个分布式矩阵
    println(mx3) //打印结果
    println(mx3.numRows)
    println(mx3.numCols)
  }
}