package com.cgtz.sparkml.dataType

import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint

/**
 * Spark中组件Mllib的学习13之给向量打标签
 */
object LabeledPointLearning {
  def main(args: Array[String]): Unit = {
    val vd = Vectors.dense(2, 0, 6)
    //对密集向量建立标记点
    val pos = LabeledPoint(1,vd)
    println(pos.features)
    println(pos.label)
    println(pos)

    val vs = Vectors.sparse(4, Array(0, 1, 2, 3), Array(9, 5, 2, 7))
    //对稀疏向量建立标记点
    val neg = LabeledPoint(2, vs)
    println(neg.features)
    println(neg.label)
    println(neg)

  }
}