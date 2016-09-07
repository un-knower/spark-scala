package com.cgtz.sparkml.dataType
import org.apache.spark.mllib.linalg.Vectors

/**
 * Spark中组件Mllib的学习12之密集向量和稀疏向量的生成
 */
object VectorLearning {
  def main(args: Array[String]): Unit = {
    val vd = Vectors.dense(2, 0, 6)
    println(vd(2))
    println(vd)
    
    //数据个数，序号，value
    val vs = Vectors.sparse(4, Array(0, 1, 2, 3), Array(9, 5, 2, 7))
    println(vs(2))
    println(vs)

    val vs2 = Vectors.sparse(4, Array(0, 2, 1, 3), Array(9, 5, 2, 7))
    println(vs2(2))
    println(vs2)
  }
}