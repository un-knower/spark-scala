package com.cgtz.sparkml.dataType
import org.apache.spark.mllib.random.RandomRDDs._
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

/**
 * Spark中组件Mllib的学习21之随机数-RandomRDD产生
 * 在org.apache.spark.mllib.random下RandomRDDs对象，
 * 处理生成RandomRDD，还可以生成uniformRDD、poissonRDD、exponentialRDD、gammaRDD等
 */
object RandomRDDLearning {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[4]").setAppName(this.getClass().getSimpleName().filter(!_.equals('$')))
    val sc = new SparkContext(conf)

    println("normalRDD:")
    val randomNum = normalRDD(sc, 10)
    randomNum.foreach(println)
    
    println("uniformRDD:")
    uniformRDD(sc, 10).foreach(println)
    
    println("poissonRDD:")
    poissonRDD(sc, 5, 10).foreach(println)
    
    println("exponentialRDD:")
    exponentialRDD(sc, 7, 10).foreach(println)
    
    println("gammaRDD:")
    gammaRDD(sc, 3, 3, 10).foreach(println)
    
    sc.stop
  }
}