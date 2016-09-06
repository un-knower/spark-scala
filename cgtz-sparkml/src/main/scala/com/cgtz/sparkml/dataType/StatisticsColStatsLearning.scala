package com.cgtz.sparkml.dataType

import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.mllib.stat.Statistics

/**
 * Spark中组件Mllib的学习17之colStats:以列为基础计算统计量的基本数据
 * 解释
 * colStats:以列为基础计算统计量的基本数据
 */
object StatisticsColStatsLearning {
  def main(args: Array[String]) {
    val conf = new SparkConf().setMaster("local[4]").setAppName(this.getClass().getSimpleName().filter(!_.equals('$')))
    val sc = new SparkContext(conf)
    val rdd = sc.textFile("data/stats.txt") //读取文件
      .map(_.split(' ') //按“ ”分割
        .map(_.toDouble)) //转成Double类型
      .map(line => Vectors.dense(line))
    val summary = Statistics.colStats(rdd) //获取Statistics实例

    //    rdd.foreach(each => print(each + " "))
    rdd.foreach(println)
    println("rdd.count:" + rdd.count())
    println()
    println(summary)
    println(summary.max) //最大
    println(summary.min) //最小
    println("count" + summary.count) //个数
    println(summary.numNonzeros) //非零
    println("variance:" + summary.variance) //方差
    println(summary.mean) //计算均值
    println(summary.variance) //计算标准差
    println(summary.normL1) //计算曼哈段距离:相加
    println(summary.normL2) //计算欧几里得距离：平方根

    //    /行向量
    println("\n row Vector:")
    val vec = Vectors.dense(1, 2, 3, 4, 5)
    println(vec)
    println(vec.size)
    println(vec.numActives)
    //    println(vec.variance)//不存在

    sc.stop
  }
}