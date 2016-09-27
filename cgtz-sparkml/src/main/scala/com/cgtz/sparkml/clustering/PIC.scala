package com.cgtz.sparkml.clustering

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.mllib.clustering.PowerIterationClustering

/**
 * 快速迭代聚类
 */
object PIC {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("pic")
    val sc = new SparkContext(conf)
    val data = sc.textFile("data/pic")
    val pic = new PowerIterationClustering().setK(3).setMaxIterations(20)
//    val model = pic.run(data)
  }
}