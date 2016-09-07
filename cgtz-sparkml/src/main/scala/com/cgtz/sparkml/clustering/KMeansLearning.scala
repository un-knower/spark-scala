package com.cgtz.sparkml.clustering

import org.apache.spark.mllib.clustering.{ KMeans, KMeansModel }
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.mllib.util.MLUtils

/**
 * kmeans聚类的算法
 */
object KMeansLearning {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[4]").setAppName(this.getClass().getSimpleName().filter(!_.equals('$')))
    val sc = new SparkContext(conf)

    //val data = MLUtils.loadLibSVMFile(sc, "data/kmeans.txt")
    val data = sc.textFile("data/kmeans.txt")
    // 数据处理
    val parsedData = data.map(s => Vectors.dense(s.split('\t').map(_.toDouble))).cache()
    // 最大分类数
    val numClusters = 3
    // 迭代次数
    val numIterations = 20
    // 训练模型
    val clusters = KMeans.train(parsedData, numClusters, numIterations)
    // 评估模型
    val WSSSE = clusters.computeCost(parsedData)
    println("Within Set Sum of Squared Errors = " + WSSSE)
    clusters.clusterCenters.foreach(println)
  }
}