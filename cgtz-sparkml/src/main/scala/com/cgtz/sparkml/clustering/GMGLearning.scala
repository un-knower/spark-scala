package com.cgtz.sparkml.clustering

import org.apache.spark.mllib.clustering.{ KMeans, KMeansModel }
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.mllib.clustering.GaussianMixture

/**
 * 混合高斯模型
 */
object GMGLearning {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[4]").setAppName(this.getClass().getSimpleName().filter(!_.equals('$')))
    val sc = new SparkContext(conf)

    val data = sc.textFile("data/gmg.txt")
    // 数据处理
    val parsedData = data.map(s => Vectors.dense(s.trim().split("\t").map(_.toDouble))).cache()

    // 训练模型
    val model = new GaussianMixture().setK(2).run(parsedData)
    
    for(i <- 0 until model.k){
      //逐个打印单个模型
      println("weight%f\nmu=%s\nsigma=\n%s\n" format(model.weights(i),model.gaussians(i).mu,model.gaussians(i).sigma))
    }
  }
}