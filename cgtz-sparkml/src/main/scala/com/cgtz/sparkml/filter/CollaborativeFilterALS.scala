package com.cgtz.sparkml.filter

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.mllib.recommendation.Rating
import org.apache.spark.mllib.recommendation.ALS
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel

/**
 * 基于ALS算法的协同过滤算法
 */
object CollaborativeFilter {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("CollaborativeFilter")
    val sc = new SparkContext(conf)
    
    val data = sc.textFile("data/CollaborativeFilter.txt")
    val ratings = data.map(_.split(',') match {
      //将数据集转换
      case Array(user, item, rate) =>
        //转化为专用的rating
        Rating(user.toInt, item.toInt, rate.toDouble)
    })
    //隐藏因子
    val  rank = 2
    //迭代次数
    val numIterations = 5
    //模型训练
    val model = ALS.train(ratings, rank, numIterations,0.01)
   
//    保存模型
    model.save(sc, "hdfs://172.16.33.66:8020/model/CollaborativeFilter")
//    加载模型
    val sameModel  = MatrixFactorizationModel.load(sc, "hdfs://172.16.33.66:8020/model/CollaborativeFilter")
    var rs = sameModel.recommendProducts(2, 1)
    
    rs.foreach { println _ }
  }
}