package com.cgtz.sparkml.filter

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.mllib.recommendation.Rating
import org.apache.spark.mllib.recommendation.ALS
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel

/**
 * Spark中组件Mllib的学习7之ALS隐式转换训练的model来预测数据
 * 结果不准
 */
object ALSImplicitFromSpark {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName(this.getClass().getSimpleName().filter(!_.equals('$')))
    //设置环境变量
    val sc = new SparkContext(conf)

    //load data
    val data = sc.textFile("data/CollaborativeFilter.txt")

    val ratings = data.map(_.split(",") match {
      case Array(user, item, data) =>
        Rating(user.toInt, item.toInt, data.toDouble)
    })

    // Build the recommendation model using ALS
    val rank = 10
    val numIterations = 10

    val alpha = 0.01
    val lambda = 0.01
    val model = ALS.trainImplicit(ratings, rank, numIterations, lambda, alpha)

    // Evaluate the model on rating data
    val userProducts = ratings.map {
      case Rating(user, product, rate) =>
        (user, product)
    }
    val predictions = model.predict(userProducts).map {
      case Rating(user, product, rate) =>
        ((user, product), rate)
    }
    val rateAndPreds = ratings.map {
      case Rating(user, product, rate) =>
        ((user, product), rate)
    }.join(predictions)
    val MSE = rateAndPreds.map {
      case ((user, product), (r1, r2)) =>
        val err = (r1 - r2)
        err * err
    }.mean()
    println("Mean Squared Error = " + MSE)
    
    // Save and load model
    model.save(sc, "hdfs://172.16.33.66:8020/model/ALSImplicitFromSpark")
    val sameModel = MatrixFactorizationModel.load(sc, "hdfs://172.16.33.66:8020/model/ALSImplicitFromSpark")
    
    /**
      * recommend
      */
    val rs =sameModel.recommendProducts(1,4)
    rs.foreach(println)
  }
}