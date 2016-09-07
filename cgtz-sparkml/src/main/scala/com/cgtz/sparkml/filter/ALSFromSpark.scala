package com.cgtz.sparkml.filter

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.mllib.recommendation.Rating
import org.apache.spark.mllib.recommendation.ALS
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel
import java.text.SimpleDateFormat
import java.util.Date

/**
 * Spark中组件Mllib的学习5之ALS测试（apache spark）
 */
object ALSFromSpark {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName(this.getClass().getSimpleName().filter(!_.equals('$')))
    //设置环境变量
    val sc = new SparkContext(conf)
    val data = sc.textFile("data/ALSFromSpark.data")

    val ratings = data.map(_.split(',') match {
      case Array(user, item, rate) =>
        //转化为专用的rating
        Rating(user.toInt, item.toInt, rate.toDouble)
    })

    val rank = 10
    val numIterations = 10
    val model = ALS.train(ratings, rank, numIterations, 0.01)

    // Evaluate the model on rating data
    val usersProducts = ratings.map {
      case Rating(user, product, rate) =>
        (user, product)
    }
    val predictions =
      model.predict(usersProducts).map {
        case Rating(user, product, rate) =>
          ((user, product), rate)
      }
    val ratesAndPreds = ratings.map {
      case Rating(user, product, rate) =>
        ((user, product), rate)
    }.join(predictions)
    val MSE = ratesAndPreds.map {
      case ((user, product), (r1, r2)) =>
        val err = (r1 - r2)
        err * err
    }.mean()
    println("Mean Squared Error = " + MSE)

    // Save and load model
    model.save(sc, "hdfs://172.16.33.66:8020/model/ALSFromSpark")
    val sameModel = MatrixFactorizationModel.load(sc, "hdfs://172.16.33.66:8020/model/ALSFromSpark")

     println("user 2 ,top 1")
    var rs = sameModel.recommendProducts(2, 1)
    rs.foreach { println _ }

    println("user 2 ,top 2")
    rs = sameModel.recommendProducts(2, 2)
    rs.foreach { println _ }

    println("user 2 ,top 3")
    rs = sameModel.recommendProducts(2, 3)
    rs.foreach { println _ }

    println("user 2 ,top 4")
    rs = sameModel.recommendProducts(2, 4)
    rs.foreach { println _ }

    println("user 2 ,top 5")
    rs = sameModel.recommendProducts(2, 5)
    rs.foreach { println _ }

    println(sameModel.predict(2, 1))
    println(sameModel.predict(2, 2))
    println(sameModel.predict(2, 3))
    println(sameModel.predict(2, 4))
  }
}