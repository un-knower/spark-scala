package com.cgtz.sparkml.regression

import org.apache.spark.mllib.regression.LinearRegressionWithSGD
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint

/**
 * 线性回归示例：
 * 某种商品的需求量y、价格x、和消费者收入x2
 */
object LinearRegression2 {
  val conf = new SparkConf().setMaster("local").setAppName(this.getClass().getSimpleName().filter(!_.equals('$')))
  val sc = new SparkContext(conf)

  def main(args: Array[String]): Unit = {
    val data = sc.textFile("data/consumer.data")

    val parsedData = data.map { line => //开始对数据集处理
      val parts = line.split('|') //根据逗号进行分区
      LabeledPoint(parts(0).toDouble, Vectors.dense(parts(1).split(',').map(_.toDouble)))
    }.cache() //转化数据格式

    val model = LinearRegressionWithSGD.train(parsedData, 10, 0.1) //建立模型
    val result = model.predict(Vectors.dense(8,30))
    //通过模型预测模型
    println("result:")
    println(result) //打印预测结果
    sc.stop
  }
}