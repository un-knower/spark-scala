package com.cgtz.sparkml.regression

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LinearRegressionWithSGD

/**
 * Spark中组件Mllib的学习24之线性回归1-小数据集
 * 简单的对6组数据进行model的training,然后再利用model来predict具体的值
 * 。过程中有输出model的权重
 * 公式：f(x)=aX1+bX2
 */
object LinearRegression2Learning {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[4]").setAppName(this.getClass().getSimpleName().filter(!_.equals('$')))
    val sc = new SparkContext(conf)

    val data = sc.textFile("data/lpsa2.data")
    val parsedData = data.map { line => //开始对数据集处理
      val parts = line.split(',') //根据逗号进行分区
      LabeledPoint(parts(0).toDouble, Vectors.dense(parts(1).split(' ').map(_.toDouble)))
    }.cache() //转化数据格式

    val model = LinearRegressionWithSGD.train(parsedData, 100, 0.1) //建立模型
    val result = model.predict(Vectors.dense(2, 1))
    //通过模型预测模型
    println("model weights:")
    println(model.weights)
    println("result:")
    println(result) //打印预测结果
    sc.stop
  }
}