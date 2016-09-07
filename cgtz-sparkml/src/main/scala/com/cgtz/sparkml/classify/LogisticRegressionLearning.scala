package com.cgtz.sparkml.classify

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.mllib.classification.LogisticRegressionWithSGD
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint

/**
 * Spark中组件Mllib的学习之逻辑回归篇
 * 1解释
 * 什么是逻辑回归？
 *
 * Logistic回归与多重线性回归实际上有很多相同之处，最大的区别就在于它们的因变量不同，其他的基本都差不多。正是因为如此，这两种回归可以归于同一个家族，即广义线性模型（generalizedlinear model）。
 *
 * 这一家族中的模型形式基本上都差不多，不同的就是因变量不同。
 *
 * 如果是连续的，就是多重线性回归；
 * 如果是二项分布，就是Logistic回归；
 * 如果是Poisson分布，就是Poisson回归；
 * 如果是负二项分布，就是负二项回归。
 * Logistic回归的因变量可以是二分类的，也可以是多分类的，但是二分类的更为常用，也更加容易解释。所以实际中最常用的就是二分类的Logistic回归。
 *
 * Logistic回归的主要用途：
 *
 * 寻找危险因素：寻找某一疾病的危险因素等；
 * 预测：根据模型，预测在不同的自变量情况下，发生某病或某种情况的概率有多大；
 * 判别：实际上跟预测有些类似，也是根据模型，判断某人属于某病或属于某种情况的概率有多大，也就是看一下这个人有多大的可能性是属于某病。
 */
object LogisticRegressionLearning {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[4]").setAppName(this.getClass().getSimpleName().filter(!_.equals('$')))
    val sc = new SparkContext(conf)

    val data = sc.textFile("data/logisticRegression1.data")
    val parsedData = data.map { line => //开始对数据集处理
      val parts = line.split('|') //根据逗号进行分区
      LabeledPoint(parts(0).toDouble, Vectors.dense(parts(1).split(' ').map(_.toDouble)))
    }.cache() //转化数据格式
    parsedData.foreach(println)
    val model = LogisticRegressionWithSGD.train(parsedData, 50) //建立模型
    val target = Vectors.dense(-1) //创建测试值
    val resulet = model.predict(target) //根据模型计算结果
    println("model.weights:")
    println(model.weights)
    println(resulet) //打印结果
    println(model.predict(Vectors.dense(10)))
    sc.stop
  }
}