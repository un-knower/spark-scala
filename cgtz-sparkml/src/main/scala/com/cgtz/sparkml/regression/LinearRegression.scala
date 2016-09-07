package com.cgtz.sparkml.regression

import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import java.text.SimpleDateFormat
import org.apache.spark.mllib.regression.LinearRegressionWithSGD
import java.util.Date

/**
 * Spark中组件Mllib的学习25之线性回归2-较大数据集（多元）
 * 对多组数据进行model的training,然后再利用model来predict具体的值
 * 。过程中有输出model的权重
 * 公式：f(x)=a1X1+a2X2+a3X3+……
 */
object LinearRegression {
  def main(args: Array[String]): Unit = {
    // 屏蔽不必要的日志显示终端上
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

    // 设置运行环境
    val conf = new SparkConf().setAppName(this.getClass().getSimpleName().filter(!_.equals('$'))).setMaster("local[4]")
    val sc = new SparkContext(conf)

    // Load and parse the data
    val data = sc.textFile("data/lpsa.data", 1)
    //如果读入不加1，会产生两个文件，应该是默认生成了两个partition
    val parsedData = data.map { line =>
      val parts = line.split(',')
      LabeledPoint(parts(0).toDouble, Vectors.dense(parts(1).split(' ').map(_.toDouble)))
    }

    //建立model的数据和predict的数据没有分开
    val numIterations = 100
    val model = LinearRegressionWithSGD.train(parsedData, numIterations)
    // Evaluate model on training examples and compute training error
    val valuesAndPreds = parsedData.map { point =>
      val prediction = model.predict(point.features)
      (point.label, prediction)
    }
    //print model.weights
    var weifhts = model.weights
    println("model.weights" + weifhts)

    //save as file
//    val iString = new SimpleDateFormat("yyyyMMddHHmmssSSS").format(new Date())
//    val path = "output/LinearRegression/" + iString + "/result"
//    valuesAndPreds.saveAsTextFile(path)
    val MSE = valuesAndPreds.map { case (v, p) => math.pow((v - p), 2) }.reduce(_ + _) / valuesAndPreds.count
    println("training Mean Squared Error = " + MSE)
    println(model.predict(Vectors.dense(-1.63735562648104,-2.00621178480549,-1.86242597251066,-1.02470580167082,-0.522940888712441,-0.863171185425945,-1.04215728919298,-0.864466507337306)))

    sc.stop()
  }
}