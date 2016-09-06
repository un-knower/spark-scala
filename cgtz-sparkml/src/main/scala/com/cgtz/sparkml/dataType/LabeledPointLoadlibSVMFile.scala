package com.cgtz.sparkml.dataType

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.mllib.util.MLUtils

/**
 * Spark中组件Mllib的学习14之从文本中读取带标签的数据，生成带label的向量
 */
object LabeledPointLoadlibSVMFile {
  def main(args: Array[String]) {
    val conf = new SparkConf().setMaster("local").setAppName(this.getClass().getSimpleName().filter(!_.equals('$')))
    //设置环境变量
    val sc = new SparkContext(conf)

    val mu = MLUtils.loadLibSVMFile(sc, "data/sample_libsvm_data.txt") //读取文件
    mu.foreach(println) //打印内容
    sc.stop
  }
}