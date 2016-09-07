package com.cgtz.sparkml.dataType

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

/**
 *  Spark中组件Mllib的学习19之分层抽样
 *  解释
 * 分层抽样的概念就不讲了，具体的操作：
 * RDD有个操作可以直接进行抽样：sampleByKey和sample等，这里主要介绍这两个
 * （1）将字符串长度为2划分为层1和层2，对层1和层2按不同的概率进行抽样
 *
 *
 * val fractions: Map[Int, Double] = (List((1, 0.2), (2, 0.8))).toMap //设定抽样格式
 * sampleByKey(withReplacement = false, fractions, 0)
 * fractions表示在层1抽0.2，在层2中抽0.8
 * withReplacement false表示不重复抽样
 * 0表示随机的seed
 */
object StratifiedSamplingLearning {
  def main(args: Array[String]) {
    val conf = new SparkConf().setMaster("local[4]").setAppName(this.getClass().getSimpleName().filter(!_.equals('$')))
    val sc = new SparkContext(conf)

    println("First:")
    val data = sc.textFile("data/StratifiedSampling.txt") //读取数
      .map(row => {
        //开始处理
        if (row.length == 3) //判断字符数
          (row, 1) //建立对应map
        else (row, 2) //建立对应map
      }).map(each => (each._2, each._1))
    data.foreach(println)

    println("sampleByKey:抽样后：")
    val fractions = (List((1, 0.2), (2, 0.8))).toMap //设定抽样格式
    val approxSample = data.sampleByKey(withReplacement = false, fractions, 0) //计算抽样样本
    approxSample.foreach(println)

    println
    println("Second:")
    //http://homepage.cs.latrobe.edu.au/zhe/ZhenHeSparkRDDAPIExamples.html#sampleByKey
    val randRDD = sc.parallelize(List((7, "cat"), (6, "mouse"), (7, "cup"), (6, "book"), (7, "tv"), (6, "screen"), (7, "heater")))
    val sampleMap = List((7, 0.4), (6, 0.6)).toMap
    val sample2 = randRDD.sampleByKey(false, sampleMap, 0).collect
    println("sampleByKey:抽样后：")
    sample2.foreach(println)

    println
    println("Third:")
    //http://bbs.csdn.net/topics/390953396
    val a = sc.parallelize(1 to 20, 3)
    val b = a.sample(true, 0.8, 0)
    val c = a.sample(false, 0.8, 0)
    println("RDD a : " + a.collect().mkString(" , "))
    println("RDD b : " + b.collect().mkString(" , "))
    println("RDD c : " + c.collect().mkString(" , "))
    sc.stop
  }
}