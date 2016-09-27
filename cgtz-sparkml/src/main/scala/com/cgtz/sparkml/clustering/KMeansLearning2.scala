package com.cgtz.sparkml.clustering

import org.apache.spark.mllib.clustering.{ KMeans, KMeansModel }
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.mllib.util.MLUtils

/**
 * 在本文中，我们所用到目标数据集是来自 UCI Machine Learning Repository 的 Wholesale customer Data Set。
 * UCI 是一个关于机器学习测试数据的下载中心站点，里面包含了适用于做聚类，分群，回归等各种机器学习问题的数据集。
 * Wholesale customer Data Set 是引用某批发经销商的客户在各种类别产品上的年消费数。
 * 为了方便处理，本文把原始的 CSV 格式转化成了两个文本文件，分别是训练用数据和测试用数据
 *
 *
 * 1）FRESH 新鲜：年度开支（MU）新产品（连续）；
 * 2）MILK 牛奶：年度开支（MU）对奶制品（连续）；
 * 3）GROCERY 食品：年支出（MU）杂货产品（连续）；
 * 4）FROZEN 冷冻：年度开支（MU）对冷冻产品（连续）
 * 5）detergents_paper：年度开支（MU）在洗涤剂和纸制品（连续）
 * 6）DELICATESSEN 熟食：年度开支（MU）和熟食产品（连续）；
 * 7）CHANNEL 渠道：客户â€™通道- Horeca（酒店/餐厅/咖啡厅Ã©）或零售渠道（名义）
 * 8）REGION 区域：客户â€™区â€”lisnon，波尔图或其他（名义）
 */
object KMeansLearning2 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[4]").setAppName(this.getClass().getSimpleName().filter(!_.equals('$')))
    val sc = new SparkContext(conf)

    /**
     * Channel Region Fresh Milk Grocery Frozen Detergents_Paper Delicassen
     * 2 3 12669 9656 7561 214 2674 1338
     * 2 3 7057 9810 9568 1762 3293 1776
     * 2 3 6353 8808 7684 2405 3516 7844
     */
    val rawTrainingData = sc.textFile("data/Wholesale customers data.txt")
    // 数据处理
    val parsedTrainingData = rawTrainingData.filter(!isColumnNameLine(_)).map(line => {
      Vectors.dense(line.split(",").map(_.trim()).filter(!"".equals(_)).map(_.toDouble))
    }).cache()
    
    val splits = parsedTrainingData.randomSplit(Array(0.6, 0.4), seed = 3L)
    val parsedData = splits(0) //分割训练数据
    val parseTtest = splits(1) //分割测试数据
    
    // 最大分类数
    val numClusters = 8
    // 迭代次数
    val numIterations = 20
    // 算法被运行的次数
    val runTimes = 3

    var clusterIndex = 0
    val clustersModel = KMeans.train(parsedData, numClusters, numIterations, runTimes)
    println("Cluster Number:" + clustersModel.clusterCenters.length)
    println("Cluster Centers Information Overview:")
    clustersModel.clusterCenters.foreach(
      x => {
        println("Center Point of Cluster " + clusterIndex + ":")
        println(x)
        clusterIndex += 1
      })

    // 如何选择 K
    // 前面提到 K 的选择是 K-means 算法的关键，Spark MLlib 在 KMeansModel 类里提供了 computeCost 方法，
    // 该方法通过计算所有数据点到其最近的中心点的平方和来评估聚类的效果。
    // 一般来说，同样的迭代次数和算法跑的次数，这个值越小代表聚类的效果越好。
    // 但是在实际情况下，我们还要考虑到聚类结果的可解释性，
    // 不能一味的选择使 computeCost 结果值最小的那个 K。
    val ks: Array[Int] = Array(3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20)
    ks.foreach(cluster => {
      val model: KMeansModel = KMeans.train(parsedTrainingData, cluster, 30, 1)
      val ssd = model.computeCost(parsedTrainingData)
      println("sum of squared distances of points to their nearest center when k=" + cluster + " -> " + ssd)
    })

    //begin to check which cluster each test data belongs to based on the clustering result
    parseTtest.collect().foreach(testDataLine => {
      val predictedClusterIndex = clustersModel.predict(testDataLine)
      println("The data " + testDataLine.toString + " belongs to cluster " + predictedClusterIndex)
    })
    println("Spark MLlib K-means clustering test finished.")
  }

  private def isColumnNameLine(line: String): Boolean = {
    if (line != null && line.contains("Channel"))
      true
    else
      false
  }
}