package com.cgtz.sparkstreaming.start

import org.apache.spark.SparkConf
import org.apache.spark.streaming.StreamingContext
import org.scalatest.time.Second
import org.apache.spark.streaming.Seconds

/**
 * 使用Scala开发集群运行的Spark 在线黑名单过滤程序
 *
 *
 * 背景描述：在广告点击计费系统中，我们在线过滤掉黑名单的点击，进而保护广告商的利益，只进行有效的广告点击计费
 * 或者在防刷评分（或者流量）系统（在电商网站，为了排名去请人刷评分或者流量），通过用户ip和信息，过滤掉无效的投票或者评分或者流量；
 * 实现技术：首先由黑名单ip库，我们这里用个集合。黑名单的内容转成rdd，使用transform Api直接基于RDD编程，进行join操作，如果黑名单比较多，要做广播变量。
 *
 */
object BlackListTest {
  def main(args: Array[String]): Unit = {
    /**
     * 第1步：创建Spark的配置对象SparkConf，设置Spark程序的运行时的配置信息，
     * 例如说通过setMaster来设置程序要链接的Spark集群的Master的URL,如果设置
     * 为local，则代表Spark程序在本地运行，特别适合于机器配置条件非常差（例如
     * 只有1G的内存）的初学者       *
     */
    val conf = new SparkConf() //创建SparkConf对象
    conf.setAppName("OnlineBlackListFilter") //设置应用程序的名称，在程序运行的监控界面可以看到名称
    conf.setMaster("local") //此时，程序在Spark集群

    val ssc = new StreamingContext(conf, Seconds(30))

    /**
     * 黑名单数据准备，实际上黑名单一般都是动态的，例如在Redis或者数据库中，黑名单的生成往往有复杂的业务
     * 逻辑，具体情况算法不同，但是在Spark Streaming进行处理的时候每次都能够访问完整的信息
     */
    val blackList = Array(("hadoop", true), ("mahout", true))
    //把黑名单转成rdd，8是随便写的
    val blackListRDD = ssc.sparkContext.parallelize(blackList, 8)
    //获取数据
    val adsClickStream = ssc.socketTextStream("localhost", 9999)

    /**
     * 此处模拟的广告点击的每条数据的格式为：time、name
     * 此处map操作的结果是name、（time，name）的格式
     * 因为过滤黑名单，肯定要把名字弄出来，也要保证广告点击的完整内容，
     */
    val adsClickStreamFormatted = adsClickStream.map(ads => (ads.split(" ")(1), ads))
    adsClickStreamFormatted.transform(userClickRDD => {
      //leftOuterJoin右侧有这个元素，会key，value的方式返回true，没有返回false，左边都会存在。
      //通过leftOuterJoin操作既保留了左侧用户广告点击内容的RDD的所有内容，又获得了相应点击内容是否在黑名单中
      val joinedBlackListRDD = userClickRDD.leftOuterJoin(blackListRDD)

      /**
       * 下面把黑名单的内容过滤出去。进行filter过滤的时候，其输入元素是一个Tuple：（name,((time,name), boolean)）
       * 其中第一个元素是黑名单的名称，第二元素的第二个元素是进行leftOuterJoin的时候是否存在该值
       * 如果存在的话，表明当前广告点击是黑名单，需要过滤掉，否则的话则是有效点击内容；
       */
      val validClicked = joinedBlackListRDD.filter(joinedItem => {
        if (joinedItem._2._2.getOrElse(false)) { //第二个元素的第2个元素即黑名单，默认返回false
          false //黑名单为true，说明是黑名单，这里就不要它，表明leftOuterJoin的时候有这个元素。
        } else {
          true //过滤掉
        }
      })

      //遍历有效点击的元素
      validClicked.map(validClick => { validClick._2._1 })
    }).print()

    /**
     * 计算后的有效数据一般都会写入Kafka中，下游的计费系统会从kafka中pull到有效数据进行计费
     */
    ssc.start()
    ssc.awaitTermination()

  }
}