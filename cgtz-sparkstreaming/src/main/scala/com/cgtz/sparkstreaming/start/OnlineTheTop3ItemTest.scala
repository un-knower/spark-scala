package com.cgtz.sparkstreaming.start

import org.apache.spark.streaming.StreamingContext
import org.apache.spark.SparkConf
import org.apache.spark.streaming.Seconds
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.hive.HiveContext
import scalaz.IsEmpty


/**
 * 使用Spark Streaming+Spark SQL来在线动态计算电商中不同类别中最热门的商品排名，例如手机这个类别下面最热门的三种手机、电视这个类别
 * 下最热门的三种电视，该实例在实际生产环境下具有非常重大的意义；
 *   实现技术：Spark Streaming+Spark SQL，之所以Spark Streaming能够使用ML、sql、graphx等功能是因为有foreachRDD和Transform
 * 等接口，这些接口中其实是基于RDD进行操作，所以以RDD为基石，就可以直接使用Spark其它所有的功能，就像直接调用API一样简单。
 *  假设说这里的数据的格式：user item category，例如Rocky Samsung Android
 */
object OnlineTheTop3ItemTest {
  def main(args: Array[String]): Unit = {
    /**
     * 第1步：创建Spark的配置对象SparkConf，设置Spark程序的运行时的配置信息，
     * 例如说通过setMaster来设置程序要链接的Spark集群的Master的URL,如果设置
     * 为local，则代表Spark程序在本地运行，特别适合于机器配置条件非常差（例如
     * 只有1G的内存）的初学者       *
     */
    val conf = new SparkConf() //创建SparkConf对象
    conf.setAppName("OnlineTheTop3ItemTest") //设置应用程序的名称，在程序运行的监控界面可以看到名称
    //    conf.setMaster("spark://Master:7077") //此时，程序在Spark集群
    conf.setMaster("local[6]")
    //设置batchDuration时间间隔来控制Job生成的频率并且创建Spark Streaming执行的入口
    val ssc = new StreamingContext(conf, Seconds(5))

    ssc.checkpoint("/root/Documents/SparkApps/checkpoint")

    val userClickLogsDStream = ssc.socketTextStream("Master", 9999)

    val formattedUserClickLogsDStream = userClickLogsDStream.map(clickLog => {
      (clickLog.split(" ")(2) + "_" + clickLog.split(" ")(1), 1)
    })
    // 任意一种商品在过去60秒钟被点击了多少次，由于key是类型category和item的组合体，所以接下来可把商品分成不同的类别，然后计算出每种商品和最热门的商品
    // val categoryUserClickLogsDStream = formattedUserClickLogsDStream.reduceByKeyAndWindow((v1:Int, v2: Int) => v1 + v2,
    //      (v1:Int, v2: Int) => v1 - v2, Seconds(60), Seconds(20))
    //这里用_+_ , _-_,这种占位符的方式，简化上面的写法。这里得到的是tuple，过去60秒内每种商品被点击的次数
    val categoryUserClickLogsDStream = formattedUserClickLogsDStream.
      reduceByKeyAndWindow(_ + _, _ - _, Seconds(60), Seconds(20))

    //在上面的基础上用foreachRDD结合spark sql进行处理
    categoryUserClickLogsDStream.foreachRDD(rdd => {
      if (rdd.isEmpty()) {
        println("nothing to do!!")
      } else {
        val categoryItemRow = rdd.map(reduceItem => {
          val category = reduceItem._1.split("_")(0)
          val item = reduceItem._1.split("_")(1)
          val click_count = reduceItem._2
          Row(category, item, click_count)
        })
        //构建DataFrame用下面代码
        val structType = StructType(Array(
          StructField("category", StringType, true),
          StructField("item", StringType, true),
          StructField("click_count", IntegerType, true)))
        val hiveContext = new HiveContext(rdd.context)
        //真正创建DataFrame
        val categoryItemDF = hiveContext.createDataFrame(categoryItemRow, structType)
        //注册一个表，用sql去操作
        categoryItemDF.registerTempTable("categoryItemTable")
        //拿到的结果是DataFrame
        val resultDateFrame = hiveContext.sql("SELECT category,item,click_count FROM (SELECT category,item,click_count,row_number()" +
          " OVER (PARTITION BY category ORDER BY click_count DESC) rank FROM categoryItemTable) subquery " +
          " WHERE rank <= 3")
        resultDateFrame.show()
        //把DF变成RDD
        val resultRowRDD = resultDateFrame.rdd

        resultRowRDD.foreachPartition { partitionOfRecords =>
          {
            //这里必须做非空判断，否则为null的时候，会报错，而且rdd不为null，partition可能为null，
            //因为不能确保60秒中的每5秒都有数据
            if (partitionOfRecords.isEmpty) {
              println("This RDD is not null but partition is null")
            } else {
              // ConnectionPool is a static, lazily initialized pool of connections
              //val connection = ConnectionPool.getConnection()
              //partitionOfRecords.foreach { record =>
              //  {
              //    val sql = "insert into categorytop3(category,item,client_count) values('" + record.getAs("category") + "','" +
              //      record.getAs("item") + "'," + record.getAs("click_count") + ")"
              //    val stmt = connection.createStatement();
              //    stmt.executeUpdate(sql);
              //  }
              //}
              //ConnectionPool.returnConnection(connection) // return to the pool for future reuse
            }
          }
        }
      }
    })
    ssc.start()
    ssc.awaitTermination()
  }
}