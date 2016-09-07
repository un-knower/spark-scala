package com.cgtz.sparkstreaming.start
import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream.DStream.toPairDStreamFunctions
object SscketTest {
  val conf = new SparkConf().setAppName("scala streaming").setMaster("local[2]")
  val ssc = new StreamingContext(conf, Seconds(1))

  def main(args: Array[String]): Unit = {
    val lines = ssc.socketTextStream("172.16.34.82", 10000)
    val words = lines.flatMap(_.split(" "))
    val pairs = words.map(word => (word, 1))
    val wordCounts = pairs.reduceByKey(_ + _)
    wordCounts.print()
    ssc.start()
    ssc.awaitTermination()
  }
}