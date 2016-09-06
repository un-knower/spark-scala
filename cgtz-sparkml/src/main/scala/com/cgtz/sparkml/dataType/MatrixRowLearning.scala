package com.cgtz.sparkml.dataType

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.mllib.linalg.distributed.IndexedRow
import org.apache.spark.mllib.linalg.distributed.IndexedRowMatrix
import org.apache.spark.mllib.linalg.distributed.CoordinateMatrix
import org.apache.spark.mllib.linalg.distributed.MatrixEntry

/**
 * Spark中组件Mllib的学习16之分布式行矩阵的四种形式
 * 分布式行矩阵有：基本行矩阵、index 行矩阵、坐标行矩阵、块行矩阵
 * 功能一次增加
 */
object MatrixRowLearning {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName(this.getClass().getSimpleName().filter(!_.equals('$')))
    val sc = new SparkContext(conf)

    println("First:Matrix ")
    val rdd = sc.textFile("data/MatrixRow.txt") //创建RDD文件路径
      .map(_.split(' ') //按“ ”分割
        .map(_.toDouble)) //转成Double类型
      .map(line => Vectors.dense(line)) //转成Vector格式
    val rm = new RowMatrix(rdd) //读入行矩阵
    //疑问：如何打印行矩阵所有值，如何定位？
    println(rm.numRows()) //打印列数
    println(rm.numCols()) //打印行数
    rm.rows.foreach(println)
    
    println("Second:index Row Matrix ")
    val rdd2 = sc.textFile("data/MatrixRow.txt") //创建RDD文件路径
      .map(_.split(' ') //按“ ”分割
      .map(_.toDouble)) //转成Double类型
      .map(line => Vectors.dense(line)) //转化成向量存储
      .map((vd) => new IndexedRow(vd.size,vd))//转化格式
    val irm = new IndexedRowMatrix(rdd2) //建立索引行矩阵实例
    println(irm.getClass) //打印类型
    irm.rows.foreach(println) //打印内容数据
    //如何定位？
    
     println("Third: Coordinate Row Matrix ")
    val rdd3 = sc.textFile("data/MatrixRow.txt") //创建RDD文件路径
      .map(_.split(' ') //按“ ”分割
      .map(_.toDouble)) //转成Double类型
      .map(vue => (vue(0).toLong, vue(1).toLong, vue(2))) //转化成坐标格式
      .map(vue2 => new MatrixEntry(vue2 _1, vue2 _2, vue2 _3)) //转化成坐标矩阵格式
    val crm = new CoordinateMatrix(rdd3) //实例化坐标矩阵
    crm.entries.foreach(println) //打印数据
    println(crm.numCols())
    println(crm.numCols())
    //    Return approximate number of distinct elements in the RDD.
    println(crm.entries.countApproxDistinct())
    sc.stop
  }
}