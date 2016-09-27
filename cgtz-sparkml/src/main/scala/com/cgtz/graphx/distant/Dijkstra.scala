package com.cgtz.graphx.distant

import scala.Iterator

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.graphx.Graph
import org.apache.spark.graphx.Graph.graphToGraphOps
import org.apache.spark.graphx.VertexId
import org.apache.spark.graphx.util.GraphGenerators
import org.apache.spark.graphx.GraphLoader
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx.Edge
import java.util.Date

/**
 * 单源最短路径
 */
object Dijkstra {
  def main(args: Array[String]) {
    val conf = new SparkConf().
//    setMaster("local[3]").
    setAppName("Dijkstra Application")
    val sc = new SparkContext(conf)
    
    val textFile = sc.
    textFile(args(0))
//    textFile("data/dijkstra")
    /**
     * 设置顶点
     */
   val vertexArray1 = textFile.map(line => {
    	(line.split("\t")(0).toLong,("",1))
    }).collect()
    val vertexArray2 = textFile.map(line => {
    	(line.split("\t")(1).toLong,("",1))
    }).collect()
    val vertexArray = vertexArray1++vertexArray2
    /**
     * 设置边
     */
    val edgeArray1 = textFile.map(line=> {
      Edge(line.split("\t")(0).toLong,line.split("\t")(1).toLong,1)
    }).collect()
    val edgeArray2 = textFile.map(line=> {
      Edge(line.split("\t")(1).toLong,line.split("\t")(0).toLong,1)
    }).collect()
    val edgeArray = edgeArray1++edgeArray2
    
    //设置顶点和边，注意顶点和边都是用元组定义的Array
    //顶点的数据类型是VD:(String,Int)
   /* val vertexArray = Array(
      (1L, ("Alice", 28)), //在这里后面的属性("Alice", 28)没有用上。
      (2L, ("Bob", 27)),
      (3L, ("Charlie", 65)),
      (4L, ("David", 42)),
      (5L, ("Ed", 55)),
      (6L, ("Fran", 50)))*/
      
    //边的数据类型ED:Int
    /*val edgeArray = Array(
      Edge(2L, 1L, 7),
      Edge(2L, 4L, 2),
      Edge(3L, 2L, 4),
      Edge(3L, 6L, 3),
      Edge(4L, 1L, 1),
      Edge(5L, 2L, 2),
      Edge(5L, 3L, 8),
      Edge(5L, 6L, 3))*/

    //构造vertexRDD和edgeRDD
    val vertexRDD: RDD[(Long, (String, Int))] = sc.parallelize(vertexArray)
    val edgeRDD: RDD[Edge[Int]] = sc.parallelize(edgeArray)

    //构造图Graph[VD,ED]
    val graph: Graph[(String, Int), Int] = Graph(vertexRDD, edgeRDD)

    val phoneList = textFile.flatMap(line=>{
      List(line.split("\t")(0).toLong,line.split("\t")(1).toLong)
    }).distinct().collect()

    var counter = 0 
    while (counter < phoneList.length) {
      val sourceId: VertexId = phoneList(counter) // 定义源点
      //初始化一个新的图，该图的节点属性为graph中各节点到原点的距离  
      val initialGraph = graph.mapVertices((id, _) => if (id == sourceId) 0.0 else Double.PositiveInfinity)

      val sssp = initialGraph.pregel(Double.PositiveInfinity)(
        // Vertex Program，节点处理消息的函数，dist为原节点属性（Double），newDist为消息类（Double） 
        (id, dist, newDist) => math.min(dist, newDist),

        // Send Message，发送消息函数，返回结果为（目标节点id，消息（即最短距离））  
        triplet => { // 计算权重
          if (triplet.srcAttr + triplet.attr < triplet.dstAttr) {
            Iterator((triplet.dstId, triplet.srcAttr + triplet.attr))
          } else {
            Iterator.empty
          }
        },

        //Merge Message，对消息进行合并的操作，类似于Hadoop中的combiner 
        (a, b) => math.min(a, b) // 最短距离
        )
        
        sssp.vertices.coalesce(1)
        
        var random = (new util.Random).nextInt(10000)
        sssp.vertices.saveAsTextFile(args(1)+random)
//      println(sssp.vertices.collect.mkString("\n"))
      counter += 1
    }
    sc.stop()
  }
}
