package com.cgtz.sparkml.filter
import org.apache.spark.mllib.recommendation.{ ALS, Rating }
import org.apache.log4j.{ Logger, Level }
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel
import org.apache.spark.rdd.RDD
import java.text.SimpleDateFormat
import java.util.Date
import org.jblas.DoubleMatrix

/**
 * 居于ALS算法的产品推荐
 */
object CollaborativeFilterAli {
  def main(args: Array[String]): Unit = {
    //环境变量
    val conf = new SparkConf().setMaster("local").setAppName("CollaborativeFilteringSpark")
    //实例化环境
    val sc = new SparkContext(conf)

    //Spark的日志级别默认为INFO，你可以手动设置为WARN级别，同样先引入log4j依赖：
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

    val data = sc.textFile("data/ratings.dat")
    val ratings = data.map(_.split("::") match {
      case Array(user, item, rate, ts) =>
        Rating(user.toInt, item.toInt, rate.toDouble)
    }).cache()
    
    println("全量数据：")
    println("第一条数据：" + ratings.first())
    val users = ratings.map(_.user).distinct()
    val products = ratings.map(_.product).distinct()
    println("用户数量：" + users.count())
    println("产品数量：" + products.count())

    //你可以对评分数据生成训练集和测试集，例如：训练集和测试集比例为8比2：
    val splits = ratings.randomSplit(Array(0.8, 0.2), seed = 111l)
    val training = splits(0)
    val test = splits(1)

    //这里，我们是将评分数据全部当做训练集，并且也为测试集
    //接下来调用ALS.train()方法，进行模型训练：
    val rank = 12
    val lambda = 0.01
    val numIterations = 10
    val model = ALS.train(ratings, rank, numIterations, lambda)

    //训练完后，我们看看model中的用户和商品特征向量：
    println
    println("模型数据:")
    println(model.userFeatures.count)
    println(model.productFeatures.count)
    
    //其实就是对测试集进行评分预测并计算相似度
    computeRmse(model, ratings)
    
    //给一个用户推荐商品
    //这个例子主要是记录如何给一个或大量用户进行推荐商品，例如，对用户编号为384的用户进行推荐，查出该用户在测试集中评分过的商品。
    //查看用户编号为384的用户的预测结果中预测评分排前10的商品：
    //val userId = users.take(1)(0) //384
    val K = 10
    val topKRecs = model.recommendProducts(384, K)
    println
    println("预测结果中预测评分排前10的商品：")
    println(topKRecs.mkString("\n"))
    
    //查看该用户的评分记录：
    val goodsForUser=ratings.keyBy(_.user).lookup(384)
    println
    println("该用户对22个商品评过分以及浏览的商品是哪些:")
    println(goodsForUser.size)
    goodsForUser.sortBy(-_.rating).take(10).map(rating => 
      (rating.product, rating.rating)).foreach(println)

    //我们可以该用户对某一个商品的实际评分和预测评分方差为多少：
    val actualRating = goodsForUser.take(1)(0)
    val predictedRating = model.predict(384, actualRating.product)
    val squaredError = math.pow(predictedRating - actualRating.rating, 2.0)
    println
    println("实际评分和预测评分方差为多少：")
    println(squaredError)
    
    //如何找出和一个已知商品最相似的商品呢？这里，我们可以使用余弦相似度来计算：
    //以2055商品为例，计算实际评分和预测评分相似度
    val itemId = 2055
    val itemFactor = model.productFeatures.lookup(itemId).head
    val itemVector = new DoubleMatrix(itemFactor)
    println
    println("2055商品为例，计算实际评分和预测评分相似度:")
    println(cosineSimilarity(itemVector, itemVector))

    //找到和该商品最相似的10个商品：
    val sims = model.productFeatures.map {
      case (id, factor) =>
        val factorVector = new DoubleMatrix(factor)
        val sim = cosineSimilarity(factorVector, itemVector)
        (id, sim)
    }
    val sortedSims = sims.top(K)(Ordering.by[(Int, Double), Double] { case (id, similarity) => similarity })
    println
    println("找到和该商品最相似的10个商品：")
    println(sortedSims.mkString("\n"))
    
    //显然第一个最相似的商品即为该商品本身，即2055，我们可以修改下代码，取前k+1个商品，然后排除第一个：
    val sortedSims2 = sims.top(K + 1)(Ordering.by[(Int, Double), Double] { 
      case (id, similarity) => similarity 
    })
    println
    println("找到和该商品最相似的10个商品(不包括自己)：")
    println(sortedSims2.slice(1, 11).map{ case (id, sim) => (id, sim) }.mkString("\n"))
    
    //给该用户推荐的商品为：
    val actualProducts = goodsForUser.map(_.product)
    println
    println("给该用户推荐的商品为：")
    actualProducts.foreach(println)
    //给该用户预测的商品为：
    val predictedProducts = topKRecs.map(_.product)
    println
    println("给该用户预测的商品为：")
    predictedProducts.foreach(println)
    //最后的准确度为：
    val apk10 = avgPrecisionK(actualProducts, predictedProducts, 10)
    println
    println("精准度：")
    println(apk10)
    
    //批量推荐
    //直接操作model中的userFeatures和productFeatures，代码如下：
    val itemFactors = model.productFeatures.map{case(id,factor)=>factor}.collect()
    val itemMatrix = new DoubleMatrix(itemFactors)
    println(itemMatrix.rows, itemMatrix.columns)

    val imBroadcast = sc.broadcast(itemMatrix)
    //获取商品和索引的映射
    var idxProducts = model.productFeatures.map { case (prodcut, factor) => prodcut }.zipWithIndex().map { case (prodcut, idx) => (idx, prodcut) }.collectAsMap()
    val idxProductsBroadcast = sc.broadcast(idxProducts)

    val allRecs = model.userFeatures.map {
      case (user, array) =>
        val userVector = new DoubleMatrix(array)
        val scores = imBroadcast.value.mmul(userVector)
        val sortedWithId = scores.data.zipWithIndex.sortBy(-_._1)
        //根据索引取对应的商品id
        val recommendedProducts = sortedWithId.map(_._2).map { idx => idxProductsBroadcast.value.get(idx).get }
        (user, recommendedProducts)
    }
    
    //如果上面结果跑出来了，就可以验证推荐结果是否正确。还是以384用户为例：
    allRecs.lookup(384).head.take(10).foreach { println }
    topKRecs.map(_.product).foreach { println }
    
    //接下来，我们可以计算所有推荐结果的准确度了，首先，得到每个用户评分过的所有商品：
    val userProducts = ratings.map{
      case Rating(user,product,rating) => (user,product)
    }.groupBy(_._1)

    //然后，预测的商品和实际商品关联求准确度：
    val MAPK = allRecs.join(userProducts).map {
      case (userId, (predicted, actualWithIds)) =>
        val actual = actualWithIds.map(_._2).toSeq
        avgPrecisionK(actual, predicted, K)
    }.reduce(_ + _) / allRecs.count
    println("Mean Average Precision at K = " + MAPK)

    //计算推荐2000个商品时的准确度为：
    val MAPK2000 = allRecs.join(userProducts).map {
      case (userId, (predicted, actualWithIds)) =>
        val actual = actualWithIds.map(_._2).toSeq
        avgPrecisionK(actual, predicted, 2000)
    }.reduce(_ + _) / allRecs.count
    println("Mean Average Precision = " + MAPK2000)
    
    //保存和加载推荐模型
    //对与实时推荐，我们需要启动一个web server，在启动的时候生成或加载训练模型，然后提供API接口返回推荐接口，需要调用的相关方法为：
    val outputDir="hdfs://172.16.33.66:8020/model/CollaborativeFilterAli"
    model.userFeatures.map{ case (id, vec) => id + "\t" + vec.mkString(",") }.saveAsTextFile(outputDir + "/userFeatures")
    model.productFeatures.map{ case (id, vec) => id + "\t" + vec.mkString(",") }.saveAsTextFile(outputDir + "/productFeatures")
  }
  /**
   * 接下来，我们可以计算给该用户推荐的前K个商品的平均准确度MAPK，该算法定义如下（该算法是否正确还有待考证）：
   */
  def avgPrecisionK(actual: Seq[Int], predicted: Seq[Int], k: Int): Double = {
    val predK = predicted.take(k)
    var score = 0.0
    var numHits = 0.0
    for ((p, i) <- predK.zipWithIndex) {
      if (actual.contains(p)) {
        numHits += 1.0
        score += numHits / (i.toDouble + 1.0)
      }
    }
    if (actual.isEmpty) {
      1.0
    } else {
      score / scala.math.min(actual.size, k).toDouble
    }
  }
  /** 
   *  其实就是对测试集进行评分预测并计算相似度 
   *  评测
   * */
  def computeRmse(model: MatrixFactorizationModel, data: RDD[Rating]) = {
    //我们要对比一下预测的结果，注意：我们将训练集当作测试集来进行对比测试。从训练集中获取用户和商品的映射：
    val usersProducts = data.map {
      case Rating(user, product, rate) =>
        (user, product)
    }
    //显然，测试集的记录数等于评分总记录数，验证一下：
    println
    println(usersProducts.count)

    //使用推荐模型对用户商品进行预测评分，得到预测评分的数据集：
    val predictions = model.predict(usersProducts).map {
      case Rating(user, product, rate) =>
        ((user, product), rate)
    }
    println
    println(predictions.count)
    
    //将真实评分数据集与预测评分数据集进行合并，这样得到用户对每一个商品的实际评分和预测评分：
    val ratesAndPreds = data.map {
      case Rating(user, product, rate) =>
        ((user, product), rate)
    }.join(predictions)
    println
    println(ratesAndPreds.count)

    //然后计算根均方差：
    val rmse = math.sqrt(ratesAndPreds.map {
      case ((user, product), (r1, r2)) =>
        val err = (r1 - r2)
        err * err
    }.mean())
    println
    println("方差:")
    println(s"RMSE = $rmse")
    
    //保存真实评分和预测评分
    //我们还可以保存用户对商品的真实评分和预测评分记录到本地文件：
    //ratesAndPreds.sortByKey().repartition(1).sortBy(_._1).map({
    //  case ((user, product), (rate, pred)) => (user + "," + product + "," + rate + "," + pred)
    //}).saveAsTextFile("hdfs://172.16.33.66:8020/model/CollaborativeFilterAli")
  }
  /**
   * 如何找出和一个已知商品最相似的商品呢？这里，我们可以使用余弦相似度来计算：
   */
  def cosineSimilarity(vec1: DoubleMatrix, vec2: DoubleMatrix): Double = {
    vec1.dot(vec2) / (vec1.norm2() * vec2.norm2())
  }
}