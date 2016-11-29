package com.cgtz.sparkml.classify

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.ml.feature.HashingTF
import org.apache.spark.ml.feature.IDF
import org.apache.spark.ml.feature.Tokenizer
import org.apache.spark.sql.SQLContext
import org.apache.spark.mllib.classification.NaiveBayes
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.sql.Row
import scala.reflect.runtime.universe

/**
 * 文本分类是指将一篇文章归到事先定义好的某一类或者某几类，
 * 在数据平台的一个典型的应用场景是，
 * 通过爬取用户浏览过的页面内容，
 * 识别出用户的浏览偏好，从而丰富该用户的画像。
 * 
 * 本文介绍使用Spark MLlib提供的朴素贝叶斯（Native Bayes）算法，
 * 完成对中文文本的分类过程。
 * 主要包括中文分词、文本表示（TF-IDF）、模型训练、分类预测等。
 * 
 * step 1
 * 中文分词:
 * 对于中文文本分类而言，需要先对文章进行分词，
 * 我使用的是IKAnalyzer中文分析工具，之前有篇文章介绍过《中文分词工具-IKAnalyzer下载及使用》，
 * 其中自己可以配置扩展词库来使分词结果更合理，
 * 我从搜狗、百度输入法下载了细胞词库，将其作为扩展词库。这里不再介绍分词。
 * 
 * 
 * step 2
 * 中文词语特征值转换（TF-IDF）:
 * 分好词后，每一个词都作为一个特征，但需要将中文词语转换成Double型来表示，
 * 通常使用该词语的TF-IDF值作为特征值，Spark提供了全面的特征抽取及转换的API，
 * 非常方便，详见http://spark.apache.org/docs/latest/ml-features.html,
 * 这里介绍下TF-IDF的API:
 * 比如，训练语料/tmp/lxw1234/1.txt：
 * 0,苹果 官网 苹果 宣布
 * 1,苹果 梨 香蕉
 * 逗号分隔的第一列为分类编号，0为科技，1为水果。
 * 将原始数据映射到DataFrame中，字段category为分类编号，字段text为分好的词，以空格分隔
  var srcDF = sc.textFile("/tmp/lxw1234/1.txt").map { 
        x => 
          var data = x.split(",")
          RawDataRecord(data(0),data(1))
  }.toDF()
  srcDF.select("category", "text").take(2).foreach(println)
  [0,苹果 官网 苹果 宣布]
  [1,苹果 梨 香蕉]
  
  //将分好的词转换为数组
  var tokenizer = new Tokenizer().setInputCol("text").setOutputCol("words")
  var wordsData = tokenizer.transform(srcDF)
  wordsData.select($"category",$"text",$"words").take(2).foreach(println)
  [0,苹果 官网 苹果 宣布,WrappedArray(苹果, 官网, 苹果, 宣布)]
  [1,苹果 梨 香蕉,WrappedArray(苹果, 梨, 香蕉)]
  
  //将每个词转换成Int型，并计算其在文档中的词频（TF）
  var hashingTF = 
  new HashingTF().setInputCol("words").setOutputCol("rawFeatures").setNumFeatures(100)
  var featurizedData = hashingTF.transform(wordsData)
      这里将中文词语转换成INT型的Hashing算法，类似于Bloomfilter，上面的setNumFeatures(100)表示将Hash分桶的数量设置为100个，
      这个值默认为2的20次方，即1048576，可以根据你的词语数量来调整，一般来说，这个值越大，
      不同的词被计算为一个Hash值的概率就越小，数据也更准确，但需要消耗更大的内存，和Bloomfilter是一个道理。
  
  featurizedData.select($"category", $"words", $"rawFeatures").take(2).foreach(println)
  [0,WrappedArray(苹果, 官网, 苹果, 宣布),(100,[23,81,96],[2.0,1.0,1.0])]
  [1,WrappedArray(苹果, 梨, 香蕉),(100,[23,72,92],[1.0,1.0,1.0])]
 	 结果中，“苹果”用23来表示，第一个文档中，词频为2，第二个文档中词频为1.
  
  //计算TF-IDF值
  var idf = new IDF().setInputCol("rawFeatures").setOutputCol("features")
  var idfModel = idf.fit(featurizedData)
  var rescaledData = idfModel.transform(featurizedData)
  rescaledData.select($"category", $"words", $"features").take(2).foreach(println)
  
  [0,WrappedArray(苹果, 官网, 苹果, 宣布),(100,[23,81,96],[0.0,0.4054651081081644,0.4054651081081644])]
  [1,WrappedArray(苹果, 梨, 香蕉),(100,[23,72,92],[0.0,0.4054651081081644,0.4054651081081644])]
  
  //因为一共只有两个文档，且都出现了“苹果”，因此该词的TF-IDF值为0.
 	 最后一步，将上面的数据转换成Bayes算法需要的格式，如：
  https://github.com/apache/spark/blob/branch-1.5/data/mllib/sample_naive_bayes_data.txt
  var trainDataRdd = rescaledData.select($"category",$"features").map {
      case Row(label: String, features: Vector) =>
      LabeledPoint(label.toDouble, Vectors.dense(features.toArray))
  }
 * 
 */
object ArticNativeBayes {
  case class RowDataRecord(category: String, text: String)
  def main(args: Array[String]): Unit = {
    val conf  = new SparkConf().setMaster("local").setAppName("ArticNativeBayes")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
    
    //C:/Users/Administrator/Downloads/Reduced
    var srcDF = sc.wholeTextFiles("data/Reduced/*/*").map(
        x=>{
          val pathFirst = x._1
          val pathSecond = pathFirst.substring(0, pathFirst.lastIndexOf("/"))
          val pathThird = pathSecond.substring(0, pathSecond.lastIndexOf("/"))
          val pathLast = pathSecond.substring(pathThird.length()+1, pathSecond.length())
          val category = pathLast match {
            case "C000008" => "0"
            case "C000010" => "1"
            case "C000013" => "2"
            case "C000014" => "3"
            case "C000016" => "4"
            case "C000020" => "5"
            case "C000022" => "6"
            case "C000023" => "7"
            case "C000024" => "8"
          }
          RowDataRecord(category,x._2)
        }
    )
//    val test = srcDF.collect();
//    for(t <- test)
//      println(t.category+"::"+t.text)

    //70%作为训练数据，30%作为测试数据
    var splits = srcDF.randomSplit(Array(0.7,0.3))
    var trainingDF = splits(0).toDF()
    var testDF = splits(1).toDF()
    
    //将词语转换成数组
    var tokenizer = new Tokenizer()
    .setInputCol("text")
    .setOutputCol("words")
    var wordsData = tokenizer.transform(trainingDF)
    println("output1:")
    wordsData.select($"category",$"text",$"words").show()
    
    //计算每个词在文档中的词频
    var hashingTF = new HashingTF()
    .setNumFeatures(500000)
    .setInputCol("words")
    .setOutputCol("rawFeatures")
    var featurizedData = hashingTF.transform(wordsData)
    println("ouput2:")
    featurizedData.select($"category", $"words", $"rawFeatures").show()
    
    //计算每个词的TF-IDF
    var idf = new IDF()
    .setInputCol("rawFeatures")
    .setOutputCol("features")
    var idfModel = idf.fit(featurizedData)
    var rescaledData = idfModel.transform(featurizedData)
    println("output3:")
    rescaledData.select($"category", $"features").show()
    
    //转换成Bayes的输入格式
    var trainDataRdd = rescaledData.select($"category",$"features").map{
        case Row(label: String, features: Vector) =>
          LabeledPoint(label.toDouble, Vectors.dense(features.toArray))
    }
    println("output4：")
    trainDataRdd.take(2)
    
    //训练模型
    val model = NaiveBayes.train(trainDataRdd, lambda = 1.0, modelType = "multinomial")
    
    //测试数据集，做同样的特征表示及格式转换
    var testwordsData = tokenizer.transform(testDF)
    var testfeaturizedData = hashingTF.transform(testwordsData)
    var testrescaledData = idfModel.transform(testfeaturizedData)
    var testDataRdd = testrescaledData.select($"category",$"features").map {
      case Row(label: String, features: Vector) =>
        LabeledPoint(label.toDouble, Vectors.dense(features.toArray))
    }
    
    //对测试数据集使用训练模型进行分类预测
    val testpredictionAndLabel = testDataRdd.map(p=>(model.predict(p.features),p.label))
    
    //统计分类准确率
    var testaccuracy = 
      1.0 * testpredictionAndLabel.filter(x=>x._1 == x._2)
      .count()/testDataRdd.count()
    println("output5：")
    println(testaccuracy)
  }
}