package com.cgtz.sparkml.classify

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.mllib.classification.NaiveBayes
import org.apache.spark.ml.feature.IDF
import org.apache.spark.ml.feature.HashingTF



/**
 * 文本情感分析是指对具有人为主观情感色彩文本材料进行处理、分析和推理的过程。
 * 文本情感分析主要的应用场景是对用户关于某个主题的评论文本进行处理和分析。
 * 比如，人们在打算去看一部电影之前，通常会去看豆瓣电影板块上的用户评论，
 * 再决定是否去看这部电影。另外一方面，电影制片人会通过对专业论坛上的用户评论进行分析，
 * 了解市场对于电影的总体反馈。本文中文本分析的对象为网络短评，为非正式场合的短文本语料，
 * 在只考虑正面倾向和负面倾向的情况下，实现文本倾向性的分类。
 * 
 * 文本情感分析主要涉及如下四个技术环节。

  1. 收集数据集：本文中，以分析电影《疯狂动物城》的用户评论为例子，
  	采集豆瓣上《疯狂动物城》的用户短评和短评评分作为样本数据，通过样本数据训练分类模型来判断微博上的一段话对该电影的情感倾向。
  2. 设计文本的表示模型：让机器“读懂”文字，是文本情感分析的基础，而这首先要解决的问题是文本的表示模型。
  	通常，文本的表示采用向量空间模型，也就是说采用向量表示文本。向量的特征项是模型中最小的单元，可以是一个文档中的字、词或短语，
  	一个文档的内容可以看成是它的特征项组成的集合，而每一个特征项依据一定的原则都被赋予上权重。
  3. 选择文本的特征：当可以把一个文档映射成向量后，那如何选择特征项和特征值呢？通常的做法是先进行中文分词（­­­­本文使用 jieba 分词工具）
  	，把用户评论转化成词语后，可以使用 TF-IDF（Term Frequency–Inverse Document Frequency，词频-逆文档频率）
  	算法来抽取特征，并计算出特征值。
  4. 选择分类模型：常用的分类算法有很多，如：决策树、贝叶斯、人工神经网络、K-近邻、支持向量机等等。
  	在文本分类上使用较多的是贝叶斯和支持向量机。本文中，也以这两种方法来进行模型训练。
 */
object naiveBayesEmotionAnalysis {
  def main(args: Array[String]): Unit = {
    val conf =new SparkConf().setMaster("local").setAppName("naiveBayesFootBall")
    val sc =new SparkContext(conf)

    //读入数据
    val originData = sc.textFile("data/naiveBayesEmotionAnalysis")
    val originDistinctData=originData.distinct()
    val rateDocument=originDistinctData.map(line=> line.split('\t')).filter(line=> line.length==2)
    
    
    //统计数据基本信息
    val fiveRateDocument=rateDocument.filter(line=>line(0).toInt == 5)
    val oneRateDocument=rateDocument.filter(line=>line(0).toInt == 1)
    val twoRateDocument=rateDocument.filter(line=>line(0).toInt == 2)
    val threeRateDocument=rateDocument.filter(line=>line(0).toInt == 3)
    val fourRateDocument=rateDocument.filter(line=>line(0).toInt == 4)
    println(fiveRateDocument.count())
    println(oneRateDocument.count())
    println(twoRateDocument.count())
    println(threeRateDocument.count())
    println(fourRateDocument.count())
    
    //合并负样本数据
    val negRateDocument = oneRateDocument.union(twoRateDocument).union(threeRateDocument).repartition(1)
    
    //成训练数̧据集
    val posRateDocument=sc.parallelize(fiveRateDocument.take(negRateDocument.count().toInt)).repartition(1)
    val allRateDocument = negRateDocument.union(posRateDocument).repartition(1)
    val rate = allRateDocument.map(line=>line(0))
    val document = allRateDocument.map(line => line(1))
    
    val words
    //分词先跳过
    //words=document.map(lambda w:"/".\
    //join(jieba.cut_for_search(w))).\
    //map(lambda line: line.split("/"))
    
    //训练词频矩阵
    val hashingTF = HashingTF()
    val tf = hashingTF.transform(words)
    tf.cache()
    
    //计算 TF-IDF 矩阵
    val idfModel = IDF().fit(tf)
    val tfidf = idfModel.transform(tf)
    
    //生成训练集和测试集
    val zipped=rate.zip(tfidf)
    val data=zipped.map(line=>LabeledPoint(line(0),line(1)))
    val training_test = data.randomSplit(Array(0.6, 0.4), seed = 0)
    
    //训练贝叶斯分类模型
    val NBmodel = NaiveBayes.train(training_test(0), 1.0)
    val predictionAndLabel = training_test.map(training_test(1)).
      map(p=>(NBmodel.predict(p.features), p.label))
    val accuracy = 1.0 * predictionAndLabel.filter(x => x._1 == x._2).count() / test.count()
  
    val yourDocument
    //=input("输入待分类的评论：")
    val yourwords
    //="/".join(jieba.cut_for_search(yourDocument)).split("/")
    val yourtf = hashingTF.transform(yourwords)
    val yourtfidf=idfModel.transform(yourtf)
    print("NaiveBayes Model Predict:"+NBmodel.predict(yourtfidf)
  }
}
