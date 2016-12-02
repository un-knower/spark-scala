package com.cgtz.sparkml.pipeline.classify
/**
 * 原文章链接:http://geek.csdn.net/news/detail/89913
 * 
 * 使用基于Apache Spark的随机森林方法预测贷款风险
 * 我们来看一个银行信贷的信用风险例子：
 * 我们需要预测什么？ 
 * 		某个人是否会按时还款
 * 		这就是标签：此人的信用度
 * 你用来预测的“是与否”问题或者属性是什么？ 
 * 		申请人的基本信息和社会身份信息：职业，年龄，存款储蓄，婚姻状态等等……
 * 		这些就是特征，用来构建一个分类模型，你从中提取出对分类有帮助的特征信息。
 * 
 * 我们使用德国人信用度数据集，它按照一系列特征属性将人分为信用风险好和坏两类。我们可以获得每个银行贷款申请者的以下信息：
 * 
 * 存放德国人信用数据的csv文件格式如下：
    1,1,18,4,2,1049,1,2,4,2,1,4,2,21,3,1,1,3,1,1,1
    1,1,9,4,0,2799,1,3,2,3,1,2,1,36,3,1,2,3,2,1,1
    1,2,12,2,9,841,2,4,2,2,1,4,1,23,3,1,1,2,1,1,1
    
      在这个背景下，我们会构建一个由决策树组成的随机森林模型来预测是否守信用的标签/类别，基于以下特征：
            标签 -> 守信用或者不守信用（1或者0）
            特征 -> {存款余额，信用历史，贷款目的等等}
 */
import org.apache.spark.ml.classification.RandomForestClassifier
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.tuning.{ ParamGridBuilder, CrossValidator }
import org.apache.spark.ml.{ Pipeline, PipelineStage }
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.mllib.evaluation.RegressionMetrics


object RandomForestCreditEvaluation {
   case class Credit(
    creditability: Double,
    balance: Double, duration: Double, history: Double, purpose: Double, amount: Double,
    savings: Double, employment: Double, instPercent: Double, sexMarried: Double, guarantors: Double,
    residenceDuration: Double, assets: Double, age: Double, concCredit: Double, apartment: Double,
    credits: Double, occupation: Double, dependents: Double, hasPhone: Double, foreign: Double
  )
  /**
   * 下面的函数解析一行数据文件，将值存入Credit类中。
   * 类别的索引值减去了1，因此起始索引值为0.
   */
   def parseCredit(line: Array[Double]): Credit = {
    Credit(
      line(0),
      line(1) - 1, line(2), line(3), line(4), line(5),
      line(6) - 1, line(7) - 1, line(8), line(9) - 1, line(10) - 1,
      line(11) - 1, line(12) - 1, line(13), line(14) - 1, line(15) - 1,
      line(16) - 1, line(17) - 1, line(18) - 1, line(19) - 1, line(20) - 1
    )
  }
  
  // function to transform an RDD of Strings into an RDD of Double
  def parseRDD(rdd: RDD[String]): RDD[Array[Double]] = {
    rdd.map(_.split(",")).map(_.map(_.toDouble))
  }
  
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("SparkDFebay").setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    import sqlContext._
    import sqlContext.implicits._
    
    val creditDF = parseRDD(sc.textFile("data/german/germancredit.csv"))
      .map(parseCredit).toDF().cache()
    creditDF.registerTempTable("credit")
    creditDF.printSchema()
    
    creditDF.show()
    
    sqlContext.sql("SELECT creditability, avg(balance) as avgbalance, "+
                   "avg(amount) as avgamt, avg(duration) as avgdur  FROM credit GROUP BY creditability").show()
  
    
    //dataframe初始化之后，你可以用SQL命令查询数据了。下面是一些使用Scala DataFrame接口查询数据的例子：
    //计算数值型数据的统计信息，包括计数、均值、标准差、最小值和最大值。
    creditDF.describe("balance").show
    creditDF.groupBy("creditability").avg("balance").show
    
    //提取特征
    //为了构建一个分类模型，你首先需要提取对分类最有帮助的特征。
    //在德国人信用度的数据集里，每条样本用两个类别来标记——1（可信）和0（不可信）
    //每个样本的特征包括以下的字段：

    //标签 -> 是否可信：0或者1
    //特征 -> {“存款”，“期限”，“历史记录”，“目的”，
    //       “数额”，“储蓄”，“是否在职”，“婚姻”，
    //       “担保人”，“居住时间”，“资产”，“年龄”，
    //       “历史信用”，“居住公寓”，“贷款”，“职业”，
    //       “监护人”，“是否有电话”，“外籍”}

    //为了在机器学习算法中使用这些特征，这些特征经过了变换，存入特征向量中，即一组表示各个维度特征值的数值向量。
    //下图中，用VectorAssembler方法将每个维度的特征都做变换，返回一个新的dataframe。
    //define the feature columns to put in the feature vector
    val featureCols = Array(
        "balance", "duration", "history", "purpose", "amount",
        "savings", "employment", "instPercent", "sexMarried",  "guarantors",
        "residenceDuration", "assets",  "age", "concCredit", "apartment",
        "credits",  "occupation", "dependents",  "hasPhone", "foreign"
        )
    //set the input and output column names
    val assembler = new VectorAssembler()
      .setInputCols(featureCols)
      .setOutputCol("features")
    //return a dataframe with all of the  feature columns in  a vector column
    val df2 = assembler.transform(creditDF)
    // the transform method produced a new column: features.
    df2.show()
    
    
    //接着，我们使用StringIndexer方法返回一个Dataframe，增加了信用度这一列作为标签。
    //  Create a label column with the StringIndexer  
    val labelIndexer = new StringIndexer()
      .setInputCol("creditability")
      .setOutputCol("label")
    val df3 = labelIndexer.fit(df2).transform(df2)
    // the  transform method produced a new column: label.
    df3.show()
    
    //下图中，数据集被分为训练数据和测试数据两个部分，70%的数据用来训练模型，30%的数据用来测试模型。
    //  split the dataframe into training and test data
    val splitSeed = 5043
    val Array(trainingData,testData) = df3.randomSplit(Array(0.7, 0.3), splitSeed)
    
    /**
     * 接着，我们按照下列参数训练一个随机森林分类器：

        maxDepth：每棵树的最大深度。增加树的深度可以提高模型的效果，但是会延长训练时间。
        maxBins：连续特征离散化时选用的最大分桶个数，并且决定每个节点如何分裂。
        impurity：计算信息增益的指标
        auto：在每个节点分裂时是否自动选择参与的特征个数
        seed：随机数生成种子
        
       	 模型的训练过程就是将输入特征和这些特征对应的样本标签相关联的过程。
     */
    //// create the classifier,  set parameters for training
    val classifier = new RandomForestClassifier()
      .setImpurity("gini")
      .setMaxDepth(3)
      .setNumTrees(20)
      .setFeatureSubsetStrategy("auto")
      .setSeed(5043)
    //  use the random forest classifier  to train (fit) the model
    val model = classifier.fit(trainingData)
    
    // print out the random forest trees
    model.toDebugString
    
    /**
     * 测试模型
		 * 接下来，我们对测试数据进行预测。    
     */
    // run the  model on test features to get predictions
    val predictions = model.transform(testData)
    //As you can see, the previous model transform produced a new columns: rawPrediction, probablity and prediction.
    predictions.show()
    
    
    /**
     * 然后，我们用BinaryClassificationEvaluator评估预测的效果，
     * 它将预测结果与样本的实际标签相比较，返回一个准确度指标（ROC曲线所覆盖的面积）。本例子中，AUC达到78%。
     */
     // create an Evaluator for binary classification, which expects two input columns: rawPrediction and label.
    val evalutor = new BinaryClassificationEvaluator().setLabelCol("label")
    //Evaluates predictions and returns a scalar metric areaUnderROC(larger is better). 
    val accuray = evalutor.evaluate(predictions)
    println("accuracy before pipeline fitting" + accuray)

    val rm = new RegressionMetrics(
    		predictions.select("prediction", "label").rdd.map(x =>
    		(x(0).asInstanceOf[Double], x(1).asInstanceOf[Double]))
    )
    
    println("MSE: " + rm.meanSquaredError)
    println("MAE: " + rm.meanAbsoluteError)
    println("RMSE Squared: " + rm.rootMeanSquaredError)
    println("R Squared: " + rm.r2)
    println("Explained Variance: " + rm.explainedVariance + "\n")
    
    
    /**
     * 使用机器学习管道
                    我们接着用管道来训练模型，可能会取得更好的效果。管道采取了一种简单的方式来比较各种不同组合的参数的效果，
                    这个方法称为网格搜索法（grid search），你先设置好待测试的参数，
       MLLib就会自动完成这些参数的不同组合。管道搭建了一条工作流，
                    一次性完成了整个模型的调优，而不是独立对每个参数进行调优。
                    下面我们就用ParamGridBuilder工具来构建参数网格。
     */
    // We use a ParamGridBuilder to construct a grid of parameters to search over
    val paramGrid = new ParamGridBuilder()
      .addGrid(classifier.maxBins, Array(25, 31))
      .addGrid(classifier.maxDepth, Array(5, 10))
      .addGrid(classifier.numTrees, Array(20, 60))
      .addGrid(classifier.impurity, Array("entropy", "gini"))
      .build()
    //创建并完成一条管道。一条管道由一系列stage组成，每个stage相当于一个Estimator或是Transformer。
    val steps: Array[PipelineStage] = Array(classifier)
    val pipeline = new Pipeline().setStages(steps)
    /**
     * 我们用CrossValidator类来完成模型筛选。
     * CrossValidator类使用一个Estimator类，
     * 一组ParamMaps类和一个Evaluator类。注意，
     * 使用CrossValidator类的开销很大。
     */
    // Evaluate model on test instances and compute test error
    val cv = new CrossValidator()
      .setEstimator(pipeline)
      .setEvaluator(evalutor)
      .setEstimatorParamMaps(paramGrid)
      .setNumFolds(10)
    /**
     * 管道在参数网格上不断地爬行，自动完成了模型优化的过程：对于每个ParamMap类，
     * CrossValidator训练得到一个Estimator，然后用Evaluator来评价结果，
     * 然后用最好的ParamMap和整个数据集来训练最优的Estimator。
     */
    val pipelineFittedModel = cv.fit(trainingData)
    /**
     * 现在，我们可以用管道训练得到的最优模型进行预测，
     * 将预测结果与标签做比较。预测结果取得了82%的准确率，
     * 相比之前78%的准确率有提高
     */
    val predictions2 = pipelineFittedModel.transform(testData)
    val accuracy2 = evalutor.evaluate(predictions2)
    println("accuracy after pipeline fitting" + accuracy2)

    println(pipelineFittedModel.bestModel.asInstanceOf[org.apache.spark.ml.PipelineModel].stages(0))

    pipelineFittedModel
      .bestModel.asInstanceOf[org.apache.spark.ml.PipelineModel]
      .stages(0)
      .extractParamMap

    val rm2 = new RegressionMetrics(
      predictions2.select("prediction", "label").rdd.map(x =>
        (x(0).asInstanceOf[Double], x(1).asInstanceOf[Double]))
    )

    println("MSE: " + rm2.meanSquaredError)
    println("MAE: " + rm2.meanAbsoluteError)
    println("RMSE Squared: " + rm2.rootMeanSquaredError)
    println("R Squared: " + rm2.r2)
    println("Explained Variance: " + rm2.explainedVariance + "\n")
  }
}