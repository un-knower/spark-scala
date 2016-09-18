package com.cgtz.sparkstreaming.start;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

import kafka.serializer.StringDecoder;
import scala.Tuple2;

/**
 * 
 * 在线处理广告点击流 广告点击的基本数据格式：timestamp、ip、userID、adID、province、city
 *
 *
 */
public class AdClickedStreamingStatsTest {

	@SuppressWarnings("deprecation")
	public static void main(String[] args) {
		/*
		 * 第一步：配置SparkConf： 1，至少2条线程：因为Spark Streaming应用程序在运行的时候，至少有一条
		 * 线程用于不断的循环接收数据，并且至少有一条线程用于处理接受的数据（否则的话无法
		 * 有线程用于处理数据，随着时间的推移，内存和磁盘都会不堪重负）；
		 * 2，对于集群而言，每个Executor一般肯定不止一个Thread，那对于处理Spark Streaming的
		 * 应用程序而言，每个Executor一般分配多少Core比较合适？根据我们过去的经验，5个左右的
		 * Core是最佳的（一个段子分配为奇数个Core表现最佳，例如3个、5个、7个Core等）；
		 */
		SparkConf conf = new SparkConf().setMaster("local[5]").setAppName("AdClickedStreamingStats");

		/*
		 * SparkConf conf = new SparkConf().setMaster("spark://Master:7077").
		 * setAppName("SparkStreamingOnKafkaReceiver");
		 */

		/*
		 * 第二步：创建SparkStreamingContext： 1，这个是SparkStreaming应用程序所有功能的起始点和程序调度的核心
		 * SparkStreamingContext的构建可以基于SparkConf参数，
		 * 也可基于持久化的SparkStreamingContext的内容 来恢复过来（典型的场景是Driver崩溃后重新启动，由于Spark
		 * Streaming具有连续7*24小时不间断运行的特征，
		 * 所有需要在Driver重新启动后继续上衣系的状态，此时的状态恢复需要基于曾经的Checkpoint）； 2，在一个Spark
		 * Streaming应用程序中可以创建若干个SparkStreamingContext对象，
		 * 使用下一个SparkStreamingContext 之前需要把前面正在运行的SparkStreamingContext对象关闭掉，由此，
		 * 我们获得一个重大的启发SparkStreaming框架也只是 Spark Core上的一个应用程序而已，只不过Spark
		 * Streaming框架箱运行的话需要Spark工程师写业务逻辑处理代码；
		 */
		JavaStreamingContext jsc = new JavaStreamingContext(conf, Durations.seconds(10));
	
		/*
	       * 第三步：创建Spark Streaming输入数据来源input Stream：
	       * 1，数据输入来源可以基于File、HDFS、Flume、Kafka、Socket等
	       * 2, 在这里我们指定数据来源于网络Socket端口，Spark Streaming连接上该端口并在运行的时候一直监听该端口
	       *        的数据（当然该端口服务首先必须存在）,并且在后续会根据业务需要不断的有数据产生(当然对于Spark Streaming
	       *        应用程序的运行而言，有无数据其处理流程都是一样的)； 
	       * 3,如果经常在每间隔5秒钟没有数据的话不断的启动空的Job其实是会造成调度资源的浪费，因为并没有数据需要发生计算，所以
	       *        实例的企业级生成环境的代码在具体提交Job前会判断是否有数据，如果没有的话就不再提交Job；
	       * 4,在本案例中具体参数含义:
	       *        第一个参数是StreamingContext实例;
	       *        第二个参数是ZooKeeper集群信息(接受Kafka数据的时候会从ZooKeeper中获得Offset等元数据信息)
	       *        第三个参数是Consumer Group
	       *        第四个参数是消费的Topic以及并发读取Topic中Partition的线程数
	       */
		
		/**
	       * 创建Kafka元数据,来让Spark Streaming这个Kafka Consumer利用
	       */
	      Map<String, String> kafkaParameters = new HashMap<String, String>();
	      kafkaParameters.put("metadata.broker.list", 
	            "Master:9092,Worker1:9092,Worker2:9092");
	      
	      Set<String> topics =  new HashSet<String>();
	      topics.add("AdClicked");
	      
	      JavaPairInputDStream<String, String> adClickedStreaming = KafkaUtils.createDirectStream(jsc, 
	            String.class, String.class, 
	            StringDecoder.class, StringDecoder.class,
	            kafkaParameters, 
	            topics);
	      
	      /*
	       * 第四步：接下来就像对于RDD编程一样基于DStream进行编程！！！原因是DStream是RDD产生的模板（或者说类），在Spark Streaming具体
	       * 发生计算前，其实质是把每个Batch的DStream的操作翻译成为对RDD的操作！！！
	       *对初始的DStream进行Transformation级别的处理，例如map、filter等高阶函数等的编程，来进行具体的数据计算
	        *     广告点击的基本数据格式：timestamp、ip、userID、adID、province、city
	        */
	   
	      //下面是把广告数据切分出来，重新组拼成后面我们需要的格式
	      JavaPairDStream<String, Long> pairs = adClickedStreaming.mapToPair(new PairFunction<Tuple2<String,String>, String, Long>() {
			private static final long serialVersionUID = 1L;

			@Override
	         public Tuple2<String, Long> call(Tuple2<String, String> t) throws Exception {
	            String[] splited = t._2.split("\t");
	            
	            String timestamp = splited[0];
	            String ip = splited[1];
	            String userID = splited[2];
	            String adID = splited[3];
	            String province = splited[4];
	            String city = splited[5];
	            
	            String clickedRecord = timestamp + "_" + ip + "_" + userID + "_" + adID + "_" 
	                  + province + "_" + city;
	            
	            return new Tuple2<String, Long>(clickedRecord, 1L);
	         }
	      });
	      
	       /*
	          *
	          *   计算每个Batch Duration中每个User的广告点击量，Function2里面的3个long，第一个value，第2个value，第3个是结果值
	          */
	      JavaPairDStream<String, Long> adClickedUsers = pairs.reduceByKey(new Function2<Long, Long, Long>(){
			private static final long serialVersionUID = 1L;

			@Override
	         public Long call(Long v1, Long v2) throws Exception {
	            // TODO Auto-generated method stub
	            return v1 + v2;
	         }
	            
	      });
	      
	      
	      /**
	       * 
	       * 计算出什么叫有效的点击？
	       * 1，复杂化的一般都是采用机器学习训练好模型直接在线进行过滤；
	       * 2，简单的？可以通过一个Batch Duration中的点击次数来判断是不是非法广告点击，但是实际上讲非法广告
	       * 点击程序，会尽可能模拟真实的广告点击行为，所以通过一个Batch来判断是 不完整的，我们需要对例如一天（也可以是每一个小时）
	       * 的数据进行判断
	       * 
	       */
	       
	      JavaPairDStream<String, Long>  filteredClickInBatch = adClickedUsers.filter(new Function<Tuple2<String,Long>, Boolean>() {
			private static final long serialVersionUID = 1L;

			@Override
	         public Boolean call(Tuple2<String, Long> v1) throws Exception {
	            if ( 1 < v1._2){
	               return false;//一个batch里面如果点击次数大于一次，视为黑名单，过滤掉
	            } else {
	               return true;
	            }
	            
	         }
	      });
	      
	      // Todo。。。。
	      
	      /*
	       * 此处的print并不会直接出发Job的执行，因为现在的一切都是在Spark Streaming框架的控制之下的，对于Spark Streaming
	       * 而言具体是否触发真正的Job运行是基于设置的Duration时间间隔的
	       * 
	       * 诸位一定要注意的是Spark Streaming应用程序要想执行具体的Job，对Dtream就必须有output Stream操作，
	       * output Stream有很多类型的函数触发，类print、saveAsTextFile、saveAsHadoopFiles等，最为重要的一个
	       * 方法是foraeachRDD,因为Spark Streaming处理的结果一般都会放在Redis、DB、DashBoard等上面，foreachRDD
	       * 主要就是用用来完成这些功能的，而且可以随意的自定义具体数据到底放在哪里！！！
	       *
	       */
	      filteredClickInBatch.foreachRDD(new Function<JavaPairRDD<String,Long>, Void>() {
			private static final long serialVersionUID = 1L;

			@Override
	          public Void call(JavaPairRDD<String, Long> rdd) throws Exception {
	             rdd.foreachPartition(new VoidFunction<Iterator<Tuple2<String,Long>>>() {
					private static final long serialVersionUID = 1L;

					@Override
	                public void call(Iterator<Tuple2<String, Long>> partition) throws Exception {
	                   /**
	                    * 在这里我们使用数据库连接池的高效读写数据库的方式把数据写入数据库MySQL;
	                    * 由于传入的参数是一个Iterator类型的集合，所以为了更加高效的操作我们需要批量处理
	                    * 例如说一次性插入1000条Record，使用insertBatch或者updateBatch类型的操作；
	                    * 插入的用户信息可以只包含：useID、adID、clickedCount、time
	                    * 这里面有一个问题：可能出现两条记录的Key是一样的，此时就需要更新累加操作
	                    * Batch里面这个放进去的就是这个用户对这个广告点击的次数，如果发现数据库中有这个数据，，就累加，每10秒更新一次
	                    */
	                }
	             });
	             return null;
	          }
	       });
	      //再次过滤，从数据库中读取数据过滤黑名单
	      
	      
	      
	      JavaPairDStream<String, Long>  blackListBasedInBatch = filteredClickInBatch.filter(new Function<Tuple2<String,Long>, Boolean>() {
			private static final long serialVersionUID = 1L;

			@Override
	  		public Boolean call(Tuple2<String, Long> v1) throws Exception {
	  			//广告点击的基本数据格式：timestamp,ip,userID,adID,province,city
	  			String[] splited = v1._1.split("\t"); //提取key值
	  			String date =splited[0];
	  			String userID =splited[2];
	  			String adID =splited[3];
	  			// 查询一下数据库同一个用户同一个广告id点击量超过50次列入黑名单
	  			//接下来 根据date、userID、adID条件去查询用户点击广告的数据表，获得总的点击次数
	  			//这个时候基于点击次数判断是否属于黑名单点击
	  			int clickedCountTotalToday = 81 ;
	  			if (clickedCountTotalToday > 50) {
	  				  return true;
	  			}else {
	  				return false ;
	  			}
	  		}
	  	 });
	      
	    //map操作，找出用户的id
	  	JavaDStream<String>   blackListuserIDBasedInBatchOnhistroy =blackListBasedInBatch.map(new Function<Tuple2<String,Long>, String>() {
			private static final long serialVersionUID = 1L;
			@Override
	  		public String call(Tuple2<String, Long> v1) throws Exception {
	  			// TODO Auto-generated method stub
	  			return v1._1.split("\t")[2];
	  		}
	  	});
	  	
	    //有一个问题，数据可能重复，在一个partition里面重复，这个好办；
		//但多个partition不能保证一个用户重复，需要对黑名单的整个rdd进行去重操作。
		//rdd去重了，partition也就去重了，一石二鸟，一箭双雕
		// 找出了黑名单，下一步就写入黑名单数据库表中
	  	JavaDStream<String>  blackListUniqueuserBasedInBatchOnhistroy = 
	  			blackListuserIDBasedInBatchOnhistroy.transform(new Function<JavaRDD<String>, JavaRDD<String>>() {
			private static final long serialVersionUID = 1L;

			@Override
			public JavaRDD<String> call(JavaRDD<String> arg0) throws Exception {
				return arg0.distinct();
			}
		});
	  	
	 // 下一步写入到数据表中
		
		blackListUniqueuserBasedInBatchOnhistroy.foreachRDD(new Function<JavaRDD<String>, Void>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Void call(JavaRDD<String> rdd) throws Exception {
				 rdd.foreachPartition(new VoidFunction<Iterator<String>>() {
					private static final long serialVersionUID = 1L;

					@Override
					public void call(Iterator<String> t) throws Exception {
						// TODO Auto-generated method stub
						//插入的用户信息可以只包含：useID
						//此时直接插入黑名单数据表即可。
						//写入数据库
						
					}
				});
				return null;
			}
		});
	      /*
	       * Spark Streaming执行引擎也就是Driver开始运行，Driver启动的时候是位于一条新的线程中的，当然其内部有消息循环体，用于
	       * 接受应用程序本身或者Executor中的消息；
	       */
	      jsc.start();
	      
	      jsc.awaitTermination();
	      jsc.close();
	}

}
