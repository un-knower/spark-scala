package com.cgtz.sparkstreaming.start;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

import kafka.serializer.StringDecoder;
import scala.Tuple2;

public class OnlineBBSUserLogsTest {
	public static void main(String[] args) {
		/*
		 * 消费者消费SparkStreamingDataManuallyProducerForKafka 类中逻辑级别产生的数据，这里pv的方式
		 */
		/*
		 * SparkConf conf = new SparkConf().setMaster("local[2]").
		 * setAppName("WordCountOnline");
		 */
		SparkConf conf = new SparkConf().setMaster("spark://Master:7077").setAppName("OnlineBBSUserLogs");
		JavaStreamingContext jsc = new JavaStreamingContext(conf, Durations.seconds(5));
		/*
		 * 链接kafka
		 */
		Map<String, String> kafkaParameters = new HashMap<String, String>();
		kafkaParameters.put("metadata.broker.list", "Master:9092,Worker1:9092,Worker2:9092");

		Set<String> topics = new HashSet<String>();
		topics.add("UserLogs");

		JavaPairInputDStream<String, String> lines = KafkaUtils.createDirectStream(jsc, String.class, String.class,
				StringDecoder.class, StringDecoder.class, kafkaParameters, topics);

		// 找pv，view的方式。所以过滤数据
		JavaPairDStream<String, String> logsDStream = 
				lines.filter(new Function<Tuple2<String, String>, Boolean>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Boolean call(Tuple2<String, String> v1) throws Exception {
				// key是kafka给的，不需要key，拿第二个元素即可
				String[] logs = v1._2.split("\t");
				// action是第6位appand进来的数据，这里的action要View不是register
				String action = logs[5];
				if ("View".equals(action)) {
					return true;
				} else {
					return false;
				}
			}
		});
		/*
		 * 第四步：对初始的DStream进行Transformation级别的处理，
		 * 例如map、filter等高阶函数等的编程，来进行具体的数据计算
		 * 第4.2步：在单词拆分的基础上对每个单词实例计数为1，也就是word => (word, 1)
		 */
		JavaPairDStream<Long, Long> pairs = 
				logsDStream.mapToPair(new PairFunction<Tuple2<String,String>, Long, Long>() {
			private static final long serialVersionUID = 1L;
			@Override
			public Tuple2<Long, Long> call(Tuple2<String, String> t) 
					throws Exception {
				// key是kafka给的，不需要key，拿第二个元素即可
				String[] logs = t._2.split("\t");
				// 第3个，生成整数
				Long pageId = Long.valueOf(logs[3]);
				return new Tuple2<Long, Long>(pageId, 1L);
			}
		});
		/*
         * 第四步：对初始的DStream进行Transformation级别的处理，
         * 例如map、filter等高阶函数等的编程，来进行具体的数据计算
         * 第4.3步：在每个单词实例计数为1基础之上统计每个单词在文件中出现的总次数
         */
		JavaPairDStream<Long, Long> wordsCount =
				pairs.reduceByKey(new Function2<Long, Long, Long>() {
			//对相同的Key，进行Value的累计（包括Local和Reducer级别同时Reduce）
			private static final long serialVersionUID = 1L;
			@Override
			public Long call(Long arg0, Long arg1) throws Exception {
				return arg0+arg1;
			}
		});
		
		// 上面操作结果就得到了页面id和点击次数
		/*
		 * 在企业生产环境下，一般会把计算的数据放入Redius或者DB中，采用J2EE等技术进行趋势的绘制
		 */

		wordsCount.print();

		/*
		 * Spark
		 * Streaming执行引擎也就是Driver开始运行，Driver启动的时候是位于一条新的线程中的，当然其内部有消息循环体，用于
		 * 接受应用程序本身或者Executor中的消息；
		 */
		jsc.start();

		jsc.awaitTermination();
		jsc.close();
	}
}
