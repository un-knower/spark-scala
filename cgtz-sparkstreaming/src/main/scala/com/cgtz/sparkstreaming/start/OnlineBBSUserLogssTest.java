package com.cgtz.sparkstreaming.start;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
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

public class OnlineBBSUserLogssTest {

	public static void main(String[] args) {
		/*
		 * 消费者消费SparkStreamingDataManuallyProducerForKafka类中逻辑级别产生的数据，这里pv，
		 * uv，注册人数，跳出率的方式
		 */
		SparkConf conf = new SparkConf().setMaster("spark://Master:7077").setAppName("OnlineBBSUserLogs");

		JavaStreamingContext jsc = new JavaStreamingContext(conf, Durations.seconds(5));

		Map<String, String> kafkaParameters = new HashMap<String, String>();
		kafkaParameters.put("metadata.broker.list", "Master:9092,Worker1:9092,Worker2:9092");

		HashSet<String> topics = new HashSet<String>();
		topics.add("UserLogs");

		JavaPairInputDStream<String, String> lines = KafkaUtils.createDirectStream(jsc, String.class, String.class,
				StringDecoder.class, StringDecoder.class, kafkaParameters, topics);

		// 在线PV计算
		onlinePagePV(lines);
		// 在线UV计算
		onlineUV(lines);
		// 在线计算注册人数
		onlineRegistered(lines);
		// 在线计算跳出率
		onlineJumped(lines);
		// 在线不同模块的PV
		onlineChannelPV(lines);

		/*
		 * Spark
		 * Streaming执行引擎也就是Driver开始运行，Driver启动的时候是位于一条新的线程中的，当然其内部有消息循环体，用于
		 * 接受应用程序本身或者Executor中的消息；
		 */
		jsc.start();

		jsc.awaitTermination();
		jsc.close();
	}

	private static void onlineChannelPV(JavaPairInputDStream<String, String> lines) {
		lines.mapToPair(new PairFunction<Tuple2<String, String>, String, Long>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<String, Long> call(Tuple2<String, String> t) throws Exception {
				String[] logs = t._2.split("\t");
				String channelID = logs[4];
				return new Tuple2<String, Long>(channelID, 1L);
			}
		}).reduceByKey(new Function2<Long, Long, Long>() {
			// 对相同的Key，进行Value的累计（包括Local和Reducer级别同时Reduce）
			private static final long serialVersionUID = 1L;

			@Override
			public Long call(Long arg0, Long arg1) throws Exception {
				return arg0 + arg1;
			}
		}).print();
	}

	/**
	 * 因为要计算UV，所以需要获得同样的Page的不同的User，这个时候就需要去重操作，DStreamzhong有distinct吗？当然没有（
	 * 截止到Spark 1.6.1的时候还没有该Api）
	 * 此时我们就需要求助于DStream魔术般的方法tranform,在该方法内部直接对RDD进行distinct操作，
	 * 这样就是实现了用户UserID的去重，进而就可以计算出UV了。
	 * 
	 * @param lines
	 */
	private static void onlineJumped(JavaPairInputDStream<String, String> lines) {
		lines.filter(new Function<Tuple2<String, String>, Boolean>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Boolean call(Tuple2<String, String> v1) throws Exception {
				String[] logs = v1._2.split("\t");
				String action = logs[5];
				if ("View".equals(action)) {
					return true;
				} else {
					return false;
				}
			}
		}).mapToPair(new PairFunction<Tuple2<String, String>, Long, Long>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<Long, Long> call(Tuple2<String, String> t) throws Exception {
				String[] logs = t._2.split("\t");
				// Long usrID = Long.valueOf(logs[2] != null ? logs[2] : "-1");
				Long usrID = Long.valueOf("null".equals(logs[2]) ? "-1" : logs[2]);
				return new Tuple2<Long, Long>(usrID, 1L);
			}
		}).reduceByKey(new Function2<Long, Long, Long>() {
			// 对相同的Key，进行Value的累计（包括Local和Reducer级别同时Reduce）
			private static final long serialVersionUID = 1L;

			@Override
			public Long call(Long v1, Long v2) throws Exception {
				return v1 + v2;
			}
		}).filter(new Function<Tuple2<Long, Long>, Boolean>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Boolean call(Tuple2<Long, Long> v1) throws Exception {

				if (1 == v1._2) {
					return true;
				} else {
					return false;
				}
			}
		}).count().print();
		;
	}

	private static void onlineRegistered(JavaPairInputDStream<String, String> lines) {
		lines.filter(new Function<Tuple2<String, String>, Boolean>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Boolean call(Tuple2<String, String> v1) throws Exception {
				String[] logs = v1._2.split("\t");
				String action = logs[5];
				if ("Register".equals(action)) {
					return true;
				} else {
					return false;
				}
			}
		}).count().print();
	}

	private static void onlineUV(JavaPairInputDStream<String, String> lines) {
		/*
		 * 第四步：接下来就像对于RDD编程一样基于DStream进行编程！！！原因是DStream是RDD产生的模板（或者说类），在Spark
		 * Streaming具体 发生计算前，其实质是把每个Batch的DStream的操作翻译成为对RDD的操作！！！
		 * 对初始的DStream进行Transformation级别的处理，例如map、filter等高阶函数等的编程，来进行具体的数据计算
		 */
		JavaPairDStream<String, String> logsDStream = lines.filter(new Function<Tuple2<String, String>, Boolean>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Boolean call(Tuple2<String, String> v1) throws Exception {
				String[] logs = v1._2.split("\t");
				String action = logs[5];
				if ("View".equals(action)) {
					return true;
				} else {
					return false;
				}
			}
		});

		logsDStream.map(new Function<Tuple2<String, String>, String>() {
			private static final long serialVersionUID = 1L;

			@Override
			public String call(Tuple2<String, String> v1) throws Exception {
				String[] logs = v1._2.split("\t");
				Long usrID = Long.valueOf(logs[2] != null ? logs[2] : "-1");
				Long pageID = Long.valueOf(logs[3]);
				return pageID + "_" + usrID;
			}
		}).transform(new Function<JavaRDD<String>, JavaRDD<String>>() {
			private static final long serialVersionUID = 1L;

			@Override
			public JavaRDD<String> call(JavaRDD<String> arg0) throws Exception {
				return arg0.distinct();
			}
		}).mapToPair(new PairFunction<String, Long, Long>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<Long, Long> call(String t) throws Exception {
				String[] logs = t.split("_");
				Long pageId = Long.valueOf(logs[0]);
				return new Tuple2<Long, Long>(pageId, 1L);
			}
		}).reduceByKey(new Function2<Long, Long, Long>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Long call(Long arg0, Long arg1) throws Exception {
				// TODO Auto-generated method stub
				return arg0 + arg1;
			}
		}).print();
	}

	private static void onlinePagePV(JavaPairInputDStream<String, String> lines) {
		/*
		 * 第四步：接下来就像对于RDD编程一样基于DStream进行编程！！！原因是DStream是RDD产生的模板（或者说类），在Spark
		 * Streaming具体 发生计算前，其实质是把每个Batch的DStream的操作翻译成为对RDD的操作！！！
		 * 对初始的DStream进行Transformation级别的处理，例如map、filter等高阶函数等的编程，来进行具体的数据计算
		 * 
		 */
		JavaPairDStream<String, String> logsDStream = lines.filter(new Function<Tuple2<String, String>, Boolean>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Boolean call(Tuple2<String, String> v1) throws Exception {
				String[] logs = v1._2.split("\t");
				String action = logs[5];
				if ("View".equals(action)) {
					return true;
				} else {
					return false;
				}
			}
		});

		/*
		 * 第四步：对初始的DStream进行Transformation级别的处理，例如map、filter等高阶函数等的编程，来进行具体的数据计算
		 * 
		 */
		JavaPairDStream<Long, Long> pairs = logsDStream
				.mapToPair(new PairFunction<Tuple2<String, String>, Long, Long>() {
					private static final long serialVersionUID = 1L;

					@Override
					public Tuple2<Long, Long> call(Tuple2<String, String> t) throws Exception {
						String[] logs = t._2.split("\t");

						Long pageId = Long.valueOf(logs[3]);

						return new Tuple2<Long, Long>(pageId, 1L);
					}
				});

		/*
		 * 第四步：对初始的DStream进行Transformation级别的处理，例如map、filter等高阶函数等的编程，来进行具体的数据计算
		 * 
		 */
		pairs.reduceByKey(new Function2<Long, Long, Long>() { // 对相同的Key，进行Value的累计（包括Local和Reducer级别同时Reduce）
			private static final long serialVersionUID = 1L;

			@Override
			public Long call(Long v1, Long v2) throws Exception {
				return v1 + v2;
			}
		}).print();

	}

}
