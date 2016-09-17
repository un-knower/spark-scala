package com.cgtz.sparkstreaming.start;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import scala.Tuple2;

/**
 * 1：广播可以自定义，例如你自定义广播里面的内容，就有很多你可以自定义的操作。尤其是结合了Broadcast和Accumulator的时候，
 * 他可以实现一些非常复杂的功能。
 * 2：广播和计数器在企业的实际开发中，非常重要，主要是可以自定义，自定义的时候可以实现非常复杂的逻辑。计数器Accumulator可以计数黑名单。
 * 黑名单数据可以写在广播里面
 * 3：下面直接上代码，当然，这只是初步的使用，广播和计算器的自定义，绝对是高端的spark技术。它们俩者结合自定义会发挥非常强大的作用。很多一线互联网公司，
 * 它们很多复杂的业务，都需要联合使用和自定义广播和计数器。
 * 
 * @author xing
 *
 */
public class BroadcastAccumulatorTest {
	// 这个是基于原子型的变量，保存黑名单
	private static volatile Broadcast<List<String>> broadcastList = null;
	private static volatile Accumulator<Integer> accumulator = null;

	@SuppressWarnings("deprecation")
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("WordCountOnline");
		JavaStreamingContext jsc = new JavaStreamingContext(conf, Durations.seconds(15));

		/**
		 * 实例化广播，使用Broadcast广播黑名单到每个Executor中，广播是基于SparkContext的。
		 * 而不是StreamingContext。 没有action，广播是不能发出的
		 */
		broadcastList = jsc.sparkContext().broadcast(Arrays.asList("Hadoop", "Mahout", "Hive"));
		/**
		 * 全局计数器，用于统计在线过滤了多少个黑名单 第一个参数计数初始值肯定是0，第2个参数，accumulator的name
		 */
		accumulator = jsc.sparkContext().accumulator(0, "OnlineBlacklistCounter");
		JavaReceiverInputDStream<String> lines = jsc.socketTextStream("master1", 9999);

		JavaPairDStream<String, Integer> pairs = lines.mapToPair(new PairFunction<String, String, Integer>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<String, Integer> call(String word) throws Exception {
				return new Tuple2<String, Integer>(word, 1);
			}
		});

		JavaPairDStream<String, Integer> wordsCount = pairs.reduceByKey(new Function2<Integer, Integer, Integer>() { // 对相同的Key，进行Value的累计（包括Local和Reducer级别同时Reduce）
			private static final long serialVersionUID = 1L;

			@Override
			public Integer call(Integer v1, Integer v2) throws Exception {
				return v1 + v2;
			}
		});

		wordsCount.foreachRDD(new Function2<JavaPairRDD<String, Integer>, Time, Void>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Void call(JavaPairRDD<String, Integer> arg0, Time arg1) throws Exception {
				// 对数据rdd进行过滤
				arg0.filter(new Function<Tuple2<String, Integer>, Boolean>() {
					private static final long serialVersionUID = 1L;

					@Override
					public Boolean call(Tuple2<String, Integer> arg0) throws Exception {
						// 判断现在循环的每个key，是否是在黑名单中
						if (broadcastList.value().contains(arg0._1)) {
							// 这里添加过滤掉的黑名单的个数，用于全局通知
							// accumulator.add(arg0._2);
							// 包含，return false，过滤掉
							return false;
						} else {
							// 不包含，return true，不过滤
							return true;
						}
					}
				}).collect();// action触发下
				// 连接上nc -lk
				// 9999，输入Hadoop，Spark，Hive，Scala，就会输出2次，是累加的。代表总共过滤了2次黑名单
				// System.out.println(" BlackList appeared : " +
				// accumulator.value() + " times");
				return null;
			}
		});
		/*
		 * Spark
		 * Streaming执行引擎也就是Driver开始运行，
		 * Driver启动的时候是位于一条新的线程中的，当然其内部有消息循环体，用于
		 * 接受应用程序本身或者Executor中的消息；
		 */
		jsc.start();

		jsc.awaitTermination();
		jsc.close();
	}

}
