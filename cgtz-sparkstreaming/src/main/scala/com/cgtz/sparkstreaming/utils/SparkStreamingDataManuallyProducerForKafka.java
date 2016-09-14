package com.cgtz.sparkstreaming.utils;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;
import java.util.Random;

import kafka.producer.KeyedMessage;
import kafka.producer.Producer;
import kafka.producer.ProducerConfig;
import scala.collection.Seq;

/**
 * 逻辑生成器，一直不断的逻辑级别的模拟用户行为，生成数据 论坛数据自动生成代码，该生成的数据会作为Producer的方式发送给Kafka，
 * 然后SparkStreaming程序会从 Kafka中在线Pull到论坛或者网站的用户在线行为信息，进而进行多维度的在线分析 数据格式如下：
 * date：日期，格式为yyyy-MM-dd 
 * timestamp：时间戳
 * userID:用户ID 
 * pageID:页面ID 
 * chanelID：板块的ID
 * action：点击和注册
 */
public class SparkStreamingDataManuallyProducerForKafka extends Thread {

	// 具体的论坛频道
	static String[] channelNames = new String[] { "Spark", 
			"Scala", "Kafka", "Flink", "Hadoop", "Storm", "Hive",
			"Impala", "HBase", "ML" };
	// 用户的两种行为模式
	static String[] actionNames = new String[] { "View", "Register" };

	private String topic; // 发送给Kafka的数据的类别
	private Producer<Integer, String> producerForKafka;

	private static String dateToday;
	private static Random random;

	// 构造函数，传递数据，构造器的核心是生成数据。
	// 因为要不断的给kafka发送数据，所以继承了Thread
	public SparkStreamingDataManuallyProducerForKafka(String topic) {
		dateToday = new SimpleDateFormat("yyyy-MM-dd").format(new Date());
		this.topic = topic;
		random = new Random();
		Properties conf = new Properties();// 设置属性,链接kafka
		conf.put("metadata.broker.list", "master1:9092,Worker1:9092,Worker2:9092");
		conf.put("serializer.class", "kafka.serializer.StringEncoder");
		producerForKafka = new Producer<Integer, String>(new ProducerConfig(conf));// 运行时基于这个producer进行写数据
	}

	@SuppressWarnings("unchecked")
	@Override
	public void run() {// 重写线程的run方法
		int counter = 0;
		while (true) {// 死循环一直发送消息给kafka
			counter++;
			String userLog = userlogs();// 拿到数据
			System.out.println("product:" + userLog);
			// 死循环发送数据给kafka
			producerForKafka
					.send((Seq<KeyedMessage<Integer, String>>) new KeyedMessage<Integer, String>(topic, userLog));

			if (0 == counter % 500) {
				counter = 0;
				try {
					Thread.sleep(1000);// 每发送500条数据，休息一下，一直发，机器受不了
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
	}

	public static void main(String[] args) {
		// 创建实例，传入topic，运行即可
		new SparkStreamingDataManuallyProducerForKafka("UserLogs").start();

	}

	private static String userlogs() {

		StringBuffer userLogBuffer = new StringBuffer("");
		int[] unregisteredUsers = new int[] { 1, 2, 3, 4, 5, 6, 7, 8 };
		long timestamp = new Date().getTime();
		Long userID = 0L;
		long pageID = 0L;

		// 随机生成的用户ID
		if (unregisteredUsers[random.nextInt(8)] == 1) {
			userID = null;
		} else {
			userID = (long) random.nextInt((int) 2000);
		}

		// 随机生成的页面ID
		pageID = random.nextInt((int) 2000);

		// 随机生成Channel
		String channel = channelNames[random.nextInt(10)];

		// 随机生成action行为
		String action = actionNames[random.nextInt(2)];

		userLogBuffer.append(dateToday).append("\t").append(timestamp).append("\t").append(userID).append("\t")
				.append(pageID).append("\t").append(channel).append("\t").append(action);
		// .append("\n");

		return userLogBuffer.toString();

	}

}