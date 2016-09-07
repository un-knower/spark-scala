package com.cgtz.risk.called;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;

import scala.Tuple2;

/**
 * 手机号码认证 通过spark core的方式
 * 将p2p的联系信息表，和用户信息表进行关联，查出哪些用户p2p打过
 * @author Administrator
 *
 */
public class PhoneAuthJoin {
	@SuppressWarnings("resource")
	public static void main(String[] args) {
		SparkConf sparkConf = new SparkConf().setMaster("local").setAppName(
				"PhoneMapJoin");
		JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);

		// 1.获取信贷公司名单，切割成key-value
		JavaRDD<String> com_rowRDD = sparkContext
				.textFile("data/com_mobile.txt");
		JavaPairRDD<String, Tuple2<String, String>> com_phoneRDD = com_rowRDD
				.mapToPair(new PairFunction<String, String, Tuple2<String, String>>() {
					private static final long serialVersionUID = 1L;

					@Override
					public Tuple2<String, Tuple2<String, String>> call(
							String arg0) throws Exception {
						String[] fields = arg0.split("\t");
						if (fields.length != 4) {
							return null;
						}
						return new Tuple2<String, Tuple2<String, String>>(
								fields[2].replaceAll("-", "")
										.replaceAll("―", "")
										.replaceAll("－", "")
										.replaceAll(" ", "")
										.replaceAll("_", ""),
								new Tuple2<String, String>(fields[1], fields[3]));
					}

				});

		
		// 2.collect到driver中
		List<Tuple2<String, Tuple2<String, String>>> com_phoneRDD_List = com_phoneRDD
				.collect();

		// 3.进行广播，可以尽可能节省内存空间，并且减少网络传输性能开销。
		final Broadcast<List<Tuple2<String, Tuple2<String, String>>>> com_phoneBC = sparkContext
				.broadcast(com_phoneRDD_List);

		// 4.获取用户通话记录
		JavaRDD<String> user_rowRDD = sparkContext
				.textFile("data/user_contact.txt");
		JavaRDD<String> user_comRDD = user_rowRDD
				.map(new Function<String, String>() {
					private static final long serialVersionUID = 1L;

					@Override
					public String call(String arg0) throws Exception {
						// 在算子函数中，通过广播变量，获取到本地Executor中的com_phoneRDD数据。
						List<Tuple2<String, Tuple2<String, String>>> com_phone_list = com_phoneBC
								.value();
						// 可以将com_phoneRDD的数据转换为一个Map，便于后面进行join操作。
						Map<String, Tuple2<String, String>> com_phone_map = new HashMap<String, Tuple2<String, String>>();
						for (Tuple2<String, Tuple2<String, String>> data : com_phone_list) {
							com_phone_map.put(data._1, data._2);
						}
						// 获取用户联系人的数据
						String[] fields = arg0.split("\t");
						if (fields.length != 8) {
							return null;
						}
						String key = fields[4].replaceAll("-", "")
								.replaceAll("―", "").replaceAll("－", "")
								.replaceAll(" ", "").replaceAll("_", "");

						// 从com_phoneRDD数据Map中，根据key获取到可以join到的数据。
						Tuple2<String, String> com_name = com_phone_map
								.get(key);
						if (com_name == null) {
							return arg0 + "\t" + "未认证" + "\t" + "未知类型";
						} else {
							return arg0 + "\t" + com_name._1 + "\t"
									+ com_name._2;
						}
					}
				});
		// 保存到数据库中
		List<String> user_com_list = user_comRDD.collect();

		for (String user_com : user_com_list) {
			System.out.println(user_com);
		}
	}
}
