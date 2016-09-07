package com.cgtz.risk.black;

import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;

import scala.Tuple2;

import com.cgtz.risk.utils.SimilarityUtils;

/**
 * 将现有的用户信息 跟黑名单数据进行对比 如果比较得到的相似度为0.8，则认为现有的用户就是在黑名单中
 * 
 * @author Administrator
 *
 */
public class FindFromBlackBySimilarJoin {
	@SuppressWarnings("resource")
	public static void main(String[] args) {
		SparkConf sparkConf = new SparkConf().setMaster("local[10]")
				.setAppName("FindFromBlackBySimilarJoin");
		JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);

		// 1.获取用户数据，切割成key-value
		JavaRDD<String> user_rowRDD = sparkContext
				.textFile("data/user_all.txt");
		JavaPairRDD<String, String> line_rowRDD = user_rowRDD
				.mapToPair(new PairFunction<String, String, String>() {
					private static final long serialVersionUID = 1L;

					@Override
					public Tuple2<String, String> call(String arg0)
							throws Exception {
						String[] fields = arg0.split("\t");
						// 三个字段
						if (fields.length == 3) {
							return new Tuple2<String, String>(arg0, fields[1]
									+ fields[0] + fields[2]);
							// 俩个字段
						} else if (fields.length == 2) {
							// 两个字段为电话、身份证
							if (fields[1].length() == 18) {
								return new Tuple2<String, String>(arg0,
										fields[0] + fields[1]);
							} else {
								// 两个字段为电话、名字
								return new Tuple2<String, String>(arg0,
										fields[1] + fields[0]);
							}
							// 一个字段
						} else {
							return new Tuple2<String, String>(arg0, fields[0]);
						}
					}

				});

		// 2.collect到driver中
		List<Tuple2<String, String>> line_rowRDD_List = line_rowRDD.collect();
		// for (Tuple2<String, String> line : line_rowRDD_List) {
		// System.out.println(line._1+"	"+line._2);
		// }

		// 3.进行广播，可以尽可能节省内存空间，并且减少网络传输性能开销。
		final Broadcast<List<Tuple2<String, String>>> com_phoneBC = sparkContext
				.broadcast(line_rowRDD_List);

		// 4.获取黑名单记录
		JavaRDD<String> black_rowRDD = sparkContext
				.textFile("data/k_blacklist_user_info.txt");
		JavaRDD<String> line_blackRDD = black_rowRDD.map(
				new Function<String, String>() {
					private static final long serialVersionUID = 1L;
					// 获取广播变量
					List<Tuple2<String, String>> com_phone_list = com_phoneBC
							.value();

					@Override
					public String call(String arg0) throws Exception {
						// 获取黑名单数据
						String fields[] = arg0.split("\t");
						String line;
						// 四个字段
						if (fields.length == 4) {
							line = fields[1] + fields[2] + fields[3];
							// 三个字段
						} else if (fields.length == 3) {
							line = fields[1] + fields[2];
							// 两个字段
						} else {
							line = fields[1];
						}

						// 将黑名单数据匹配
						for (Tuple2<String, String> tu : com_phone_list) {
							String key = tu._1;
							String value = tu._2;
							double sim = SimilarityUtils.sim(line, value);
							if (sim >= 0.6d) {
								return key + "	" + sim;
							}
						}
						return null;
					}
				}).filter(new Function<String, Boolean>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Boolean call(String v1) throws Exception {
				if (null != v1 && !"".equals(v1.trim())) {
					return true;
				}
				return false;
			}
		});
		// 保存到数据库中
		List<String> user_com_list = line_blackRDD.collect();

		for (String user_com : user_com_list) {
			System.out.println(user_com);
		}
	}
}
