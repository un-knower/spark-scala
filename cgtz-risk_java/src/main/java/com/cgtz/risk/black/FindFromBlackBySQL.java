package com.cgtz.risk.black;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import com.mysql.jdbc.StringUtils;

/**
 * 通过用户的原始记录去匹配黑名单的记录 查找出用户中的黑名单
 * 
 * @author Administrator
 *
 */
public class FindFromBlackBySQL {
	public static void main(String[] args) {
		// 1.配置
		SparkConf sparkConf = new SparkConf().setMaster("local[4]").setAppName(
				"FindFromBlackBySQL");
		JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
		SQLContext sqlContext = new SQLContext(sparkContext);

		// 2.获取用户信息记录
		JavaRDD<String> user_data = sparkContext.textFile("data/user_all.txt");

		JavaRDD<Row> userRowRDD = user_data.map(new Function<String, Row>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Row call(String arg0) throws Exception {
				String[] fields = arg0.split("\t");
				// 只有一个字段
				if (fields.length == 1) {
					// 如果是手机号
					if (fields[0].length() == 11) {
						return RowFactory.create(fields[0].substring(0, 8), "",
								"", arg0);
						// 如果是身份证
					} else if (fields[0].length() == 18) {
						return RowFactory.create("", "",
								fields[0].substring(0, 14), arg0);
						// 否则为名字
					} else {
						return RowFactory.create("", fields[0], "", arg0);
					}
					// 只有两个字段
				} else if (fields.length == 2) {
					// 一二字段为号码、名字
					if (fields[0].length() == 11 && fields[1].length() != 18) {
						return RowFactory.create(fields[0].substring(0, 8),
								fields[1], "", arg0);
						// 一二字段为号码、身份证
					} else if (fields[0].length() == 11
							&& fields[1].length() == 18) {
						return RowFactory.create(fields[0].substring(0, 8), "",
								fields[1].substring(0, 14), arg0);
						// 一二字段为名字、身份证
					} else {
						if (fields[1].length() == 18) {
							return RowFactory.create("", fields[0],
									fields[1].substring(0, 14), arg0);
						} else {
							return RowFactory.create("", fields[0], fields[1],
									arg0);
						}
					}
					// 有三个字段
				} else {
					if (fields[0].length() == 11 && fields[2].length() == 18) {
						return RowFactory.create(fields[0].substring(0, 8),
								fields[1], fields[2].substring(0, 14), arg0);
					} else if (fields[0].length() == 11) {
						return RowFactory.create(fields[0].substring(0, 8),
								fields[1], fields[2], arg0);
					} else if (fields[2].length() == 18) {
						return RowFactory.create(fields[0], fields[1],
								fields[2].substring(0, 14), arg0);
					} else {
						return RowFactory.create(fields[0], fields[1],
								fields[2], arg0);
					}
				}
			}
		});

		// 3.字段映射
		String userSchema = "user_phone user_name user_idcard fullAll";
		List<StructField> user_fields = new ArrayList<StructField>();
		for (String fieldName : userSchema.split(" ")) {
			user_fields.add(DataTypes.createStructField(fieldName,
					DataTypes.StringType, true));
		}
		StructType userType = DataTypes.createStructType(user_fields);

		// 4.註冊成表
		DataFrame userContactDataFrame = sqlContext.createDataFrame(userRowRDD,
				userType);
		userContactDataFrame.registerTempTable("user_info");

		// 5.获取黑名单记录
		JavaRDD<String> blacklist_data = sparkContext
				.textFile("data/k_blacklist_user_info.txt");
		JavaRDD<Row> blacklistRowRDD = blacklist_data
				.map(new Function<String, Row>() {
					private static final long serialVersionUID = 1L;

					@Override
					public Row call(String arg0) throws Exception {
						String[] fields = arg0.split("\t");
						// 长度为2
						if (fields.length == 2) {
							// 如果为手机号码
							if (fields[1].length() == 11) {
								return RowFactory.create(fields[0], "",
										fields[1].substring(0, 8), "");
								// 如果为身份证
							} else if (fields[1].length() == 18) {
								return RowFactory.create(fields[0], "", "",
										fields[1].substring(0, 14));
								// 否则为名字
							} else {
								return RowFactory.create(fields[0], fields[1],
										"", "");
							}
						}
						// 长度为3
						if (fields.length == 3) {
							// 一二字段为号码、身份证
							if (fields[1].length() == 11
									&& fields[2].length() == 18) {
								return RowFactory.create(fields[0], "",
										fields[1].substring(0, 8),
										fields[2].substring(0, 14));
								// 一二字段为名字、身份证
							} else if (fields[2].length() == 18) {
								return RowFactory.create(fields[0], fields[1],
										"", fields[2].substring(0, 14));
								// 否则为名字、号码
							} else {
								if (fields[2].length() == 11) {
									return RowFactory.create(fields[0],
											fields[1],
											fields[2].substring(0, 8), "");
								}
								return RowFactory.create(fields[0], fields[1],
										fields[2], "");
							}
							// 长度为4
						} else {
							if (fields[2].length() == 11
									&& fields[3].length() == 18) {
								return RowFactory.create(fields[0], fields[1],
										fields[2].substring(0, 8),
										fields[3].substring(0, 14));
							} else if (fields[2].length() == 11) {
								return RowFactory.create(fields[0], fields[1],
										fields[2].substring(0, 8), fields[3]);
							} else if (fields[3].length() == 18) {
								return RowFactory.create(fields[0], fields[1],
										fields[2], fields[3].substring(0, 14));
							} else {
								return RowFactory.create(fields[0], fields[1],
										fields[2], fields[3]);
							}
						}
					}
				});

		// 6.字段映射
		String blacklistSchema = "blacklist_id blacklist_name blacklist_phone blacklist_idcard";
		List<StructField> blacklist_fields = new ArrayList<StructField>();
		for (String fieldName : blacklistSchema.split(" ")) {
			blacklist_fields.add(DataTypes.createStructField(fieldName,
					DataTypes.StringType, true));
		}
		StructType blacklistType = DataTypes.createStructType(blacklist_fields);

		// 7.註冊成表
		DataFrame blacklistContactDataFrame = sqlContext.createDataFrame(
				blacklistRowRDD, blacklistType);
		blacklistContactDataFrame.registerTempTable("black_info");

		// 8.关联查询
		DataFrame results = sqlContext
				.sql("SELECT user_info.fullAll,black_info.blacklist_id "
						+ "FROM  user_info "
						+ "JOIN black_info "
						+ "ON user_info.user_phone = black_info.blacklist_phone "
						+ "WHERE user_info.user_phone!='' "
						+ "UNION "
						+ "SELECT user_info.fullAll,black_info.blacklist_id "
						+ "FROM  user_info "
						+ "JOIN black_info "
						+ "ON user_info.user_idcard = black_info.blacklist_idcard "
						+ "WHERE user_info.user_idcard!=''");

		// 9.结果打印
		List<String> names = results.javaRDD().map(new Function<Row, String>() {
			private static final long serialVersionUID = 1L;

			@Override
			public String call(Row row) throws Exception {
				String fullAll = row.getString(0);
				String blackListid = row.getString(1);
				if (StringUtils.isEmptyOrWhitespaceOnly(fullAll)) {
					fullAll = "这天记录为空";
				}
				if (StringUtils.isEmptyOrWhitespaceOnly(blackListid)) {
					blackListid = "blackListid";
				}
				return fullAll + "	" + blackListid;
			}
		}).collect();
		for (String name : names) {
			System.out.println(name);
		}
	}
}
