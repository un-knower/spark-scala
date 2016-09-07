package com.cgtz.risk.called;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.Row;

/**
 * 手机号码认证 通过sparksql的方式 将p2p的联系信息表，
 * 和用户信息表进行关联，查出哪些用户p2p打过 
 * 
 * @author Administrator
 *
 */
public class PhoneAuthSQL {
	private static final String JDBCURL = "jdbc:mysql://172.16.34.48:3306/cgjrRisk";

	public static void main(String[] args) {
		SparkConf sparkConf = new SparkConf().setMaster("local").setAppName(
				"PhoneAuth");
		JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
		SQLContext sqlContext = new SQLContext(sparkContext);
		// 1.加载mysql信息 获取信贷公司名单
		Properties property = new Properties();
		property.put("user", "risk");
		property.put("password", "9674596");
		DataFrame jdbcDF = sqlContext.read().jdbc(JDBCURL, "k_name_phone_type",
				property);
		jdbcDF.registerTempTable("com_contact");

		// 2.获取用户通话记录
		JavaRDD<String> user_data = sparkContext
				.textFile("data/user_contact.txt");
		JavaRDD<Row> userRowRDD = user_data.map(new Function<String, Row>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Row call(String arg0) throws Exception {
				String[] fields = arg0.split("\t");
				if (fields.length != 8) {
					return null;
				}
				return RowFactory.create(fields[0], fields[1], fields[2],
						fields[3],
						fields[4].replaceAll("-", "").replaceAll("―", "")
								.replaceAll("－", "").replaceAll(" ", "")
								.replaceAll("_", ""), fields[5], fields[6]);
			}
		});
		// 3.字段映射
		String userSchema = "user_id start_time trade_addr call_type receiver_phone trade_time trade_type";
		List<StructField> user_fields = new ArrayList<StructField>();
		for (String fieldName : userSchema.split(" ")) {
			user_fields.add(DataTypes.createStructField(fieldName,
					DataTypes.StringType, true));
		}
		StructType userType = DataTypes.createStructType(user_fields);

		// 4.註冊成表
		DataFrame userContactDataFrame = sqlContext.createDataFrame(userRowRDD,
				userType);
		userContactDataFrame.registerTempTable("user_contact");

		// 5.join查询数据
		DataFrame results = sqlContext
				.sql("SELECT user_contact.* , com_contact.name , com_contact.type FROM"
						+ " user_contact LEFT JOIN com_contact ON "
						+ "user_contact.receiver_phone = com_contact.phone");

		List<String> names = results.javaRDD().map(new Function<Row, String>() {
			private static final long serialVersionUID = 1L;

			@Override
			public String call(Row row) throws Exception {
				return row.getString(0) + "	" + row.getString(1) + "	"
						+ row.getString(2) + "	" + row.getString(3) + "	"
						+ row.getString(4) + "	" + row.getString(5) + "	"
						+ row.getString(6) + "	" + row.getString(7) + "	"
						+ row.getString(8);
			}
		}).collect();
		for (String name : names) {
			System.out.println(name);
		}
	}
}
