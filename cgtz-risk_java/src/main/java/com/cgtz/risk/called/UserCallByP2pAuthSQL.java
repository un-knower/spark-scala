package com.cgtz.risk.called;

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

/**
 * 这个是被p2p打过的电话的用户通话记录
 * 
 * @author Administrator
 *
 */
public class UserCallByP2pAuthSQL {
	public static void main(String[] args) {
		// 1.配置
		SparkConf sparkConf = new SparkConf().setMaster("local").setAppName(
				"UserCallByP2pAuthSQL");
		JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
		SQLContext sqlContext = new SQLContext(sparkContext);
		
		// 2.获取用户通话记录
		JavaRDD<String> user_data = sparkContext
				.textFile("data/user_CallByP2P.txt");

		JavaRDD<Row> userRowRDD = user_data.map(new Function<String, Row>() {
			private static final long serialVersionUID = 1L;
			@Override
			public Row call(String arg0) throws Exception {
				String[] fields = arg0.split("\t");
				if (fields.length != 9) {
					return null;
				}
				return RowFactory.create(fields[0], fields[1], fields[2],
						fields[3], fields[4], fields[5], fields[6], fields[7],
						fields[8]);
			}
		});

		// 3.字段映射
		String userSchema = "user_id start_time trade_addr call_type trade_time trade_type receiver_phone com_name com_type";
		List<StructField> user_fields = new ArrayList<StructField>();
		for (String fieldName : userSchema.split(" ")) {
			user_fields.add(DataTypes.createStructField(fieldName,
					DataTypes.StringType, true));
		}
		StructType userType = DataTypes.createStructType(user_fields);

		// 4.註冊成表
		DataFrame userContactDataFrame = sqlContext.createDataFrame(userRowRDD,
				userType);
		userContactDataFrame.registerTempTable("user_called");

		// 5.查询数据
		//每个用户被P2P call了多少次
		//DataFrame results = sqlContext.sql("SELECT user_id,count(user_id) as user_sum FROM user_called group by user_id order by user_sum desc");
		//单个用户被call的情况
		DataFrame results = sqlContext.sql("SELECT user_id,start_time,com_name,trade_time FROM user_called where user_id=191805029392 order by start_time desc");
		
		List<String> names = results.javaRDD().map(new Function<Row, String>() {
			private static final long serialVersionUID = 1L;
			@Override
			public String call(Row row) throws Exception {
				//每个用户被P2P到了多少次
				//return row.getString(0) +"	"+row.getLong(1);
				//单个用户被call的情况
				return row.getString(0) +"	"+row.getString(1)+"	"+row.getString(2)+"	"+row.getString(3);
			}
		}).collect();
		
		for (String name : names) {
			System.out.println(name);
		}
	}
}
