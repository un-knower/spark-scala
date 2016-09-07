package com.cgtz.risk.search;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;

import scala.Tuple2;

/**
 * 深度遍历算法得到用户的关系圈
 * 
 * @author Administrator
 *
 */
public class CopyOfDeepSearchSQL {
	private static int circleTime = 0;
	private static Broadcast<Set<Integer>> allID = null;
	// 保存每个用户迭代过程中所有使用的到其他用户的集合
	private static Set<Integer> allUseIds = new HashSet<Integer>();
	private static Broadcast<List<Integer>> commonID = null;
	private static Accumulator<Integer> accumulator = null;
	// 保存当前用户的rdd
	private static JavaPairRDD<String, Integer> usertmpjavaRDD = null;
	
	public static void main(String[] args) {
		// 1.配置
		SparkConf sparkConf = new SparkConf().setMaster("local").setAppName(
				"DeepSearchSQL");
		JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
		
		// 2.获取用户电话和联系人电话
		JavaRDD<String> line_data = sparkContext
				.textFile("data/test.txt");
		
		accumulator = sparkContext.accumulator(0,"acc");
		JavaPairRDD<String, Integer>  javaRDD = line_data.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
			private static final long serialVersionUID = 1L;
			int i = 0;
			String user = "";
			@Override
			public Iterable<Tuple2<String, Integer>> call(String t) throws Exception {
				String spilt[] =  t.split("\t");
				if(spilt.length != 4){
					return null;
				}
				if(!user.equals(spilt[1])){
					accumulator.add(1);
					i++;
					user = spilt[1];
					return Arrays.asList(new Tuple2<String, Integer>(user,i),new Tuple2<String, Integer>(spilt[3],i));
				}
				List<Tuple2<String, Integer>> list = Arrays.asList(new Tuple2<String, Integer>(spilt[3],i));
				return list;
			}
		}).mapToPair(new PairFunction<Tuple2<String,Integer>, String, Integer>() {
			private static final long serialVersionUID = 1L;
			@Override
			public Tuple2<String, Integer> call(Tuple2<String, Integer> t)
					throws Exception {
				return new Tuple2<String, Integer>(t._1,t._2);
			}
		}).cache();
		
		javaRDD.count();
		Integer count = accumulator.value();
		// 迭代所有的组
		for(int i=1 ; i<= count ; i++){
			// 每次清空该集合
			allUseIds.clear();
			int id = i;
			// 得到当前用户的集合
			JavaPairRDD<String, Integer> userjavaRDD = javaRDD
					.filter(new Function<Tuple2<String, Integer>, Boolean>() {
						private static final long serialVersionUID = 1L;
						@Override
						public Boolean call(Tuple2<String, Integer> v1)
								throws Exception {
							if (v1._2 == id) {
								return true;
							}
							return false;
						}
					});
			
			// 得到其他用户的集合(全集集合，去除上述的集合)
			JavaPairRDD<String, Integer> contactjavaRDD = javaRDD
					.filter(new Function<Tuple2<String, Integer>, Boolean>() {
						private static final long serialVersionUID = 1L;

						@Override
						public Boolean call(Tuple2<String, Integer> v1)
								throws Exception {
							if (v1._2 > id) {
								return true;
							}
							return false;
						}
					});
			
			
			// 进行关联,找共同元素
			JavaPairRDD<String, Tuple2<Integer, Integer>> joinRDD = contactjavaRDD.join(userjavaRDD);
			long commonCount = joinRDD.count();
			
			// 第一轮迭代，如果有共同元素
			if(commonCount!=0){
				// 保存当前用户的rdd,用于去除共同元素集合的当前用户的集合
				usertmpjavaRDD = userjavaRDD;
				// 轮次加1
				circleTime ++;
				// 开始迭代
				phoneIterator(sparkContext, javaRDD,joinRDD,userjavaRDD,id);
			}
		}
	}
	private static void phoneIterator(JavaSparkContext sparkContext,
			JavaPairRDD<String, Integer> javaRDD,
			JavaPairRDD<String, Tuple2<Integer, Integer>> joinRDD,
			JavaPairRDD<String, Integer> userjavaRDD,
			int id) {
		
		// 取出共同元素的id（通过号码找id）
		JavaRDD<Integer>  phone2IDRDD = null;
		// 通过id找共同元素的全集
		JavaPairRDD<String, Integer> commonCollectRDD = null;
		// 全局元素中，去除共同元素的全局元素
		JavaPairRDD<String, Integer> subCommonRDD = null;
		
		// 打印共同元素
		System.out.println("-------集合共同元素--------");
		List<Tuple2<String, Tuple2<Integer, Integer>>> idaaaa = joinRDD.collect();
		for (Tuple2<String, Tuple2<Integer, Integer>> name : idaaaa) {
			System.out.println("("+name._1+","+name._2._1+","+circleTime+")");
			System.out.println("("+name._1+","+name._2._2+","+circleTime+")");
		}
		
		// 取出共同元素的id（通过号码找id）
		phone2IDRDD = joinRDD.flatMap(new FlatMapFunction<Tuple2<String,Tuple2<Integer,Integer>>, Tuple2<Integer,Integer>>() {
			private static final long serialVersionUID = 1L;
			@Override
			public Iterable<Tuple2<Integer, Integer>> call(
					Tuple2<String, Tuple2<Integer, Integer>> t)
					throws Exception {
				return Arrays.asList(new Tuple2<Integer, Integer>(t._2._1,1),new Tuple2<Integer, Integer>(t._2._2,1));
			}
		}).map(new Function<Tuple2<Integer,Integer>, Integer>() {
			private static final long serialVersionUID = 1L;
			@Override
			public Integer call(Tuple2<Integer, Integer> v1) throws Exception {
				return v1._1;
			}
		}).distinct();
		
		// 获取所有用过的id集合
		allUseIds.addAll(phone2IDRDD.collect());
		// 用户共同元素id集合
		List<Integer> ids = phone2IDRDD.collect();
		// 广播所有使用过的id
		allID = sparkContext.broadcast(allUseIds);
		// 广播集合的共同元素集合id
		commonID = sparkContext.broadcast(ids);
		
		// 通过id找共同元素的全集
		commonCollectRDD = javaRDD.filter(new Function<Tuple2<String,Integer>, Boolean>() {
			private static final long serialVersionUID = 1L;
			@Override
			public Boolean call(Tuple2<String, Integer> v1) throws Exception {
				List<Integer> list = commonID.value();
				if (list.contains(v1._2)) {
					return true;
				} else {
					return false;
				}
			}
		});
		// 打印共同元素全集
		System.out.println("-------共同元素全集--------");
		List<Tuple2<String, Integer>> idbbbbb = commonCollectRDD.collect();
		for (Tuple2<String, Integer> name : idbbbbb) {
			System.out.println(name);
		}
		
		// 得到共同元素的全集，去除共同元素和刚才匹配过的集合本身
		// 1.去除本身集合
		JavaPairRDD<String, Integer> subRDD = commonCollectRDD.subtract(usertmpjavaRDD);
		System.out.println("-------全集去除当前集合本身--------");
		List<Tuple2<String, Integer>> idbbbcc = subRDD.collect();
		for (Tuple2<String, Integer> name : idbbbcc) {
			System.out.println(name);
		}
		// 2.去除共同元素
		userjavaRDD = subRDD.subtractByKey(joinRDD);
		System.out.println("-------全集去除共同元素其他元素和刚才匹配过的集合本身--------");
		List<Tuple2<String, Integer>> idccccc = userjavaRDD.collect();
		for (Tuple2<String, Integer> name : idccccc) {
			System.out.println(name);
		}
		
		// 全局元素中，去除当前id以上的集合和已经有关联的集合
		subCommonRDD = javaRDD.filter(new Function<Tuple2<String,Integer>, Boolean>() {
			private static final long serialVersionUID = 1L;
			@Override
			public Boolean call(Tuple2<String, Integer> v1) throws Exception {
				Set<Integer> list = allID.value();
				if(list.contains(v1._2)){
					return false;
				}else if(v1._2.intValue() < id){
					return false;
				}else{
					return true;
				}
			}
		});
		System.out.println("-------全局元素中，去除当前id以上的集合和已经有关联的集合--------");
		List<Tuple2<String, Integer>> idddddd = subCommonRDD.collect();
		for (Tuple2<String, Integer> name : idddddd) {
			System.out.println(name);
		}
		
		joinRDD = subCommonRDD.join(userjavaRDD);
		long joinAgainRDD= joinRDD.count();
		if(joinAgainRDD != 0){
			usertmpjavaRDD = subRDD;
			circleTime ++;
			phoneIterator(sparkContext, javaRDD,joinRDD,userjavaRDD,id);
		}else{
			System.out.println("###########没有找到共同元素，迭代终止##############");
			return ;
		}
	}
}
