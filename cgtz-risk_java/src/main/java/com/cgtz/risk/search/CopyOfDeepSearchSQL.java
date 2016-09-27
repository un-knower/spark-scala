package com.cgtz.risk.search;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

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
 * 最新代码
 * 所有的遍历都是通过map shuffle
 * @author Administrator
 *
 */
public class CopyOfDeepSearchSQL {
	// 当前用户组的广播
	private static Broadcast<List<Tuple2<String, Integer>>> currentUserBroadCast= null;
	// 迭代完成后新的圈子号
	private static int newCircle = 0;
	// 整个迭代中所有涉及到的用户
	private static Broadcast<Set<Integer>> allUserBroadCast = null;
	// 保存整个迭代过程中所有使用到的用户的集合
	private static Set<Integer> allUserSet = new HashSet<Integer>();
	// 一个用户一次迭代过程中共同的元素
	private static Broadcast<List<Integer>> oneUserBroadCast = null;
	// 存放一个用户迭代完成后所属圈子的成员的原始ID号
	private static Set<Integer> oneUserCircleSet = new HashSet<Integer>();
	// 存放一个用户迭代完成后所属圈子的成员的原始ID号的广播
	private static Broadcast<Set<Integer>> oneUserCircleBroadCast = null;
	// 保存当前用户的rdd
	private static JavaPairRDD<String, Integer> usertmpjavaRDD = null;
	
	public static void main(String[] args) {
		// 1.配置
		SparkConf sparkConf = new SparkConf().
				setMaster("local").
				setAppName("DeepSearchSQL");
		JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
		
		// 2.获取用户电话和联系人电话
		JavaRDD<String> line_data = sparkContext
				.textFile("data/test.txt").cache();
		
		// 3.数据格式化
		// 3.1.获取声请人去重,圈子号
		 JavaPairRDD<String, Integer> userPhoneGroupRdd= line_data.map(new Function<String, String>() {
			private static final long serialVersionUID = 1L;
			@Override
			public String call(String v1) throws Exception {
				String split[] =  v1.split("\t");
				if(split.length != 4){
					return null;
				}
				return split[1];
			}
		}).filter(new Function<String, Boolean>() {
			private static final long serialVersionUID = 1L;
			@Override
			public Boolean call(String v1) throws Exception {
				if(v1!=null){
					return true;
				}
				return false;
			}
		}).distinct().mapToPair(new PairFunction<String, String, Integer>() {
			private static final long serialVersionUID = 1L;
			int i=0;
			@Override
			public Tuple2<String, Integer> call(String t) throws Exception {
				i++;
				return new Tuple2<String, Integer>(t,i);
			}
		});
		 
		// 3.2.获取申请人,联系人
		JavaPairRDD<String, String> userContactGroupRdd = line_data.mapToPair(new PairFunction<String,String, String>() {
			private static final long serialVersionUID = 1L;
			@Override
			public Tuple2<String, String> call(String t) throws Exception {
				String split[] =  t.split("\t");
				if(split.length != 4){
					return null;
				}
				return new Tuple2<String, String>(split[1],split[3]);
			}
		}).distinct().filter(new Function<Tuple2<String,String>, Boolean>() {
			private static final long serialVersionUID = 1L;
			@Override
			public Boolean call(Tuple2<String, String> v1) throws Exception {
				if(v1!=null){
					return true;
				}
				return false;
			}
		});
		
		// 3.3.join操作
		JavaPairRDD<String, Tuple2<Integer, String>>  PhoneGroupRDD = userPhoneGroupRdd.join(userContactGroupRdd);
		JavaPairRDD<String, Integer> javaRDD = PhoneGroupRDD.flatMap(
				new FlatMapFunction<Tuple2<String,Tuple2<Integer, String>>, String>() {
			private static final long serialVersionUID = 1L;
			@Override
			public Iterable<String> call(
					Tuple2<String, Tuple2<Integer, String>> t) throws Exception {
				List<String> list = new ArrayList<String>();
				list.add(t._1+"\t"+t._2._1);
				list.add(t._2._2+"\t"+t._2._1);
				return list;
			}
		}).distinct().mapToPair(new PairFunction<String, Integer, String>() {
			private static final long serialVersionUID = 1L;
			@Override
			public Tuple2<Integer, String> call(String t) throws Exception {
				String split[] = t.split("\t");
				return new Tuple2<Integer, String>(Integer.parseInt(split[1]),split[0]);
			}
		}).sortByKey().mapToPair(new PairFunction<Tuple2<Integer,String>, String, Integer>() {
			private static final long serialVersionUID = 1L;
			@Override
			public Tuple2<String, Integer> call(Tuple2<Integer, String> t)
					throws Exception {
				return new Tuple2<String, Integer>(t._2,t._1);
			}
		}).cache();

		// 4.获取用户的总的组数
		List<Tuple2<String, Integer>> tuple2s = javaRDD.collect();
		Integer count = tuple2s.get(tuple2s.size()-1)._2;
		
		// 5.迭代所有的用户组
		for(int i=1 ; i<= count ; i++){
			// 如果迭代的过程中，使用过该组，则跳过
			if(allUserSet.contains(i)){
				continue;
			}else{
				newCircle ++;
			}
			// 当前用户的圈子清空
			oneUserCircleSet.clear();
			// 当前该组用户的id临时编号
			int id = i;
			// 得到当前用户的集合
			JavaPairRDD<String, Integer> currentUserRDD = javaRDD
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
			
			// 将当前用户广播出去找相同元素，而不是reduce join,是map join
			List<Tuple2<String, Integer>> currentGroup = currentUserRDD.collect();
			currentUserBroadCast = sparkContext.broadcast(currentGroup);
			
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
			
			// 将当前用户广播出去找相同元素(且不包含自己)
			JavaPairRDD<String, Tuple2<Integer, Integer>> CommonFilterCurrentRDD =  
					contactjavaRDD.mapToPair(new PairFunction<Tuple2<String,Integer>, String, Tuple2<Integer,Integer>>() {
				private static final long serialVersionUID = 1L;
				@Override
				public Tuple2<String, Tuple2<Integer,Integer>> call(Tuple2<String, Integer> t)
						throws Exception {
					List<Tuple2<String, Integer>> list = currentUserBroadCast.value();
					for(Tuple2<String, Integer> tu:list){
						if(tu._1.equals(t._1)){
							return new Tuple2<String, Tuple2<Integer,Integer>>(t._1,new Tuple2<Integer,Integer>(tu._2,t._2));
						}
					}
					return null;
				}
			}).distinct().filter(new Function<Tuple2<String,Tuple2<Integer,Integer>>, Boolean>() {
				private static final long serialVersionUID = 1L;
				@Override
				public Boolean call(Tuple2<String, Tuple2<Integer, Integer>> v1)
						throws Exception {
					if(v1!=null){
						return true;
					}
					return false;
				}
			});
			
			// 如果有共同元素
			long CommonFilterCurrentSize = CommonFilterCurrentRDD.count();
			if(CommonFilterCurrentSize != 0){
				// 保存当前用户的rdd,用于去除共同元素集合的当前用户的集合
				usertmpjavaRDD = currentUserRDD;
				// 开始迭代
				phoneIterator(sparkContext, javaRDD,CommonFilterCurrentRDD,currentUserRDD,id);
			}else{
				// 第一轮没有共同元素则该用户和联系人就为一个圈子
				for(Tuple2<String, Integer> li:currentGroup){
					System.out.println(li._1+"\t"+newCircle);
				}
			}
		}
	}
	
	private static void phoneIterator(JavaSparkContext sparkContext,
			JavaPairRDD<String, Integer> javaRDD,
			JavaPairRDD<String, Tuple2<Integer, Integer>> joinRDD,
			JavaPairRDD<String, Integer> userjavaRDD,
			int id) {
		// 取出共同元素的id（通过号码找id）
		JavaRDD<Integer>  commonPhoneRDD = null;
		// 通过id找共同元素的全集
		JavaPairRDD<String, Integer> commonCollectRDD = null;
		// 全局元素中，去除共同元素的全局元素
		JavaPairRDD<String, Integer> subCommonRDD = null;
		
		// 将共同元素放到集合中
		List<Tuple2<String, Tuple2<Integer, Integer>>> commonidsList = joinRDD.collect();
		for (Tuple2<String, Tuple2<Integer, Integer>> name : commonidsList) {
			oneUserCircleSet.add(name._2._1);
			oneUserCircleSet.add(name._2._2);
		}
		
		// 取出共同元素的id并去重（通过号码找id）
		commonPhoneRDD = joinRDD.flatMap(new FlatMapFunction<Tuple2<String,Tuple2<Integer,Integer>>, Tuple2<Integer,Integer>>() {
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
		
		// 用户共同元素id集合
		List<Integer> ids = commonPhoneRDD.collect();
		// 存放每次用过的id
		allUserSet.addAll(ids);
		
		// 广播所有使用过的id
		allUserBroadCast = sparkContext.broadcast(allUserSet);
		// 广播集合的共同元素集合id
		oneUserBroadCast = sparkContext.broadcast(ids);
		// 广播每一轮迭代完成后，该圈子的用户
		oneUserCircleBroadCast = sparkContext.broadcast(oneUserCircleSet);
		
		// 通过id找共同元素的全集
		commonCollectRDD = javaRDD.filter(new Function<Tuple2<String,Integer>, Boolean>() {
			private static final long serialVersionUID = 1L;
			@Override
			public Boolean call(Tuple2<String, Integer> v1) throws Exception {
				List<Integer> list = oneUserBroadCast.value();
				if (list.contains(v1._2)) {
					return true;
				} else {
					return false;
				}
			}
		});
		
		// 得到共同元素的全集，去除共同元素和刚才匹配过的集合本身
		// 1.去除本身集合
		JavaPairRDD<String, Integer> subRDD = commonCollectRDD.subtract(usertmpjavaRDD);
		// 2.去除共同元素
		userjavaRDD = subRDD.subtractByKey(joinRDD);
		// 3.获取获取共同元素的集合
		List<Tuple2<String, Integer>> commonListGroup = userjavaRDD.collect();
		// 4.广播该集合
		currentUserBroadCast = sparkContext.broadcast(commonListGroup);
		
		// 全局元素中，去除当前id以上的集合和已经有关联的集合
		subCommonRDD = javaRDD.filter(new Function<Tuple2<String,Integer>, Boolean>() {
			private static final long serialVersionUID = 1L;
			@Override
			public Boolean call(Tuple2<String, Integer> v1) throws Exception {
				Set<Integer> list = allUserBroadCast.value();
				if(list.contains(v1._2)){
					return false;
				}else if(v1._2.intValue() < id){
					return false;
				}else{
					return true;
				}
			}
		});
		
		// 不是join操作，而是从原来的集合中查找
		joinRDD = subCommonRDD.mapToPair(new PairFunction<Tuple2<String,Integer>, String, Tuple2<Integer,Integer>>() {
			private static final long serialVersionUID = 1L;
			@Override
			public Tuple2<String, Tuple2<Integer, Integer>> call(
					Tuple2<String, Integer> t) throws Exception {
				List<Tuple2<String, Integer>> list = currentUserBroadCast.getValue();
				for(Tuple2<String, Integer> tu:list){
					if(tu._1.equals(t._1)){
						return new Tuple2<String, Tuple2<Integer,Integer>>(t._1,new Tuple2<Integer,Integer>(tu._2,t._2));
					}
				}
				return null;
			}
		}).distinct().filter(new Function<Tuple2<String,Tuple2<Integer,Integer>>, Boolean>() {
			private static final long serialVersionUID = 1L;
			@Override
			public Boolean call(Tuple2<String, Tuple2<Integer, Integer>> v1)
					throws Exception {
				if(v1!=null){
					return true;
				}
				return false;
			}
		});
				
		long joinAgainRDD= joinRDD.count();
		if(joinAgainRDD != 0){
			usertmpjavaRDD = subRDD;
			phoneIterator(sparkContext, javaRDD,joinRDD,userjavaRDD,id);
		}else{
			//System.out.println("@@@没有找到共同元素,迭代终止,并打印该圈子@@@");
			JavaRDD<String> circleRDD= javaRDD.map(new Function<Tuple2<String,Integer>, String>() {
				private static final long serialVersionUID = 1L;
				@Override
				public String call(Tuple2<String, Integer> v1) throws Exception {
					Set<Integer> set = oneUserCircleBroadCast.value();
					if(set.contains(v1._2)){
						return v1._1;
					}else{
						return null;
					}
				}
			}).filter(new Function<String, Boolean>() {
				private static final long serialVersionUID = 1L;
				@Override
				public Boolean call(String v1)
						throws Exception {
					if(v1 != null){
						return true;
					}
					return false;
				}
			});
			List<String> list = circleRDD.collect();
			Set<String> set = new HashSet<String>();
			set.addAll(list);
			for(String li : set){
				System.out.println(li+"\t"+newCircle);
			}
			return ;
		}
	}
}
