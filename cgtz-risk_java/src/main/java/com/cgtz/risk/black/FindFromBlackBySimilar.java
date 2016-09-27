package com.cgtz.risk.black;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.List;

import com.cgtz.risk.utils.SimilarityUtils;

/**
 * 将现有的用户信息 跟黑名单数据进行对比 如果比较得到的相似度为0.8，则认为现有的用户就是在黑名单中
 * 
 * @author Administrator
 *
 */
public class FindFromBlackBySimilar {
	public static void main(String[] args) {
		try {
			List<String> blackList = getBlackList();
			List<String> userList = getUserList();
			
			// 从两个集合中比较相似度的用户
			for(String user:userList){
				System.out.println(user);
				for(String black:blackList){
					System.out.println(user+"	"+black);
					double sim = SimilarityUtils.sim(user, black);
					if (sim >= 0.8d) {
						System.out.println(user);
					}
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * 获取黑名单集合
	 * 
	 * @return
	 * @throws Exception
	 */
	public static List<String> getBlackList() throws Exception {
		List<String> blackList = new ArrayList<String>();
		FileReader fr = new FileReader("data/k_blacklist_user_info.txt");
		BufferedReader br = new BufferedReader(fr);
		String line = "";
		while ((line = br.readLine()) != null) {
			String fields[] = line.split("\t");
			// 四个字段
			if (fields.length == 4) {
				blackList.add(fields[1] + "|" +fields[2] + "|"+fields[3]);
				// 三个字段
			} else if (fields.length == 3) {
				blackList.add(fields[1] + "|" + fields[2]);
				// 两个字段
			} else if (fields.length == 2) {
				blackList.add(fields[1]);
			}
		}
		br.close();
		fr.close();
		return blackList;
	}

	/**
	 * 获取用户的集合
	 * 
	 * @return
	 * @throws Exception
	 */
	public static List<String> getUserList() throws Exception {
		List<String> userList = new ArrayList<String>();
		FileReader fr = new FileReader("data/user_all.txt");
		BufferedReader br = new BufferedReader(fr);
		String line = "";
		while ((line = br.readLine()) != null) {
			String[] fields = line.split("\t");
			// 三个字段
			if (fields.length == 3) {
				userList.add(fields[1] +  "|" +fields[0] + "|" + fields[2]);
				// 俩个字段
			} else if (fields.length == 2) {
				// 两个字段为电话、身份证
				if (fields[1].length() == 18) {
					userList.add(fields[0] + "|" + fields[1]);
				} else {
					// 两个字段为电话、名字
					userList.add(fields[1] + "|" + fields[0]);
				}
				// 一个字段
			} else {
				userList.add(fields[0]);
			}
		}
		br.close();
		fr.close();
		return userList;
	}
}
