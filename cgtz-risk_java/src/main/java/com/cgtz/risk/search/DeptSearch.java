package com.cgtz.risk.search;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * 深度遍历，刻画用的关系圈
 * 
 * @author Administrator
 *
 */
public class DeptSearch {
	private static List<List<String>> allUser = new ArrayList<List<String>>();

	public static void main(String args[]) {
		try {
			System.err.println("begin");
			readFileByLine("data/test.txt");
			deepErgodic();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * 读取文件,加载到内存中
	 * 
	 * @param path
	 */
	public static void readFileByLine(String path) {
		try {
			BufferedReader br = new BufferedReader(new FileReader(path));
			// 用户的联系人计数器
			int counter = 1;
			// 用户的编号
			int i = 1;
			// 某个用户的信息
			String user = "";
			String line = null;
			List<String> oneUser = null;
			while ((line = br.readLine()) != null) {
				// 每个用户第一次来的时候一个集合
				if (counter == 1) {
					oneUser = new ArrayList<String>();
				}
				String strs[] = line.split("\t");
				if (strs.length != 4) {
					continue;
				}

				// 第一次获取用户信息
				if (counter == 1) {
					user = strs[0] + "\t" + strs[1];
					String user_info = strs[0] + "\t" + strs[1] + "\t" + i;
					oneUser.add(user_info);
					String contact_info = strs[2] + "\t" + strs[3] + "\t" + i;
					oneUser.add(contact_info);
				} else {
					if (user.equals(strs[0] + "\t" + strs[1])) {
						String contact_info = strs[2] + "\t" + strs[3] + "\t"
								+ i;
						oneUser.add(contact_info);
					}
				}
				// 每个用户三个联系人
				counter++;
				if (counter == 4) {
					user = "";
					counter = 1;
					i++;
					allUser.add(oneUser);
				}

			}
			br.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * 深度遍历算法
	 */
	public static void deepErgodic() {
		// 遍历每一个用户
		if (allUser.size() != 0) {
			int allSize = allUser.size();
			for (int i = 0; i < allSize; i++) {
				// 存放该第一次匹配的集合,包括本身,其他元素如果匹配上,则追加到该集合
				List<List<String>> userUnionList = new ArrayList<List<String>>();
				List<String> tmpList = allUser.get(i);
				userUnionList.add(tmpList);
				// 存放该第一次没有匹配的集合
				List<List<String>> userNoList = new ArrayList<List<String>>();
				// 是否匹配上，没匹配上则将该本次集合清空
				boolean flag = false;

				// 遍历单个用户的联系人
				if (tmpList.size() == 4) {
					for (String constact : tmpList) {
						if (constact != null) {

							String user_info[] = constact.split("\t");
							if (user_info.length != 3) {
								continue;
							}
							// 用户、联系人姓名
							//String name = user_info[0];
							// 用户的号码，或者联系人号码
							String phone = user_info[1];
							// 用户、联系人标签
							//String tag = user_info[2];

							// 以本元素下个元素为起始刻度，开始遍历
							for (int j = i + 1; j < allSize; j++) {
								List<String> tmpSearchList = allUser.get(j);
								if (tmpSearchList.size() == 4) {
									// 每个用户、联系人的号码
									for (String search_constact : tmpSearchList) {
										if (search_constact != null) {
											String userSearch_info[] = search_constact
													.split("\t");
											if (userSearch_info.length != 3) {
												continue;
											}
											//String search_name = userSearch_info[0];
											String search_phone = userSearch_info[1];
											//String search_tag = userSearch_info[2];
											// 找到相同的元素后，将两个集合进行叠加
											if (phone.equals(search_phone)) {
												// 匹配上了
												flag = true;
												if (!userUnionList
														.contains(tmpSearchList)) {
													userUnionList
															.add(tmpSearchList);
												}
											} else {// 没有匹配上
												if (!userNoList
														.contains(tmpSearchList)) {
													userNoList
															.add(tmpSearchList);
												}
											}
										}
									}
								}
							}
						}
					}
				}
				if (flag == false) {
					userUnionList.clear();
				}
				// 第一次匹配完成之后
				if (userUnionList.size() != 0) {
					for (List<String> userUnion : userUnionList) {
						for (String union : userUnion) {
							System.err.println(union);
						}
					}
				}
			}
		}
	}

	/**
	 * 写文件
	 * 
	 * @param path
	 */
	public static void writeFileByLine(String path, String str) {
		BufferedWriter writer = null;
		File file = new File(path);
		try {
			if (!file.exists()) {
				file.createNewFile();
			}
			writer = new BufferedWriter(new FileWriter(file));
			writer.write(str);
			writer.newLine();// 换行
			writer.flush();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				writer.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
}