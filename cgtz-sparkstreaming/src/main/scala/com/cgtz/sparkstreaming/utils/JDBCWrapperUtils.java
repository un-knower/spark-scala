package com.cgtz.sparkstreaming.utils;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;

public class JDBCWrapperUtils {
	private static JDBCWrapperUtils jdbcInstance = null;
	private static LinkedBlockingQueue<Connection> dbConnectionPool = new LinkedBlockingQueue<Connection>();
	// 底层访问mysql，首先通过静态代码块来加载驱动，这里先硬编码，实际肯定结合架构设计做配置字符串
	static {
		try {
			Class.forName("com.mysql.jdbc.Driver");
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	// 有了驱动，获取数据库的句柄，基于句柄做事情，下面是个单例
	public static JDBCWrapperUtils getJDBCInstance() {
		if (jdbcInstance == null) {
			// 同时三个线程来请求，如果不用同步synchronized，就会3个位null，弄出3个实例。
			synchronized (JDBCWrapperUtils.class) {
				if (jdbcInstance == null) {
					jdbcInstance = new JDBCWrapperUtils();
				}
			}
		}
		return jdbcInstance;
	}

	private JDBCWrapperUtils() {
		for (int i = 0; i < 10; i++) {
			// 这里创建10个connection
			try {
				Connection conn = DriverManager.
						getConnection("jdbc:mysql://Master:3306/sparkstreaming", 
						"root",
						"root");
				dbConnectionPool.put(conn);// 数据库更高效，使用连接池
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

	// 从池子中拿conn，考虑线程问题，加synchronized，让一个线程获得一个实例
	public synchronized Connection getConnection() {
		// 池子里面可能没东西，就死循环，确保下面能从池子里拿到实例
		while (0 == dbConnectionPool.size()) {
			try {
				Thread.sleep(20);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		// 拿到实例后，拿实例进行操作
		return dbConnectionPool.poll();
	}

	// 拿到实例，做查询，插入，更新等操作，
	// 而且是批量处理，不是一条条处理。
	// 第1个参数是你的sql语句，第2个参数是一批参数，因为批量，返回数组
	public int[] doBatch(String sqlText, 
			List<Object[]> paramsList, 
			ExecuteCallBack callBack) {
		Connection conn = getConnection();
		PreparedStatement preparedStatement = null;
		int[] result = null;
		try {
			// 设置批量操作不能自动提交
			conn.setAutoCommit(false);
			// 写入statement，传入sql
			preparedStatement = conn.prepareStatement(sqlText);
			for (Object[] parameters : paramsList) {
				for (int i = 0; i < parameters.length; i++) {
					// 这里索引必须加1
					preparedStatement.setObject(i + 1, parameters[i]);
				}
				preparedStatement.addBatch();
			}
			// 针对处理的结果，一般会有回调函数，要写个接口
			result = preparedStatement.executeBatch();
			conn.commit();// 这里真正执行
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			if (preparedStatement != null) {
				try {
					preparedStatement.close();// 要关闭prepareStatement
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			if (conn != null) {
				try {
					dbConnectionPool.put(conn);// 弄过之后，把conn放回池子中
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
		return result;
	}

	public void doQuery(String sqlText, Object[] paramsList,
			ExecuteCallBack callBack) {
		Connection conn = getConnection();
		PreparedStatement preparedStatement = null;
		ResultSet result = null;
		try {
			preparedStatement = conn.prepareStatement(sqlText);
			for (int i = 0; i < paramsList.length; i++) {
				preparedStatement.setObject(i + 1, paramsList[i]);
			}
			result = preparedStatement.executeQuery();
			callBack.resultCallBack(result);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			if (preparedStatement != null) {
				try {
					preparedStatement.close();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			if (conn != null) {
				try {
					dbConnectionPool.put(conn);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
	}
}
// 针对处理的结果，一般会有回调函数，要写个接口，调用回调函数，把result传过去
interface ExecuteCallBack {
	void resultCallBack(ResultSet result) throws Exception;
}
