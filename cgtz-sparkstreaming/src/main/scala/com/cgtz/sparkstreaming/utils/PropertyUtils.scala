package com.cgtz.sparkstreaming.utils

import java.util.Properties
import java.io.FileInputStream
/**
 * 读取配置文件
 */
object PropertyUtils {
  def loadProperties(key: String): String = {
    val properties = new Properties()
    //文件要放到resource文件夹下
    val path = Thread.currentThread().getContextClassLoader.getResource("system.properties").getPath
    properties.load(new FileInputStream(path))
    properties.getProperty(key)
  }
}