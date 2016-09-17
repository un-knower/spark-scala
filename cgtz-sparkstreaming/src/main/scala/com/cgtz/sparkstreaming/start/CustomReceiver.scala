package com.cgtz.sparkstreaming.start

import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.storage.StorageLevel
import org.apache.spark.Logging
import java.net.Socket
import java.io.BufferedReader
import java.io.InputStreamReader

/**
 * 1：SparkStreaming虽然说已经支持了很多不同类型的数据来源。但是有时候可能我们的一些数据来源非常特殊 ，不是它天然默认支持的，这时候就要自定义Receiver。而自定义Receiver，一般都是基于网络的方式。因为你传数据的话，一般是从另外一个网络端口传过来，至于传的协议是另外一码事。
 * 2：从本质上来说，SparkStreaming中的所有Receiver，都是自定义的Receiver。所以你要想自定义一个Receiver，最最简单的方式，你就是看下已有的Receiver怎么去实现。
 * 具体步骤：http://spark.apache.org/docs/latest/streaming-custom-receivers.html
 */
class CustomReceiver(host: String, port: Int)
    extends Receiver[String](StorageLevel.MEMORY_AND_DISK_2) with Logging {
  def onStart() {
    new Thread("socket Receiver") {
      //开启线程调receiver()方法
      override def run() { receive() }
    }
  }

  def onStop() {
    // There is nothing much to do as the thread calling receive()
    // is designed to stop by itself if isStopped() returns false
  }
  /** Create a socket connection and receive data until receiver is stopped */
  private def receive() {
    var socket: Socket = null
    var userInput: String = null
    try {
      // Connect to host:port，Receiver的时候就连上Socket
      socket = new Socket(host, port)

      val reader = new BufferedReader(new InputStreamReader(socket.getInputStream(), "UTF-8"))

      userInput = reader.readLine()
      while (!isStopped() && userInput != null) {
        store(userInput)
        userInput = reader.readLine() //每读一行，存一次，一直循环
      }
      reader.close()
      socket.close()

      // Restart in an attempt to connect again when server is active again
      restart("Trying to connect again") //先stop，然后再start()
    } catch {
      case e: java.net.ConnectException =>
        // restart if could not connect to server
        restart("Error connecting to " + host + ":" + port, e)
      case t: Throwable =>
        // restart if there is any other error
        restart("Error receiving data", t)
    }
  }
  /**
   * 上面就已经自定义完了一个Receiver，下面就new出它的对象，传进去。因为返回的是Dstream，以前对Dstream怎么操作，继续怎么操作，这里先从flatMap开始。
   * // Assuming ssc is the StreamingContext
   * val customReceiverStream = ssc.receiverStream(new CustomReceiver(host, port))
   * val words = lines.flatMap(_.split(" "))
   */
}