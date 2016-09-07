package samples

import java.util.Properties
import scala.util.Properties
import kafka.javaapi.producer.Producer
import kafka.producer.KeyedMessage
import kafka.producer.KeyedMessage
import kafka.producer.ProducerConfig
import scala.util.Random
import com.cgtz.sparkstreaming.utils.PropertyUtils

object KafkaProduce {
  def main(args: Array[String]): Unit = {
    produce
  }
  def produce(): Unit = {
    val topic = PropertyUtils.loadProperties("kafka.topic")
    val props = new Properties()
    props.put("metadata.broker.list", PropertyUtils.loadProperties("kafka.broker"))
    props.put("serializer.class", "kafka.serializer.StringEncoder")
    props.put("request.required.acks", "1")
    //    props.put("partitioner.class", "samples.EventPartitioner");

    val kafkaConfig = new ProducerConfig(props)
    val producer = new Producer[String, String](kafkaConfig)

    for (i <- 1 to 100) {
      val key = i%3
      producer.send(new KeyedMessage[String, String](topic, key+"", "message" + i))
      println(key+"   "+"message" + i)
    }
  }
  def test(): Unit = {
    for (i <- 1 to 1000) {
      println(i%3)
    }
  }
}