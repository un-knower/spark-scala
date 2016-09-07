package samples

import kafka.producer.Partitioner
import kafka.utils.VerifiableProperties

class EventPartitioner(var props: VerifiableProperties) extends Partitioner {
  def partition(key: Any, numPartitions: Int): Int = {
    1
  }
}