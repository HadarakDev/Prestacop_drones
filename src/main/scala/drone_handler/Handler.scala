package drone_handler
import java.util
import java.util.Properties
import java.util.concurrent.TimeUnit

import org.apache.kafka.clients.consumer.KafkaConsumer;
import scala.concurrent.duration._
import scala.collection.JavaConverters._

class Handler {


  def consumeFromKafka(topic: String) = {
    val props = new Properties()
    val timeout = Duration(100, MILLISECONDS)
    props.put("bootstrap.servers", "localhost:9092")
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("auto.offset.reset", "latest")
    props.put("group.id", "consumer-group")
    val consumer: KafkaConsumer[String, String] = new KafkaConsumer[String, String](props)
    consumer.subscribe(util.Arrays.asList(topic))
    while (true) {
      val record = consumer.poll(1000).asScala
      for (data <- record.iterator)
        println(data.value().getClass)

    }
  }
}
