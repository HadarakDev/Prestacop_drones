package csv_to_kafka

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import scala.io.Source

object csv_producer {
  def main(args: Array[String]): Unit = {
    val props = new Properties()

    props.put("bootstrap.servers", "localhost:9092")
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    val fileName = "./Parking_Violations_Issued_-_Fiscal_Year_2017.csv"
    val topicName = "NYPD-Csv"
    val producer = new KafkaProducer[String, String](props)
    for (line <- Source.fromFile(fileName).getLines().drop(1)) { // Dropping the column names
      // Extract Key
      val key = line.split(","){0}
      val value = line.split(",").drop(1)


      // Prepare the record to send
      val record: ProducerRecord[String, String] = new ProducerRecord[String, String](topicName, key, value.mkString(","))

      // Send to topic
      Thread.sleep(500)
      producer.send(record)
    }
    producer.close()

  }
}
