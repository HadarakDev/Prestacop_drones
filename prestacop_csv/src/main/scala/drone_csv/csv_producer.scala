package drone_csv

import java.nio.ByteBuffer
import java.util

import com.amazonaws.ClientConfiguration
import com.amazonaws.auth.{AWSStaticCredentialsProvider, BasicSessionCredentials}
import com.amazonaws.regions.Regions
import com.amazonaws.services.kinesis.{AmazonKinesis, AmazonKinesisClientBuilder}
import com.amazonaws.services.kinesis.model.{PutRecordsRequest, PutRecordsRequestEntry}
import play.api.libs.json
import play.api.libs.json.{JsObject, JsValue, Json}

import scala.io.Source

object csv_producer {
  def put_json_kinesis(jsonData:JsValue, kinesisClient:AmazonKinesis, putRecordsRequest:PutRecordsRequest) = {
    val putRecordsRequestEntryList = new util.ArrayList[PutRecordsRequestEntry]

    val putRecordsRequestEntry = new PutRecordsRequestEntry
    putRecordsRequestEntry.setData(ByteBuffer.wrap(jsonData.toString().getBytes))
    putRecordsRequestEntry.setPartitionKey(String.format("partitionKey-%d", 1))
    putRecordsRequestEntryList.add(putRecordsRequestEntry)


    putRecordsRequest.setRecords(putRecordsRequestEntryList)
    val putRecordsResult = kinesisClient.putRecords(putRecordsRequest)
    System.out.println("Put Result" + putRecordsResult)
  }

  def main(args: Array[String]): Unit = {
    val fileName = "./Parking_Violations_Issued_-_Fiscal_Year_2017.csv"

    val aws_access_key_id = "ASIAWQZQX3A3XYC2AQHX"
    val aws_secret_access_key = "mGvz/oOpG4ZksBmimKlS31+7WHC5yx1YK2qPD20F"
    val aws_session_token = "FwoGZXIvYXdzECEaDJCLLe29Rtc//jIkmCK/AZZCGgJx4BVhnrz9fCwwLyaV9ddW8KqJWB0Ra4O23T+M5tSAe0D7NvixEC65NCE3pMjr1lG0ShKefHcpsEc9Y1fN++pcDMtxxcYQQrg37n7kWvwrmaknWwy8i+zjw7ugYDpe6b1LXrYvn1daDnWrumtTrFt4O8WCV/4FrY+lcdIXyST/a1CCmB7s7yrcdFG2rgCZNtf8COVDc0yzRm/5YMpmj6jKXURRtB8CV6Jix3lby3jfBhWizvubi/c+2Y6aKM3zh/gFMi2E+LS18AutO6JoioBJvXAMlW1Or/TkhGbIczNbLoPnfs3+BgQ+LFkbkaptaw0="
    val awsCreds = new BasicSessionCredentials(aws_access_key_id,
      aws_secret_access_key,
      aws_session_token
    )

    val config = new ClientConfiguration()
    val kinesisClient = AmazonKinesisClientBuilder
      .standard
      .withCredentials(new AWSStaticCredentialsProvider(awsCreds))
      .withRegion(Regions.US_EAST_1)
      .withClientConfiguration(new ClientConfiguration())
      .build()


    val putRecordsRequest = new PutRecordsRequest
    putRecordsRequest.setStreamName("Prestacop-Kinesis-csv")
    val putRecordsRequestEntry = new PutRecordsRequestEntry

    val key = Source.fromFile(fileName).getLines.next()
      .replace(" ", "_")
      .replace("?", "").split(",")


    for (line <- Source.fromFile(fileName).getLines().drop(1)) { // Dropping the column names
      var line_fixed = line
      while (line_fixed.contains(",,")) {
        line_fixed = line_fixed.replace(",,", ",Null,")
          .replace("\\s", "")
          .replace("[", "")
          .replace(";", "")
          .replace("{", "")
          .replace("}", "")
          .replace("(", "")
          .replace(")", "")
          .replace("|", "")

      }
      val value = line_fixed.split(",")

      if (value.length == key.length) {
        var data = collection.mutable.Map[String, String]()
        for (i <- 0 until key.length) {
          data += (key(i).toString -> value(i).toString)
        }
        val jsonData: JsValue = Json.toJson(data)
        put_json_kinesis(jsonData, kinesisClient, putRecordsRequest)
      }
    }
  }
}
