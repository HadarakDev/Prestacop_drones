package prestacop_csv

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
  def put_json_kinesis(jsonData: JsValue, kinesisClient: AmazonKinesis, putRecordsRequest: PutRecordsRequest) = {
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
    val fileName = "../../Parking_Violations_Issued_-_Fiscal_Year_2017.csv"

    val aws_access_key_id = "ASIAWQZQX3A3UOMBILHT"
    val aws_secret_access_key = "0TRj0XXQPLT0HqyyO5J9KVivk7klrc0jTBVybgVu"
    val aws_session_token = "FwoGZXIvYXdzEK///////////wEaDNEg+Ak+VTaIrHvhbCK/AVFDhY1Y+CrjTE+HLpOGmbieIf5aarieylgEh4SSr/RyKpnKPJAgzFCz6YZcbH5IqyAD4kimuwV8t9eXjWoX01hGdXObnwfmmX86rh78cYvHmSk913q4oZ6p8tgWcFDJ+83d+mok0xvjXLIgGy8i2jQF571kVr6BmOVeHrnb1Jzs7+aPQ0MOpRU2kSL9ADXhjX7ZAa2RDG6VDQD3661KbKjcnNoSk7+mN3HrtTGoNbE/TS1hUO/eMuELd5SFjMhHKOSEp/gFMi1GDbibYxtxbj6N92i1M+wBk+Dij5c2tfp1G/3yBD78mIUucI5nywTWrdBLjLk="
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
      .replace("?", "")
      .replace("\\s", "")
      .replace("[", "")
      .replace(";", "")
      .replace("{", "")
      .replace("}", "")
      .replace("(", "")
      .replace(")", "")
      .replace("|", "")
      .split(",")


    for (line <- Source.fromFile(fileName).getLines().drop(1)) { // Dropping the column names
      var line_fixed = line
      while (line_fixed.contains(",,")) {
        line_fixed = line_fixed.replace(",,", ", ,")
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
