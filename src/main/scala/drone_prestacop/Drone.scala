package drone_prestacop
import java.util.UUID.randomUUID
import java.time.Instant
import java.util
import java.util.Properties

import com.amazonaws.ClientConfiguration
import com.amazonaws.auth.{AWSSessionCredentials, AWSStaticCredentialsProvider, BasicSessionCredentials, InstanceProfileCredentialsProvider}
import com.amazonaws.regions.{Region, Regions}
import com.amazonaws.util.json
import com.google.gson.Gson

import scala.collection.immutable.HashMap

//import org.apache.kafka.clients.producer._
//import org.json4s.jackson.Json

import scala.util.Random
//import org.json4s.native.Json
//import org.json4s.DefaultFormats
import com.amazonaws.services.kinesis.AmazonKinesisClientBuilder

import com.amazonaws.services.kinesis.model.PutRecordsResult
import java.nio.ByteBuffer
import java.util
import scala.collection.JavaConverters._
import com.amazonaws.services.kinesis.model.PutRecordsRequest
import com.amazonaws.services.kinesis.model.PutRecordsRequestEntry





class Drone {
  var  r = new Random();
  var latitude:Float = 40 + r.nextFloat()
  var longitude:Float = -74 + r.nextFloat()
  var id:String = randomUUID().toString
  var timestamp:Long = Instant.now.getEpochSecond

  import com.amazonaws.auth.BasicAWSCredentials
  val aws_access_key_id = "ASIAWQZQX3A3RWDMFIOH"
  val aws_secret_access_key = "V5lG66hQw7GgXKwtHpq1rA0vZbAP4BWSrkeVX3le"
  val aws_session_token = "FwoGZXIvYXdzEK3//////////wEaDKqdsJJmwwFPIByWryK/AdQ5IvHlwOA1ACCtGEOiMEaEpow67L9TpfS22u4ucrc7pbwtg6ZNYuTuy9D5xcG2Oo2JPlGKYLafQHdM/P5mNUAfLXZQHrNnBDCzfRG2bMkirpWB2M6vABi/yBMwlFxs1FPZcnQXZKgqixouO1exWt5Qq7J55LgYbBBXBIs8qo7GM3SJq7HaI6uxx509LJEIDB+CQb6xTjCBQDw+wYrK/WJxAghXv7r2iv/hn231A9vFTKmg8B6eL0s+n/69CV2TKLCq7vcFMi1Ob8Tso70rcaoaN3ZoOEFOig4KyI2cVVAW+EeMBWa0z9RpVXEF7chX37rWFPw="
  val awsCreds =  new BasicSessionCredentials(aws_access_key_id,
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
  putRecordsRequest.setStreamName("message-prestacop")


  def update: Unit = {
    val lat_rand = Random.between(-1, 1)
    val long_rand = Random.between(-1, 1)
    latitude = latitude +  (r.nextFloat() * lat_rand)
    longitude = longitude + (r.nextFloat() * long_rand)
    timestamp = Instant.now.getEpochSecond
    send

  }

  def generate_violation(): (Integer, Integer, String) = {
    val message = Random.between(0, 100)
    val images: List[String] = List("iVBORw0KGgoAAAANSUhEUgAAABgAAAAYCAIAAABvFaqvAAAAO0lEQVR4nGLZ1FjJQAjcndxFUA0TQRVEglGDRg0angYxnn9wjaAigbefCKoZfF4bNWjUoEFlECAAAP//VjcITPSGjckAAAAASUVORK5CYII=",
                                    "iVBORw0KGgoAAAANSUhEUgAAABgAAAAYCAIAAABvFaqvAAAAO0lEQVR4nGIJDy5mIAS003wIqmEiqIJIMGrQqEHD0yDGZjFegooUTwUQVDP4vDZq0KhBg8ogQAAAAP//XzAEDQnaF84AAAAASUVORK5CYII=",
                                  "iVBORw0KGgoAAAANSUhEUgAAABgAAAAYCAIAAABvFaqvAAAAO0lEQVR4nGJJ0nrEQAhwWkYQVMNEUAWRYNSgUYOGp0Es95V8CSpKPBVDUM3g89qoQaMGDSqDAAEAAP//COYFEo893xAAAAAASUVORK5CYII=",
                                  "iVBORw0KGgoAAAANSUhEUgAAABgAAAAYCAIAAABvFaqvAAAAO0lEQVR4nGLhc0tnIAR2ePAQVMNEUAWRYNSgUYOGp0GMi8uvEFS04g4fQTWDz2ujBo0aNKgMAgQAAP//eXoFeWfvFsAAAAAASUVORK5CYII=",
                                  "iVBORw0KGgoAAAANSUhEUgAAABgAAAAYCAIAAABvFaqvAAAAO0lEQVR4nGLxviXOQAj0rr9IUA0TQRVEglGDRg0angYx7rp8lKCiGVZqBNUMPq+NGjRq0KAyCBAAAP//AX8GxSuiKjcAAAAASUVORK5CYII=",
                                  "iVBORw0KGgoAAAANSUhEUgAAABgAAAAYCAIAAABvFaqvAAAAO0lEQVR4nGKZf7eJgRDoLw4iqIaJoAoiwahBowYNT4MYV8laElTkFfqQoJrB57VRg0YNGlQGAQIAAP//k/UGBNb4hyEAAAAASUVORK5CYII=",
                                  "iVBORw0KGgoAAAANSUhEUgAAABgAAAAYCAIAAABvFaqvAAAAO0lEQVR4nGLREKtiIATqSvUJqmEiqIJIMGrQqEHD0yDGrClXCCpasLKRoJrB57VRg0YNGlQGAQIAAP//TwEFqDqe93kAAAAASUVORK5CYII=",
                                  "iVBORw0KGgoAAAANSUhEUgAAABgAAAAYCAIAAABvFaqvAAAAO0lEQVR4nGK5NrGagRA4fo2HoBomgiqIBKMGjRo0PA1i2R+iRVCRQ/svgmoGn9dGDRo1aFAZBAgAAP//vd0GvofKJmAAAAAASUVORK5CYII=",
                                  "iVBORw0KGgoAAAANSUhEUgAAABgAAAAYCAIAAABvFaqvAAAAO0lEQVR4nGIJK1vHQAgIZGwmqIaJoAoiwahBowYNT4MY115RIajoie1XgmoGn9dGDRo1aFAZBAgAAP//GLoGkqUaMxAAAAAASUVORK5CYII=",
                                  "iVBORw0KGgoAAAANSUhEUgAAABgAAAAYCAIAAABvFaqvAAAAO0lEQVR4nGJ5PWEWAyEQvcuBoBomgiqIBKMGjRo0PA1iCXzlSVDReqZbBNUMPq+NGjRq0KAyCBAAAP//KREGrr/OQCEAAAAASUVORK5CYII=",
                                  "iVBORw0KGgoAAAANSUhEUgAAABgAAAAYCAIAAABvFaqvAAAAO0lEQVR4nGLp+7yYgRBIcv1OUA0TQRVEglGDRg0angYxWl54S1DRiR1iBNUMPq+NGjRq0KAyCBAAAP//FpYHgLbOiicAAAAASUVORK5CYII=",
                                  "iVBORw0KGgoAAAANSUhEUgAAABgAAAAYCAIAAABvFaqvAAAAO0lEQVR4nGJRf6DNQAjc3ihIUA0TQRVEglGDRg0angYxvn5QTlCRqYwvQTWDz2ujBo0aNKgMAgQAAP//cocF4f3hEJUAAAAASUVORK5CYII=",
                                  "iVBORw0KGgoAAAANSUhEUgAAABgAAAAYCAIAAABvFaqvAAAAO0lEQVR4nGLJU5/FQAhU6lwlqIaJoAoiwahBowYNT4NYDgtPJ6iIL9yboJrB57VRg0YNGlQGAQIAAP//37YE+xgQuusAAAAASUVORK5CYII=",
                                  "iVBORw0KGgoAAAANSUhEUgAAABgAAAAYCAIAAABvFaqvAAAAO0lEQVR4nGKRNZjPQAhkTPxHUA0TQRVEglGDRg0angaxnHZ5T1DRwS+uBNUMPq+NGjRq0KAyCBAAAP//gjUHEPSMrfAAAAAASUVORK5CYII=",
                                  "iVBORw0KGgoAAAANSUhEUgAAABgAAAAYCAIAAABvFaqvAAAAO0lEQVR4nGLJkvnLQAjcXrGNoBomgiqIBKMGjRo0PA1iUXUvJKjoYrANQTWDz2ujBo0aNKgMAgQAAP//cigGLsfF7XMAAAAASUVORK5CYII=",
                                  "iVBORw0KGgoAAAANSUhEUgAAABgAAAAYCAIAAABvFaqvAAAAO0lEQVR4nGLZs8+cgRB4X19PUA0TQRVEglGDRg0angYxRitfIqjo8/JbBNUMPq+NGjRq0KAyCBAAAP//to4HlF8Qx7gAAAAASUVORK5CYII=",
                                  "iVBORw0KGgoAAAANSUhEUgAAABgAAAAYCAIAAABvFaqvAAAAO0lEQVR4nGLhiGVkIATWlkUQVMNEUAWRYNSgUYOGp0GM61YVEFR0/48LQTWDz2ujBo0aNKgMAgQAAP//aAMF+gxB8S8AAAAASUVORK5CYII=",
                                  "iVBORw0KGgoAAAANSUhEUgAAABgAAAAYCAIAAABvFaqvAAAAO0lEQVR4nGKp8b7DQAjs6VElqIaJoAoiwahBowYNT4MYz9iGElR04acrQTWDz2ujBo0aNKgMAgQAAP//4BkGrizO5GcAAAAASUVORK5CYII=")

    val image_id = Random.between(0, images.length)
    val image = images(image_id)
    (message, image_id, image)



  }

  def send_violation: Unit = {
    if (Random.between(0, 100) > 95) {
      val (message, image_id, image) = generate_violation
      //code envoie
    }
  }

  def send: Unit = {
//
//    println(latitude)
//    println(longitude)
//    println(id)
//    println(timestamp)


    val putRecordsRequestEntryList = new util.ArrayList[PutRecordsRequestEntry]

    val data = Map("long" -> longitude, "lat" -> latitude,"timestamp" -> timestamp)

    val gson = new Gson();
    val jsondata = gson.toJson(data.asJava + "\n");

    val putRecordsRequestEntry = new PutRecordsRequestEntry
    putRecordsRequestEntry.setData(ByteBuffer.wrap(jsondata.getBytes))
    putRecordsRequestEntry.setPartitionKey(String.format("partitionKey-%d", 1))
    putRecordsRequestEntryList.add(putRecordsRequestEntry)


    putRecordsRequest.setRecords(putRecordsRequestEntryList)
    val putRecordsResult = kinesisClient.putRecords(putRecordsRequest)
    System.out.println("Put Result" + putRecordsResult)

  }

}


