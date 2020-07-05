package prestacop_drone

import java.util.UUID.randomUUID
import java.time.Instant
import play.api.libs.json._
import com.amazonaws.ClientConfiguration
import com.amazonaws.auth.{AWSStaticCredentialsProvider, BasicSessionCredentials, InstanceProfileCredentialsProvider}
import com.amazonaws.regions.{Region, Regions}


import scala.util.Random

import com.amazonaws.services.kinesis.AmazonKinesisClientBuilder

import com.amazonaws.services.kinesis.model.PutRecordsResult
import java.nio.ByteBuffer
import java.util
import com.amazonaws.services.kinesis.model.PutRecordsRequest
import com.amazonaws.services.kinesis.model.PutRecordsRequestEntry

class Drone {


  var r = new Random();
  var latitude: Float = 40 + r.nextFloat()
  var longitude: Float = -74 + r.nextFloat()
  var id: String = randomUUID().toString
  var timestamp: Long = Instant.now.getEpochSecond


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
  putRecordsRequest.setStreamName("message-prestacop")


  def update: Unit = {
    val lat_rand = Random.between(-1, 1)
    val long_rand = Random.between(-1, 1)
    latitude = latitude + (r.nextFloat() * lat_rand)
    longitude = longitude + (r.nextFloat() * long_rand)
    timestamp = Instant.now.getEpochSecond

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

  def put_json_kinesis(jsonData: JsValue) = {
    val putRecordsRequestEntryList = new util.ArrayList[PutRecordsRequestEntry]

    val jsonFinal = Json.toJson(jsonData)
    println(jsonFinal)

    val putRecordsRequestEntry = new PutRecordsRequestEntry
    putRecordsRequestEntry.setData(ByteBuffer.wrap(jsonFinal.toString().getBytes))
    putRecordsRequestEntry.setPartitionKey(String.format("partitionKey-%d", 1))
    putRecordsRequestEntryList.add(putRecordsRequestEntry)


    putRecordsRequest.setRecords(putRecordsRequestEntryList)
    val putRecordsResult = kinesisClient.putRecords(putRecordsRequest)
    System.out.println("Put Result" + putRecordsResult)
  }

  def send_violation: Unit = {

    val (message, image_id, image) = generate_violation
    val json: JsValue = Json.obj(
      "droneId" -> id,
      "long" -> longitude,
      "lat" -> latitude,
      "image" -> image,
      "message" -> message.toString,
      "timestamp" -> timestamp,
    )
    put_json_kinesis(json)
  }


  def send: Unit = {
    val json: JsValue = Json.obj(
      "droneId" -> id,
      "long" -> longitude,
      "lat" -> latitude,
      "timestamp" -> timestamp,
    )
    put_json_kinesis(json)
  }
}
