package drone_prestacop
import java.util.UUID.randomUUID
import java.time.Instant

import scala.util.Random

class Drone {
  var  r = new Random();
  var latitude:Float = 40 + r.nextFloat()
  var longitude:Float = -74 + r.nextFloat()
  var id:String = randomUUID().toString
  var timestamp:Long = Instant.now.getEpochSecond

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

  def send: Unit = {
    if (Random.between(0, 100) > 95) {
      val (message, image_id, image) = generate_violation
      print(message)
      print(image_id)
      print(image)
    }
    println(latitude)
    println(longitude)
    println(id)
    println(timestamp)


  }

}


