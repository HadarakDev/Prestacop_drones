package drone_prestacop

import scala.util.Random

class Drone {
  var latitude:Float = Random.between(20, 30)
  var longitude:Float = Random.between(20, 30)
  var id:Integer = Random.between(20, 30)
  var timestamp:Integer = Random.between(20, 30)

  def update: Unit = {
      //update les datas
  }

  def send: Unit = {
    println(latitude)
    println(longitude)
    println(id)
    println(timestamp)



  }

}


