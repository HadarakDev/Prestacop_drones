import drone_prestacop.Drone

import scala.util.Random

object Main {
  def main(args: Array[String]): Unit = {

    val drones: List[Drone] = List.fill(1)(new Drone)
    for (i <-  0 until 100000)
      {
      Thread.sleep(2000)
      for (d <- drones) {
        d.update
        if (Random.between(0, 100) >= 90) {
          d.send_violation
        }
        else {
          d.send
        }
      }
    }
  }
}
