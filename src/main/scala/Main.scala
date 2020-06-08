import drone_prestacop.Drone

object Main {
  def main(args: Array[String]): Unit = {

    val drones: List[Drone] = List.fill(10)(new Drone)
    while (true) {
      for (d <- drones) {
        d.update
        d.send_violation
        d.send
      }
    }
  }
}
