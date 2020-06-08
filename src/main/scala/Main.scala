import drone_prestacop.Drone

object Main {
  def main(args: Array[String]): Unit = {

    val drones: List[Drone] = List.fill(10)(new Drone)
    for( d <- drones) {

      d.send
    }
  }
}
