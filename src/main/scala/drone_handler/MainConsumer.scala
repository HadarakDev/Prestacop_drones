package drone_handler

object MainConsumer {
  def main(args: Array[String]): Unit = {
    var handler = new Handler
    handler.consumeFromKafka("Prestacop")
  }

}
