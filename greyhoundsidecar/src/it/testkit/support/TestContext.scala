package support

case class TestContext(topicName: String, payload: Option[String], topicKey: Option[String])

object TestContext {

  import scala.util.Random.{nextInt, nextString}

  def random: TestContext = TestContext(topicName = s"topic-$nextInt", Option(nextString(10)), Option(nextString(5)))
}