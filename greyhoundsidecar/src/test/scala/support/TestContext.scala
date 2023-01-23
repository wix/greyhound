package support

import com.wixpress.dst.greyhound.sidecar.api.v1.greyhoundsidecar.ProduceRequest.Target

import scala.util.Random.{nextInt, nextString}
case class TestContext(topicName: String,
                       payload: Option[String],
                       topicKey: Option[String],
                       consumerId: String,
                       partition: Option[Int],
                       group: String) {

  def target = topicKey
    .map(Target.Key)
    .getOrElse(Target.Empty)
}

object TestContext {

  def random: TestContext = TestContext(
    topicName = s"topic-$nextInt",
    payload = Some(nextString(10)),
    topicKey = Some(nextString(5)),
    consumerId = nextString(10),
    partition = Some(1),
    group = nextString(10))

}