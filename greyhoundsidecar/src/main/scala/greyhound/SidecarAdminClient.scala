package greyhound

import com.wixpress.dst.greyhound.core.admin.{AdminClient, AdminClientConfig}

object SidecarAdminClient {

  def admin(kafkaAddress: String) = {
    println(s"~~~ Admin Kafka address is $kafkaAddress")
    AdminClient.make(AdminClientConfig(kafkaAddress))
  }
}
