package greyhound

import com.wixpress.dst.greyhound.core.admin.{AdminClient, AdminClientConfig}

object SidecarAdminClient {

  def admin(kafkaAddress: String) =
    AdminClient.make(AdminClientConfig(kafkaAddress))
}
