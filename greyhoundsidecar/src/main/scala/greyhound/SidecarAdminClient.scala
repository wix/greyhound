package greyhound

import com.wixpress.dst.greyhound.core.admin.{AdminClient, AdminClientConfig}

object SidecarAdminClient {

  val admin = AdminClient.make(AdminClientConfig(Produce.bootstrapServer))
}
