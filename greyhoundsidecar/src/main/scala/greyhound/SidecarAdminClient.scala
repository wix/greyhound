package greyhound

import com.wixpress.dst.greyhound.core.admin.{AdminClient, AdminClientConfig}
import zio.{Scope, ZIO}

object SidecarAdminClient {

  def admin(kafkaAddress: String): ZIO[Scope with Any, Throwable, AdminClient] =
    AdminClient.make(AdminClientConfig(kafkaAddress))
}
