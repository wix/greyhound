package greyhound.sidecaruser

import com.wixpress.dst.greyhound.sidecar.api.v1.greyhoundsidecaruser.ZioGreyhoundsidecaruser.RCGreyhoundSidecarUser
import scalapb.zio_grpc.{ServerMain, ServiceList}

class TestServer(servicePort: Int, impl: RCGreyhoundSidecarUser) extends ServerMain {

  override def port: Int = servicePort

  override def services: ServiceList[Any] = ServiceList.add(impl)
}