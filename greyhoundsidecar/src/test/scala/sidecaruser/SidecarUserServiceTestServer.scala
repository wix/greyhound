package sidecaruser

import com.wixpress.dst.greyhound.sidecar.api.v1.greyhoundsidecaruser.ZioGreyhoundsidecaruser.RGreyhoundSidecarUser
import scalapb.zio_grpc.{ServerMain, ServiceList}

class SidecarUserServiceTestServer(servicePort: Int, impl: RGreyhoundSidecarUser[Any]) extends ServerMain {

  override def port: Int = servicePort

  override def services: ServiceList[Any] = ServiceList.add(impl)
}