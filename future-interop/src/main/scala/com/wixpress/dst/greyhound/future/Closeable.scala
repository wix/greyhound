package com.wixpress.dst.greyhound.future

import scala.concurrent.Future

trait Closeable {
  def shutdown: Future[Unit]
}
