package com.wixpress.dst.greyhound.core.producer

import com.wixpress.dst.greyhound.core._

case class ProducerRecord[+K, +V](topic: Topic,
                                  value: V,
                                  key: Option[K] = None,
                                  partition: Option[Partition] = None,
                                  headers: Headers = Headers.Empty)
