package com.wixpress.dst.greyhound.core.compression

import com.wixpress.dst.greyhound.core.testkit.BaseTestNoEnv

class GzipCompressionTest extends BaseTestNoEnv {
  "GZIPCompressor" should {
    "return None for bad input" in {
      GzipCompression.decompress("not a gzip".toCharArray.map(_.toByte)) must beNone
    }
  }
}
