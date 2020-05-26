package com.wixpress.dst.greyhound.core

import com.wixpress.dst.greyhound.core.metrics.GreyhoundMetrics
import zio.Has

package object testkit {
  type TestMetrics = Has[TestMetrics.Service] with GreyhoundMetrics
}
