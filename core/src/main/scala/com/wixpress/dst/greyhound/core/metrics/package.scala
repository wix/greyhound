package com.wixpress.dst.greyhound.core

import zio.Has

package object metrics {
  type GreyhoundMetrics = Has[GreyhoundMetrics.Service]
}
