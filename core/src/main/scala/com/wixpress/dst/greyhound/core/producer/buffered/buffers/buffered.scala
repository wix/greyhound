package com.wixpress.dst.greyhound.core.producer.buffered.buffers

package object buffers {
  type PersistedMessageId = Long

  object PersistedMessageId {
    val notPersisted = -1
  }
}
