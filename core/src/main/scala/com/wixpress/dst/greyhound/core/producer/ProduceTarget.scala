package com.wixpress.dst.greyhound.core.producer

import com.wixpress.dst.greyhound.core.Serializer

sealed trait ProduceTarget[+K]

object ProduceTarget {
  case object None extends ProduceTarget[Nothing]
  case class Partition(partition: Int) extends ProduceTarget[Nothing]
  case class Key[K](key: K, serializer: Serializer[K]) extends ProduceTarget[K]
}
