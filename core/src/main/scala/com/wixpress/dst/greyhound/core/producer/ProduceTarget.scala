package com.wixpress.dst.greyhound.core.producer

sealed trait ProduceTarget[+K]

object ProduceTarget {
  case object None extends ProduceTarget[Nothing]
  case class Partition(partition: Int) extends ProduceTarget[Nothing]
  case class Key[K](key: K) extends ProduceTarget[K]
}
