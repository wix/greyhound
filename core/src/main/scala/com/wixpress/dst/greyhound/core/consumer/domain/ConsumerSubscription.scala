package com.wixpress.dst.greyhound.core.consumer.domain

import java.util.regex.Pattern

import com.wixpress.dst.greyhound.core.{NonEmptySet, Topic}

object ConsumerSubscription {

  case class TopicPattern(p: Pattern, discoveredTopics: Set[Topic] = Set.empty) extends ConsumerSubscription

  case class Topics(topics: NonEmptySet[Topic]) extends ConsumerSubscription

}

sealed trait ConsumerSubscription