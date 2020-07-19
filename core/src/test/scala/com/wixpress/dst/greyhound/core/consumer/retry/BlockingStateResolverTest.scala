package com.wixpress.dst.greyhound.core.consumer.retry

import com.wixpress.dst.greyhound.core.Headers
import com.wixpress.dst.greyhound.core.consumer.retry.RecordHandlerTest.{bytes, offset, partition, randomTopicName}
import com.wixpress.dst.greyhound.core.consumer.domain.{ConsumerRecord, TopicPartition}
import com.wixpress.dst.greyhound.core.consumer.retry.BlockingState.{Blocked, IgnoringAll, IgnoringOnce, Blocking => InternalBlocking}
import com.wixpress.dst.greyhound.core.metrics.GreyhoundMetrics
import com.wixpress.dst.greyhound.core.testkit.{BaseTest, TestMetrics}
import org.specs2.specification.core.Fragment
import zio.test.environment.TestEnvironment
import zio.{Ref, test}

class BlockingStateResolverTest  extends BaseTest[TestEnvironment with GreyhoundMetrics] {

  override def env =
    for {
      env <- test.environment.testEnvironment.build
      testMetrics <- TestMetrics.make
    } yield env ++ testMetrics

  "BlockingStateResolver" should {
    Fragment.foreach(Seq(
      (true, InternalBlocking),
      (true, BlockedMessageState),
      (false, IgnoringAll),
      (false, IgnoringOnce))) { pair =>
      val (expectedShouldBlock, state) = pair
      s"return '${expectedShouldBlock}' if state is '${state}' for TopicPartition target" in {
        for {
          topic <- randomTopicName
          blockingState <- Ref.make[Map[BlockingTarget, BlockingState]](Map.empty)
          key <- bytes
          value <- bytes

          resolver = BlockingStateResolver(blockingState)
          _ <- blockingState.set(Map(TopicPartitionTarget(TopicPartition(topic, partition)) -> state))

          shouldBlock <- resolver.resolve(ConsumerRecord(topic, partition, offset, Headers.Empty, Some(key), value, 0L, 0L, 0L))
        } yield shouldBlock === expectedShouldBlock
      }
    }

    Fragment.foreach(Seq((true, InternalBlocking), (false, IgnoringAll), (false, IgnoringOnce))) { pair =>
      val (expectedShouldBlock, state) = pair
      s"return '${expectedShouldBlock}' if state is '${state}' for Topic target" in {
        for {
          topic <- randomTopicName
          blockingState <- Ref.make[Map[BlockingTarget, BlockingState]](Map.empty)
          key <- bytes
          value <- bytes

          resolver = BlockingStateResolver(blockingState)
          _ <- blockingState.set(Map(TopicTarget(topic) -> state))

          shouldBlock <- resolver.resolve(ConsumerRecord(topic, partition, offset, Headers.Empty, Some(key), value, 0L, 0L, 0L))
        } yield shouldBlock === expectedShouldBlock
      }
    }

    "return true when state is missing - default is Blocking " in {
      for {
        missingTopic <- randomTopicName
        blockingState <- Ref.make[Map[BlockingTarget, BlockingState]](Map.empty)
        key <- bytes
        value <- bytes

        resolver = BlockingStateResolver(blockingState)

        shouldBlock <- resolver.resolve(ConsumerRecord(missingTopic, partition, offset, Headers.Empty, Some(key), value, 0L, 0L, 0L))
      } yield shouldBlock === true
    }

    "switch state to Blocked(message) when previous state was empty for TopicPartitionTarget" in {
      for {
        topic <- randomTopicName
        tpartition = TopicPartition(topic, partition)
        blockingState <- Ref.make[Map[BlockingTarget, BlockingState]](Map.empty)
        key = "some-key"
        value = Foo("some-value")
        headers = Headers.from("header-key" -> "header-value")

        resolver = BlockingStateResolver(blockingState)

        shouldBlock <- resolver.resolve(ConsumerRecord(topic, partition, offset, headers, Some(key), value, 0L, 0L, 0L))
        updatedStateMap <- blockingState.get
        updatedState = updatedStateMap(TopicPartitionTarget(tpartition))
      } yield shouldBlock === true and updatedState === Blocked(Some(key), value, headers, tpartition, offset)
    }

    "switch state to Blocked(message) when previous state was Blocking for TopicPartitionTarget" in {
      for {
        topic <- randomTopicName
        tpartition = TopicPartition(topic, partition)
        blockingState <- Ref.make[Map[BlockingTarget, BlockingState]](Map(TopicPartitionTarget(tpartition) -> InternalBlocking))
        key = "some-key"
        value = Foo("some-value")
        headers = Headers.from("header-key" -> "header-value")

        resolver = BlockingStateResolver(blockingState)

        shouldBlock <- resolver.resolve(ConsumerRecord(topic, partition, offset, headers, Some(key), value, 0L, 0L, 0L))
        updatedStateMap <- blockingState.get
        updatedState = updatedStateMap(TopicPartitionTarget(tpartition))
      } yield shouldBlock === true and updatedState === Blocked(Some(key), value, headers, tpartition, offset)
    }

    "Keep 'Blocking' state when previous state was Blocking for TopicTarget" in {
      for {
        topic <- randomTopicName
        blockingState <- Ref.make[Map[BlockingTarget, BlockingState]](Map(TopicTarget(topic) -> InternalBlocking))
        key <- bytes
        value <- bytes

        resolver = BlockingStateResolver(blockingState)

        shouldBlock <- resolver.resolve(ConsumerRecord(topic, partition, offset, Headers.Empty, Some(key), value, 0L, 0L, 0L))
        updatedStateMap <- blockingState.get
        updatedState = updatedStateMap(TopicTarget(topic))
      } yield shouldBlock === true and updatedState === InternalBlocking
    }

    Fragment.foreach(Seq((InternalBlocking, InternalBlocking, true), (InternalBlocking, IgnoringAll, false), (InternalBlocking, IgnoringOnce, false),
      (IgnoringAll, InternalBlocking, false), (IgnoringAll, IgnoringAll, false), (IgnoringAll, IgnoringOnce, false),
      (IgnoringOnce, InternalBlocking, false), (IgnoringOnce, IgnoringAll, false), (IgnoringOnce, IgnoringOnce, false),
        (InternalBlocking, BlockedMessageState, true), (IgnoringAll, BlockedMessageState, false),
      (IgnoringOnce, BlockedMessageState, false))) { pair =>
      val (topicTargetState, topicPartitionTargetState, expectedShouldBlock) = pair
      s"prefer to return false when both targets are available and one is Ignoring ($topicTargetState,$topicPartitionTargetState)" in {
        for {
          topic <- randomTopicName
          blockingState <- Ref.make[Map[BlockingTarget, BlockingState]](Map.empty)
          key <- bytes
          value <- bytes

          resolver = BlockingStateResolver(blockingState)
          _ <- blockingState.set(Map(TopicTarget(topic) -> topicTargetState, TopicPartitionTarget(TopicPartition(topic, partition)) -> topicPartitionTargetState))

          shouldBlock <- resolver.resolve(ConsumerRecord(topic, partition, offset, Headers.Empty, Some(key), value, 0L, 0L, 0L))
        } yield shouldBlock === expectedShouldBlock
      }
    }
  }

  final val BlockedMessageState = Blocked(None, "", Headers.Empty, TopicPartition("", 0), 0)
}

case class Foo(message: String)