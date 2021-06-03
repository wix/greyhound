package com.wixpress.dst.greyhound.core.consumer.retry

import com.wixpress.dst.greyhound.core.consumer.domain.ConsumerRecord
import com.wixpress.dst.greyhound.core.consumer.retry.BlockingState.{Blocked, IgnoringAll, IgnoringOnce, Blocking => InternalBlocking}
import com.wixpress.dst.greyhound.core.consumer.retry.RetryConsumerRecordHandlerTest.{bytes, offset, partition, randomTopicName}
import com.wixpress.dst.greyhound.core.metrics.GreyhoundMetrics
import com.wixpress.dst.greyhound.core.testkit.{BaseTest, TestMetrics}
import com.wixpress.dst.greyhound.core.{Headers, TopicPartition}
import org.specs2.specification.core.Fragment
import zio.test.environment.TestEnvironment
import zio.{Ref, UIO, test}

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

        record = ConsumerRecord(topic, partition, offset, headers, Some(key), value, 0L, 0L, 0L)
        shouldBlock <- resolver.resolve(record)
        updatedStateMap <- blockingState.get
        updatedState = updatedStateMap(TopicPartitionTarget(tpartition))
      } yield shouldBlock === true and updatedState === Blocked(record)
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

        record = ConsumerRecord(topic, partition, offset, headers, Some(key), value, 0L, 0L, 0L)
        shouldBlock <- resolver.resolve(record)
        updatedStateMap <- blockingState.get
        updatedState = updatedStateMap(TopicPartitionTarget(tpartition))
      } yield shouldBlock === true and updatedState === Blocked(record)
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
      (IgnoringAll, InternalBlocking, true), (IgnoringAll, IgnoringAll, false), (IgnoringAll, IgnoringOnce, false),
      (IgnoringOnce, InternalBlocking, true), (IgnoringOnce, IgnoringAll, false), (IgnoringOnce, IgnoringOnce, false),
        (InternalBlocking, BlockedMessageState, true), (IgnoringAll, BlockedMessageState, true),
      (IgnoringOnce, BlockedMessageState, true))) { pair =>
      val (topicTargetState, topicPartitionTargetState, expectedShouldBlock) = pair
      s"prefer the state of TopicPartitionTarget when it differs from TopicTarget " +
        s"because whenever TopicTarget is set it also sets TopicPartitionTarget ($topicTargetState,$topicPartitionTargetState)" in {
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

    "when setting blocking state for topicTarget, also set it to related topicPartitions targets transitively, but not to others" in {
      for {
        topic <- randomTopicName
        tpartition = TopicPartition(topic, partition)
        anotherTopic <- randomTopicName
        anotherTPartition =  TopicPartition(anotherTopic, partition)
        blockingState <- Ref.make[Map[BlockingTarget, BlockingState]](Map(
          TopicPartitionTarget(tpartition) -> IgnoringAll,
          TopicPartitionTarget(anotherTPartition) -> IgnoringOnce))
        key = "some-key"
        value = Foo("some-value")
        headers = Headers.from("header-key" -> "header-value")

        resolver = BlockingStateResolver(blockingState)

        record = ConsumerRecord(topic, partition, offset, headers, Some(key), value, 0L, 0L, 0L)
        record2 = ConsumerRecord(anotherTopic, partition, offset, headers, Some(key), value, 0L, 0L, 0L)
        shouldBlockBefore <- resolver.resolve(record)
        shouldBlockBefore2 <- resolver.resolve(record2)
        _ <- resolver.setBlockingState(BlockErrors(topic))
        shouldBlockAfter <- resolver.resolve(record)
        shouldBlockAfter2 <- resolver.resolve(record2)

        updatedStateMap <- blockingState.get
        updatedState = updatedStateMap(TopicPartitionTarget(tpartition))
        updatedState2 = updatedStateMap(TopicPartitionTarget(anotherTPartition))
      } yield (shouldBlockBefore aka "shouldBlockBefore" mustEqual(false)) and
        (shouldBlockAfter aka "shouldBlockAfter" mustEqual(true)) and
        (shouldBlockBefore2 aka "shouldBlockBefore2" mustEqual(false)) and
        (shouldBlockAfter2 aka "shouldBlockAfter2" mustEqual(false)) and
        (updatedState === Blocked(record)) and
        (updatedState2 === IgnoringOnce)
    }

    "fail ignoring once when the previous state was not blocked" in {
      for {
        topic <- randomTopicName
        tpartition = TopicPartition(topic, partition)
        anotherTopic <- randomTopicName
        anotherTPartition =  TopicPartition(anotherTopic, partition)
        blockingState <- Ref.make[Map[BlockingTarget, BlockingState]](Map(TopicPartitionTarget(tpartition) -> InternalBlocking))

        resolver = BlockingStateResolver(blockingState)
        failed1 <- resolver.setBlockingState(IgnoreOnceFor(tpartition)).as(false).catchAll(_ => UIO(true))
        failed2 <- resolver.setBlockingState(IgnoreOnceFor(anotherTPartition)).as(false).catchAll(_ => UIO(true))

        updatedStateMap <- blockingState.get
        updatedState1 = updatedStateMap(TopicPartitionTarget(tpartition))
        updatedState2 = updatedStateMap.getOrElse(TopicPartitionTarget(anotherTPartition), InternalBlocking)
      } yield failed1 === false and
        updatedState1 === IgnoringOnce and
        failed2 === true and
        updatedState2 === InternalBlocking
    }

    "Update state to Blocked when Blocking is set for TopicPartitionTarget while IgnoreAll is set for TopicTarget" in {
      for {
        topic <- randomTopicName
        tpartition = TopicPartition(topic, partition)
        blockingState <- Ref.make[Map[BlockingTarget, BlockingState]](Map(
          TopicTarget(topic) -> IgnoringAll,
          TopicPartitionTarget(tpartition) -> InternalBlocking))
        key <- bytes
        value <- bytes

        resolver = BlockingStateResolver(blockingState)

        record = ConsumerRecord(topic, partition, offset, Headers.Empty, Some(key), value, 0L, 0L, 0L)
        shouldBlock <- resolver.resolve(record)
        updatedStateMap <- blockingState.get
        updatedStateTopic = updatedStateMap(TopicTarget(topic))
        updatedStateTopicPartition = updatedStateMap(TopicPartitionTarget(tpartition))
      } yield shouldBlock === true and
        updatedStateTopic === IgnoringAll and
        updatedStateTopicPartition === Blocked(record)
    }
  }

  final val BlockedMessageState = Blocked(ConsumerRecord("", 0, 0, Headers.Empty, None, "", 0L, 0L, 0L))
}

case class Foo(message: String)