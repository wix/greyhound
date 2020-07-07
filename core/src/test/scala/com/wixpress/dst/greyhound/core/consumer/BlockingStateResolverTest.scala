package com.wixpress.dst.greyhound.core.consumer

import com.wixpress.dst.greyhound.core.Headers
import com.wixpress.dst.greyhound.core.consumer.BlockingState.{Blocking => InternalBlocking, IgnoringAll, IgnoringOnce}
import com.wixpress.dst.greyhound.core.consumer.RecordHandlerTest.{bytes, offset, partition, randomTopicName}
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
    Fragment.foreach(Seq((true, InternalBlocking), (false, IgnoringAll), (false, IgnoringOnce))) { pair =>
      val (expectedShouldBlock, state) = pair
      s"return '${expectedShouldBlock}' if state is '${state}' for TopicPartition target" in {
        for {
          topic <- randomTopicName
          blockingState <- Ref.make[Map[BlockingTarget, BlockingState]](Map.empty)
          key <- bytes
          value <- bytes

          resolver = BlockingStateResolver(blockingState)
          _ <- blockingState.set(Map(TopicPartitionTarget(TopicPartition(topic, partition)) -> state))

          shouldBlock <- resolver.shouldBlock(ConsumerRecord(topic, partition, offset, Headers.Empty, Some(key), value, 0L, 0L, 0L))
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

          shouldBlock <- resolver.shouldBlock(ConsumerRecord(topic, partition, offset, Headers.Empty, Some(key), value, 0L, 0L, 0L))
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

        shouldBlock <- resolver.shouldBlock(ConsumerRecord(missingTopic, partition, offset, Headers.Empty, Some(key), value, 0L, 0L, 0L))
      } yield shouldBlock === true
    }

    Fragment.foreach(Seq((InternalBlocking, InternalBlocking, true), (InternalBlocking, IgnoringAll, false), (InternalBlocking, IgnoringOnce, false),
      (IgnoringAll, InternalBlocking, false), (IgnoringAll, IgnoringAll, false), (IgnoringAll, IgnoringOnce, false),
      (IgnoringOnce, InternalBlocking, false), (IgnoringOnce, IgnoringAll, false), (IgnoringOnce, IgnoringOnce, false))) { pair =>
      val (topicTargetState, topicPartitionTargetState, expectedShouldBlock) = pair
      s"prefer to return false when both targets are available and one is Ignoring ($topicTargetState,$topicPartitionTargetState)" in {
        for {
          topic <- randomTopicName
          blockingState <- Ref.make[Map[BlockingTarget, BlockingState]](Map.empty)
          key <- bytes
          value <- bytes

          resolver = BlockingStateResolver(blockingState)
          _ <- blockingState.set(Map(TopicTarget(topic) -> topicTargetState, TopicPartitionTarget(TopicPartition(topic, partition)) -> topicPartitionTargetState))

          shouldBlock <- resolver.shouldBlock(ConsumerRecord(topic, partition, offset, Headers.Empty, Some(key), value, 0L, 0L, 0L))
        } yield shouldBlock === expectedShouldBlock
      }
    }
  }
}
