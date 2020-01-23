package com.wixpress.dst.greyhound.core.consumer

import com.wixpress.dst.greyhound.core.Offset
import zio.{Promise, Ref, UIO, ZIO}

trait Offsets {
  def committable: UIO[Map[TopicPartition, Offset]]

  def update(topicPartition: TopicPartition, offset: Offset): UIO[Unit]

  def waitFor(offsets: Map[TopicPartition, Offset]): UIO[Unit]

  def update(record: ConsumerRecord[_, _]): UIO[Unit] =
    update(TopicPartition(record), record.offset + 1)
}

object Offsets {
  def make: UIO[Offsets] =
    Ref.make(State.Empty).map { state =>
      new Offsets {
        override def committable: UIO[Map[TopicPartition, Offset]] =
          state.modify(_.commit)

        override def update(topicPartition: TopicPartition, offset: Offset): UIO[Unit] =
          state.modify(_.update(topicPartition, offset)).flatten

        override def waitFor(offsets: Map[TopicPartition, Offset]): UIO[Unit] =
          for {
            waitForOffsets <- WaitForOffsets.make(offsets)
            done <- state.modify(_.waitingFor(waitForOffsets))
            _ <- ZIO.when(!done)(waitForOffsets.await)
          } yield ()
      }
    }

  def merge(a: Map[TopicPartition, Offset],
            b: Map[TopicPartition, Offset]): Map[TopicPartition, Offset] = {
    val (smaller, bigger) = if (a.size < b.size) (a, b) else (b, a)
    smaller.foldLeft(bigger) {
      case (acc, (partition, offset)) =>
        acc + (partition -> updatedOffset(acc, partition, offset))
    }
  }

  case class State(committable: Map[TopicPartition, Offset],
                   committed: Map[TopicPartition, Offset],
                   waiting: List[WaitForOffsets]) {

    def commit: (Map[TopicPartition, Offset], State) =
      (committable, copy(committable = Map.empty, committed = merge(committed, committable)))

    def update(partition: TopicPartition, offset: Offset): (UIO[Unit], State) = {
      val updatedCommittable = committable + (partition -> updatedOffset(committable, partition, offset))
      val (ready, stillWaiting) = checkWaiting(merge(committed, updatedCommittable))
      (ready, copy(committable = updatedCommittable, waiting = stillWaiting))
    }

    def waitingFor(waitForOffsets: WaitForOffsets): (Boolean, State) =
      if (waitForOffsets.isReady(merge(committed, committable))) (true, this)
      else (false, copy(waiting = waitForOffsets :: waiting))

    private def checkWaiting(offsets: Map[TopicPartition, Offset]): (UIO[Unit], List[WaitForOffsets]) =
      waiting.foldLeft((ZIO.unit, List.empty[WaitForOffsets])) {
        case ((ready, waiting), current) =>
          if (current.isReady(offsets)) (ready *> current.resolve, waiting)
          else (ready, current :: waiting)
      }

  }

  private def updatedOffset(offsets: Map[TopicPartition, Offset],
                            partition: TopicPartition,
                            offset: Offset): Offset =
    offsets.get(partition).foldLeft(offset)(_ max _)

  object State {
    val Empty = State(Map.empty, Map.empty, Nil)
  }

  case class WaitForOffsets(minimumOffsets: Map[TopicPartition, Offset],
                            promise: Promise[Nothing, Unit]) {

    def await: UIO[Unit] = promise.await

    def resolve: UIO[Unit] = promise.succeed(()).unit

    def isReady(currentOffsets: Map[TopicPartition, Offset]): Boolean =
      WaitForOffsets.isReady(minimumOffsets, currentOffsets)

  }

  object WaitForOffsets {
    def make(minimumOffsets: Map[TopicPartition, Offset]): UIO[WaitForOffsets] =
      Promise.make[Nothing, Unit].map(WaitForOffsets(minimumOffsets, _))

    def isReady(minimumOffsets: Map[TopicPartition, Offset],
                currentOffsets: Map[TopicPartition, Offset]): Boolean =
      minimumOffsets.forall {
        case (partition, minimumOffset) =>
          val partitionLag = minimumOffset - currentOffsets.getOrElse(partition, -1L)
          partitionLag <= 0
      }
  }
}
