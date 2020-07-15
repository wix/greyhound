package com.wixpress.dst.greyhound.core.producer

import org.apache.kafka.common.KafkaException
import org.apache.kafka.common.errors.{AuthenticationException, AuthorizationException, InterruptException, SerializationException, TimeoutException}
import zio.IO

sealed abstract class ProducerError(cause: Throwable) extends RuntimeException(cause)

case class SerializationError(cause: Throwable) extends ProducerError(cause)
case class AuthenticationError(cause: AuthenticationException) extends ProducerError(cause)
case class AuthorizationError(cause: AuthorizationException) extends ProducerError(cause)
case class IllegalStateError(cause: IllegalStateException) extends ProducerError(cause)
case class InterruptError(cause: InterruptException) extends ProducerError(cause)
case class TimeoutError(cause: TimeoutException) extends ProducerError(cause)
case class KafkaError(cause: KafkaException) extends ProducerError(cause)

object ProducerError {
  def apply(exception: Throwable): IO[ProducerError, Nothing] = exception match {
    case e: AuthenticationException => IO.fail(AuthenticationError(e))
    case e: AuthorizationException => IO.fail(AuthorizationError(e))
    case e: IllegalStateException => IO.fail(IllegalStateError(e))
    case e: InterruptException => IO.fail(InterruptError(e))
    case e: SerializationException => IO.fail(SerializationError(e))
    case e: TimeoutException => IO.fail(TimeoutError(e))
    case e: KafkaException => IO.fail(KafkaError(e))
    case e => IO.die(e)
  }
}
