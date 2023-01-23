package com.wixpress.dst.greyhound.core.producer

import org.apache.kafka.common.KafkaException
import org.apache.kafka.common.errors._
import zio.{IO, ZIO}

sealed abstract class ProducerError(cause: Throwable) extends RuntimeException(s"${cause.getClass.getName}: ${cause.getMessage}", cause)

case class SerializationError(cause: Throwable)                extends ProducerError(cause)
case class AuthenticationError(cause: AuthenticationException) extends ProducerError(cause)
case class AuthorizationError(cause: AuthorizationException)   extends ProducerError(cause)
case class IllegalStateError(cause: IllegalStateException)     extends ProducerError(cause)
case class InterruptError(cause: InterruptException)           extends ProducerError(cause)
case class TimeoutError(cause: TimeoutException)               extends ProducerError(cause)
case class KafkaError(cause: KafkaException)                   extends ProducerError(cause)
case class GrpcProxyError(cause: GrpcError)                    extends ProducerError(cause)
case class ProducerClosedError()                               extends ProducerError(ProducerClosed())
case class IllegalArgumentError(e: IllegalArgumentException)   extends ProducerError(e)
case class UnknownError(cause: Throwable)                      extends ProducerError(cause)

object ProducerError {
  def from(exception: Throwable): ProducerError               = {
    exception match {
      case e: ProducerError            => e
      case e: AuthenticationException  => AuthenticationError(e)
      case e: AuthorizationException   => AuthorizationError(e)
      case e: IllegalStateException    => IllegalStateError(e)
      case e: InterruptException       => InterruptError(e)
      case e: SerializationException   => SerializationError(e)
      case e: TimeoutException         => TimeoutError(e)
      case e: KafkaException           => KafkaError(e)
      case e: GrpcError                => GrpcProxyError(e)
      case e: IllegalArgumentException => IllegalArgumentError(e)
      case _: ProducerClosed           => ProducerClosedError()
      case e                           => UnknownError(e)
    }
  }
  def apply(exception: Throwable): IO[ProducerError, Nothing] = ZIO.fail(ProducerError.from(exception))
}

case class ProducerClosed()            extends RuntimeException("Producer is closing, not accepting writes")
case class GrpcError(cause: Throwable) extends RuntimeException(cause)
