package com.wixpress.dst.greyhound.core.zioutils

import com.wixpress.dst.greyhound.core.zioutils.PromiseSyntax._
import zio.Promise
import zio.duration._
import zio.test.Assertion._
import zio.test._
import zio.test.environment.Live
import zio.test.junit.JUnitRunnableSpec

class PromiseSyntaxTest extends JUnitRunnableSpec {
  def spec = suite("PromiseSyntaxTest") (
    suite("map") (
      testM("map successful promise") {
        for {
          promise <- Promise.make[Throwable, String]
          _ <- (Live.live(zio.clock.sleep(10.millis)) *> promise.succeed("foo")).fork
          mapped <- promise.map(_ + "bar")
          res <- mapped.await
        } yield {
          assert(res)(equalTo("foobar"))
        }
      },
      testM("map failing promise") {
        val e = new RuntimeException("kaboom")
        for {
          promise <- Promise.make[Throwable, String]
          _ <- (Live.live(zio.clock.sleep(10.millis)) *> promise.fail(e)).fork
          mapped <- promise.map(_ + "bar")
          res <- mapped.await.either
        } yield {
          assert(res)(isLeft(equalTo(e)))
        }
      },
      testM("map dying promise") {
        val e = new RuntimeException("kaboom")
        for {
          promise <- Promise.make[Throwable, String]
          _ <- (Live.live(zio.clock.sleep(10.millis)) *> promise.die(e)).fork
          mapped <- promise.map(_ + "bar")
          res <- mapped.await.cause
        } yield {
          assert(res.dieOption)(isSome(equalTo(e)))
        }
      },
      testM("map interrupted promise") {
        val e = new RuntimeException("kaboom")
        for {
          promise <- Promise.make[Throwable, String]
          _ <- (Live.live(zio.clock.sleep(1000000.millis)) *> promise.succeed("foo")).fork
          _ <- (Live.live(zio.clock.sleep(10.millis)) *> promise.interrupt).fork
          mapped <- promise.map(_ + "bar")
          res <- mapped.await.cause
        } yield {
          assert(res.interrupted)(isTrue)
        }
      }
    )
  )
}
