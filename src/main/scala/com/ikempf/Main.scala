package com.ikempf

import cats.effect.{Concurrent, ConcurrentEffect, ExitCode, IO, IOApp}
import cats.implicits._
import fs2.Stream
import fs2.concurrent.{Queue, SignallingRef}
import StreamFinalizer._

import scala.concurrent.duration._

object Main extends IOApp {

  // ---------- Signal terminated queue ----------
  private val signal = SignallingRef.apply[IO, Boolean](false).unsafeRunSync()
  private val queue  = Queue.unbounded[IO, String].unsafeRunSync()

  private val consumerStreamSignalClosed =
    queue
      .dequeue
      .safe(signal)
      .evalTap(a => IO(println("received " + a)))
      .evalTap(_ => IO.sleep(1.second))
      .evalTap(a => IO(println("handled " + a)))
      .compile
      .drain

  private val producerSignalSignalCloser =
    (Stream.emits[IO, String](List("1", "2", "3")) ++ Stream.fixedRate(1.second).as("other"))
      .interruptWhen(haltWhenTrue = signal)
      .evalTap(queue.enqueue1)
      .evalMap(v => if (v == "3") signal.set(true) else IO(()))
      .compile
      .drain

  // 1, 2 and 3 are not always received. Values streaming and "signal flipping" are not sequenced, bad solution :/
  private def signalTerminated =
    ConcurrentEffect[IO]
      .racePair(consumerStreamSignalClosed, producerSignalSignalCloser)
      .flatMap {
        case Right((fiber, _)) => fiber.join
        case Left((_, fiber))  => fiber.join
      }
      .as(ExitCode.Success)

  // ---------- None terminated queue ----------
  private val queueNone = Queue.noneTerminated[IO, String].unsafeRunSync()

  private val consumerStreamNoneClosed =
    queueNone
      .dequeue
      .safe(signal)
      .evalTap(a => IO(println("received " + a)))
      .evalTap(_ => IO.sleep(1.second))
      .evalTap(a => IO(println("handled " + a)))
      .compile
      .drain

  private val producerStreamNoneCloser =
    (Stream.emits[IO, Option[String]](List(Some("1"), Some("2"), None)) ++ Stream.fixedRate(1.second).as(Some("other")))
      .safe(signal)
      .evalMap(queueNone.enqueue1)
      .compile
      .drain

  // 1, 2 and 3 are always received since they come before the closing None message.
  private def noneTerminated =
    ConcurrentEffect[IO]
      .racePair(consumerStreamNoneClosed, producerStreamNoneCloser)
      .flatMap {
        case Right((fiber, _)) => fiber.join
        case Left((_, fiber))  => fiber.join
      }
      .as(ExitCode.Success)

  override def run(args: List[String]): IO[ExitCode] =
    signalTerminated
//    noneTerminated

}

object StreamFinalizer {
  implicit class StreamOps[F[_]: Concurrent, O](stream: Stream[F, O]) {
    def safe(signal: SignallingRef[F, Boolean]): Stream[F, O] =
      stream
        .onFinalize(signal.set(true))
        .interruptWhen(haltWhenTrue = signal)
  }
}
