package pipeline

import cats.effect.IO
import domain.AccountEvent
import domain.AccountEventCodec._
import fs2.Stream
import fs2.kafka.KafkaConsumer
import io.circe.parser.decode

import scala.concurrent.duration.DurationInt
import scala.language.postfixOps

object EventPipeline {

  def stream(
              consumer: KafkaConsumer[IO, String, String],
              topic: String
            ): Stream[IO, Unit] = {
    Stream.eval(consumer.subscribeTo(topic)) >> consumer
      .stream
      .evalMap { committable =>
        val rawJson = committable.record.value

        decode[AccountEvent](rawJson) match {
          case Right(event) => IO.pure((event, committable.offset))
          case Left(error) => IO.raiseError(error)
        }
      }
      .groupWithin(10, 5.seconds)
      .evalMap { chunk =>
        val batch = chunk.toList
        val events = batch.map(_._1)
        val offsets = batch.map(_._2)

        for {
          _ <- IO.println(s"Processing batch of ${events.size} events")
          _ <- IO.println(events)
          _ <- offsets.last.commit
          _ <- IO.println(s"Commited batch offset")
        } yield ()
      }
  }
}