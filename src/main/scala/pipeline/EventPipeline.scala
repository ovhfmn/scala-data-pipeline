package pipeline

import cats.effect.IO
import domain.AccountEvent
import domain.AccountEventCodec._
import fs2.Stream
import fs2.kafka.{KafkaConsumer, KafkaProducer, ProducerRecord}
import io.circe.parser.decode
import storage.EventFileWriter

import scala.concurrent.duration.DurationInt
import scala.language.postfixOps

object EventPipeline {

  def stream(
              consumer: KafkaConsumer[IO, String, String],
              producer: KafkaProducer[IO, String, String],
              topic: String,
              deadLetterTopic: String
            ): Stream[IO, Unit] = {
    Stream.eval(consumer.subscribeTo(topic)) >> consumer
      .stream
      .evalMap { committable =>
        val rawJson = committable.record.value

        decode[AccountEvent](rawJson) match {
          case Right(event) => IO.pure(Some((event, committable.offset)))
          case Left(error) => for {
            _ <- IO.println(
              s"""
                 |Failed decoding event:
                 |payload: $rawJson
                 |error: $error
                 |""".stripMargin
            )

            _ <- publishToDlq(
              producer,
              deadLetterTopic,
              rawJson,
              error.getMessage
            )

            _ <- committable.offset.commit
          } yield None
        }
      }
      .unNone
      .groupWithin(10, 5.seconds)
      .evalMap { chunk =>
        val batch = chunk.toList
        val (events, offsets) = batch.unzip

        for {
          _ <- IO.println(s"Processing batch of ${events.size} events")
          _ <- EventFileWriter.writeBatch(events)
          _ <- IO.println(s"Wrote ${events.size} events to disk")
          _ <- offsets.last.commit
          _ <- IO.println(s"Commited batch offset")
        } yield ()
      }
  }

  private def publishToDlq(
                            producer: KafkaProducer[IO, String, String],
                            topic: String,
                            payload: String,
                            error: String
                          ): IO[Unit] = {
    val record = ProducerRecord(
      topic = topic,
      key = "dead-letter",
      value =
        s"""
           |{
           | "payload": $payload,
           | "error": $error
           |}
           |""".stripMargin
    )

    producer
      .produceOne(record)
      .flatten
      .void
  }
}