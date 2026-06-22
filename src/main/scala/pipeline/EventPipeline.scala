package pipeline

import cats.effect.IO
import domain.AccountEvent
import fs2.Stream
import fs2.kafka.{KafkaConsumer, KafkaProducer, ProducerRecord}
import io.circe.parser.decode
import storage.EventFileWriter

import scala.concurrent.duration.DurationInt
import scala.language.postfixOps

/** Core streaming pipeline — consumes account events from Kafka,
 * persists valid events to JSONL, and isolates malformed events to the DLQ.
 */
object EventPipeline {

  /** Builds the main fs2 stream that runs for the lifetime of the service.
   *
   * Pipeline stages:
   *  1. Subscribe and consume raw JSON messages from `topic`
   *     2. Decode each message into [[AccountEvent]] — failures are routed to DLQ and skipped
   *     3. Buffer valid events into micro-batches via `groupWithin`
   *     4. Persist each batch to JSONL and commit offsets only after a successful write
   *
   * Offsets are never committed on decode failure — the DLQ handles those independently.
   * This ensures at-least-once delivery: a crash before commit replays the batch on restart.
   *
   * @param consumer        fs2-kafka consumer, managed externally via Resource.
   * @param producer        fs2-kafka producer, scoped exclusively to DLQ publishing.
   * @param topic           Kafka topic to consume events from.
   * @param deadLetterTopic Kafka topic for malformed or undecodable events.
   */
  def stream(
              consumer: KafkaConsumer[IO, String, String],
              producer: KafkaProducer[IO, String, String],
              topic: String,
              deadLetterTopic: String,
              instanceId: String
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
            _ <- publishToDlq(producer, deadLetterTopic, rawJson, error.getMessage)
            _ <- committable.offset.commit
          } yield None
        }
      }
      .unNone
      .groupWithin(100, 180.seconds)
      .evalMap { chunk =>
        val (events, offsets) = chunk.toList.unzip
        for {
          _ <- IO.println(s"Processing batch of ${events.size} events")
          _ <- EventFileWriter.writeBatch(events, instanceId)
          _ <- IO.println(s"Wrote ${events.size} events to disk")
          _ <- offsets.last.commit
          _ <- IO.println(s"Committed batch offset")
        } yield ()
      }
  }

  /** Publishes a malformed raw payload to the DLQ topic with the associated decoding error.
   *
   * Keyed as "dead-letter" — all malformed events land in the same DLQ partition,
   * preserving inspectability without polluting per-account ordering guarantees.
   *
   * @param producer fs2-kafka producer.
   * @param topic    DLQ topic name.
   * @param payload  Raw JSON string that failed to decode.
   * @param error    Decoding error message.
   */
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