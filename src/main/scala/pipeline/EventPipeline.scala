package pipeline

import cats.effect.IO
import domain.AccountEvent
import domain.AccountEventCodec._
import fs2.Stream
import fs2.kafka.KafkaConsumer
import io.circe.parser.decode

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
          case Right(event) => IO.println(s"Received event: $event")
          case Left(error) => IO.println(s"Failed decodeing: $error")
        }
      }
  }
}