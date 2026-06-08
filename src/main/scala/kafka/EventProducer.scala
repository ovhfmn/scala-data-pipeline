package kafka

import cats.effect.IO
import domain.AccountEvent
import fs2.kafka.{KafkaProducer, ProducerRecord}
import io.circe.syntax.EncoderOps

class EventProducer(
                  producer: KafkaProducer[IO,String,String],
                  topic: String
                  ) {

  def publish(event: AccountEvent): IO[Unit] = {

    val record = ProducerRecord(
      topic = topic,
      key = event.accountId,
      value = event.asJson.noSpaces
    )

    producer
      .produceOne(record)
      .flatten
      .void
  }
}
