package kafka

import cats.effect.{IO, Resource}
import fs2.kafka.{KafkaProducer, ProducerSettings}

object KafkaResources {

  def producer(bootstrapServer: String): Resource[IO, KafkaProducer[IO, String, String]] = {
    val settings =
      ProducerSettings[IO, String, String]
        .withBootstrapServers(bootstrapServer)

    KafkaProducer.resource(settings)
  }
}
