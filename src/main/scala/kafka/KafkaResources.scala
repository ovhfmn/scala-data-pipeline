package kafka

import cats.effect.{IO, Resource}
import config.KafkaConsumerConfig
import fs2.kafka.{KafkaProducer, ProducerSettings, KafkaConsumer, ConsumerSettings, AutoOffsetReset}

object KafkaResources {

  def producer(bootstrapServer: String): Resource[IO, KafkaProducer[IO, String, String]] = {
    val settings =
      ProducerSettings[IO, String, String]
        .withBootstrapServers(bootstrapServer)

    KafkaProducer.resource(settings)
  }

  def consumer(config: KafkaConsumerConfig): Resource[IO, KafkaConsumer[IO, String, String]] = {
    val settings =
      ConsumerSettings[IO, String, String]
        .withBootstrapServers(config.bootstrapServers)
        .withGroupId(config.groupId)
        .withAutoOffsetReset(AutoOffsetReset.Earliest)

    KafkaConsumer.resource(settings)
  }
}
