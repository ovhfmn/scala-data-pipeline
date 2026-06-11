package kafka

import cats.effect.{IO, Resource}
import config.KafkaConsumerConfig
import fs2.kafka.{KafkaProducer, ProducerSettings, KafkaConsumer, ConsumerSettings, AutoOffsetReset}

/** Manages Kafka producer and consumer lifecycle as Cats Effect Resources.
 *
 * All connections are acquired lazily and released deterministically on shutdown,
 * preventing connection leaks regardless of how the application exits.
 */
object KafkaResources {

  /** Creates a Kafka producer for publishing messages to a topic.
   *
   * Scoped exclusively to DLQ publishing in this pipeline —
   * valid events are never republished.
   *
   * @param bootstrapServer Kafka/Redpanda bootstrap server address.
   */
  def producer(bootstrapServer: String): Resource[IO, KafkaProducer[IO, String, String]] =
    KafkaProducer.resource(
      ProducerSettings[IO, String, String]
        .withBootstrapServers(bootstrapServer)
    )

  /** Creates a Kafka consumer subscribed to the configured topic.
   *
   * Uses `AutoOffsetReset.Earliest` to ensure no events are missed on
   * first startup or after a consumer group reset.
   * Offsets are committed manually after each batch is successfully persisted.
   *
   * @param config Consumer configuration — broker address, group ID, and topic.
   */
  def consumer(config: KafkaConsumerConfig): Resource[IO, KafkaConsumer[IO, String, String]] =
    KafkaConsumer.resource(
      ConsumerSettings[IO, String, String]
        .withBootstrapServers(config.bootstrapServers)
        .withGroupId(config.groupId)
        .withAutoOffsetReset(AutoOffsetReset.Earliest)
    )
}