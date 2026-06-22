package config

import cats.effect.IO.catsSyntaxTuple6Parallel
import ciris.{ConfigValue, Effect, env}

/** Runtime configuration for the pipeline service.
 *
 * @param kafkaBroker Bootstrap server address for the Kafka/Redpanda cluster.
 * @param groupId     Consumer group identifier for offset tracking.
 * @param topic       Kafka topic to consume events from.
 * @param concurrency Maximum number of events processed concurrently.
 */
case class AppConfig(
                      kafkaBroker: String,
                      groupId: String,
                      topic: String,
                      deadLetterTopic: String,
                      concurrency: Int,
                      instanceId: String
                    )

/** Loads configuration from environment variables using Ciris.
 * Falls back to defaults when variables are not set.
 */
object AppConfig {
  def load: ConfigValue[Effect, AppConfig] = (
    env("KAFKA_BOOTSTRAP_SERVERS").as[String].default("redpanda:29092"),
    env("KAFKA_GROUP_ID").as[String].default("data-pipeline"),
    env("KAFKA_TOPIC").as[String].default("account-events"),
    env("KAFKA_DLQ_TOPIC").as[String].default("account-events-dlq"),
    env("CONCURRENCY").as[Int].default(16),
    env("INSTANCE_ID").as[String].default(java.util.UUID.randomUUID().toString.take(8))
  ).parMapN(AppConfig.apply)
}
