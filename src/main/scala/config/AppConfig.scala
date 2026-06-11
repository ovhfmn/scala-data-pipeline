package config

import cats.effect.IO.{catsSyntaxTuple3Parallel, catsSyntaxTuple4Parallel}
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
                      concurrency: Int
                    )

/** Loads configuration from environment variables using Ciris.
 * Falls back to defaults when variables are not set.
 */
object AppConfig {
  def load: ConfigValue[Effect, AppConfig] = (
    env("KAFKA_BOOTSTRAP_SERVERS").as[String].default("redpanda:29092"),
    env("KAFKA_GROUP_ID").as[String].default("account-events"),
    env("KAFKA_TOPIC").as[String].default("account-events"),
    env("CONCURRENCY").as[Int].default(16)
  ).parMapN(AppConfig.apply)
}
