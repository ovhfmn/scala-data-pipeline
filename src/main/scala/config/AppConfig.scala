package config

import cats.effect.IO
import cats.effect.IO.{catsSyntaxTuple3Parallel, catsSyntaxTuple4Parallel}
import cats.syntax.parallel.catsSyntaxParallelSequence_
import ciris._

/** Immutable data configuration record capturing runtime parameter bindings for the service.
 *
 * @param kafkaBroker The connection topology URI matching the targeted Kafka/Redpanda bootstrap cluster.
 * @param topic       The upstream Kafka event feed stream to listen to.
 * @param concurrency The maximum upper bound of concurrent fiber pipelines allocated to process
 * and dispatch notifications simultaneously via `mapAsync`.
 */
case class AppConfig(
                      kafkaBroker: String,
                      groupId: String,
                      topic: String,
                      concurrency: Int
                    )

/** Companion object acting as a purely functional configuration loader utilizing [[ciris]].
 *
 * Reads environmental configurations explicitly from system parameters. Evaluates safely
 * in parallel using [[cats.Parallel]] syntax constructs before binding values into runtime spaces.
 */
object AppConfig {
  def load: ConfigValue[Effect, AppConfig] = (
    env("KAFKA_BOOTSTRAP_SERVERS").as[String].default("redpanda:29092"),
    env("KAFKA_GROUP_ID").as[String].default("account-events"),
    env("KAFKA_TOPIC").as[String].default("account-events"),
    env("CONCURRENCY").as[Int].default(16)
  ).parMapN(AppConfig.apply)
}
