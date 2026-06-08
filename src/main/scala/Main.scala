import cats.effect.{IO, IOApp}
import cats.implicits.{catsSyntaxTuple2Semigroupal, toFoldableOps}
import config.{KafkaConfig, KafkaConsumerConfig}
import kafka.KafkaResources
import pipeline.EventPipeline

object Main extends IOApp.Simple {

  private val producerConfig = KafkaConfig(
    bootstrapServers = "redpanda:29092",
    topic = "account-events")

  private val consumerConfig = KafkaConsumerConfig(
    bootstrapServers = "redpanda:29092",
    groupId = "data-pipe-line",
    topic = "account-events",
    deadLetterTopic = "account-events-dlq"
  )

  override def run: IO[Unit] = {
    (KafkaResources.producer(producerConfig.bootstrapServers),
      KafkaResources.consumer(consumerConfig)
    ).tupled.use {
      case (kafkaProducer, kafkaConsumer) =>

        for {
          _ <- IO.println("Publishing events...")
          _ <- IO.println("Starting consumer stream ...")
          _ <- EventPipeline
            .stream(
              kafkaConsumer,
              kafkaProducer,
              consumerConfig.topic,
              consumerConfig.deadLetterTopic
            )
            .compile
            .drain
        } yield ()
    }
  }
}