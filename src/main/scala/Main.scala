import cats.effect.{IO, IOApp}
import cats.implicits.catsSyntaxTuple2Semigroupal
import config.{AppConfig, KafkaConfig, KafkaConsumerConfig}
import kafka.KafkaResources
import pipeline.EventPipeline

object Main extends IOApp.Simple {

  override def run: IO[Unit] = {
    for {
      config <- AppConfig.load.load[IO]
      _ <- IO.println(s"Instance Id: ${config.instanceId}")

      producerConfig = KafkaConfig(
        bootstrapServers = config.kafkaBroker,
        topic = config.topic)

      consumerConfig = KafkaConsumerConfig(
        bootstrapServers = config.kafkaBroker,
        groupId = config.groupId,
        topic = config.topic,
        deadLetterTopic = config.deadLetterTopic
      )

      _ <- (KafkaResources.producer(producerConfig.bootstrapServers),
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
                consumerConfig.deadLetterTopic,
                config.instanceId
              )
              .compile
              .drain
          } yield ()
      }
    } yield ()
  }
}