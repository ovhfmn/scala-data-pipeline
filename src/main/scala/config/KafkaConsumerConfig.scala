package config

final case class KafkaConsumerConfig(
                                    bootstrapServers: String,
                                    topic: String,
                                    groupId: String
                                    )
