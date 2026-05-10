package config

final case class KafkaConfig(
                              bootstrapServers: String,
                              topic: String
                            )
