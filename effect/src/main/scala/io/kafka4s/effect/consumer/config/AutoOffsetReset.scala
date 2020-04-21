package io.kafka4s.effect.consumer.config

/**
  * What to do when there is no initial offset in Kafka or if the current offset does not exist any more on the server.
  * earliest: automatically reset the offset to the earliest offset
  * latest: automatically reset the offset to the latest offset
  * none: throw exception to the consumer if no previous offset is found for the consumer's group
  * anything else: throw exception to the consumer.
  *
  * @see [[org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_OFFSET_RESET_CONFIG]]
  */
sealed abstract class AutoOffsetReset private (val value: String) { self =>
  def unapply(value: String): Option[self.type] = if (value == self.value) Some(self) else None
}

object AutoOffsetReset {
  final object Earliest extends AutoOffsetReset(value = "earliest")
  final object Latest extends AutoOffsetReset(value   = "latest")
  final object None extends AutoOffsetReset(value     = "none")

  def apply(value: String): AutoOffsetReset = value match {
    case Earliest(a) => a
    case Latest(a)   => a
    case None(a)     => a
  }
}
