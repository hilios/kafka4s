package io.kafka4s.effect.producer.adts

/**
  * The number of acknowledgments the producer requires the leader to have received before considering a request
  * complete. This controls the durability of records that are sent.
  *
  * @see [[org.apache.kafka.clients.producer.ProducerConfig.ACKS_DOC]]
  */
sealed abstract class Acks private (val value: String) { self =>
  def unapply(value: String): Option[self.type] = if (value == self.value) Some(self) else None
}

object Acks {
  final object Zero extends Acks(value = "0")
  final object One extends Acks(value  = "1")
  final object All extends Acks(value  = "all")

  def apply(value: String): Acks = value match {
    case Zero(a) => a
    case One(a)  => a
    case All(a)  => a
  }
}
