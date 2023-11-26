package io.kafka4s.effect.consumer.config

sealed trait Semantic

object Semantic {
  object AtLeastOnce extends Semantic
}
