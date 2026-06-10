package io.kafka4s.middlewares.otel

import cats.effect.Sync
import io.kafka4s.{Consumer, Producer}
import io.opentelemetry.api.{GlobalOpenTelemetry, OpenTelemetry}


object OtelMiddleware {


  def global[F[_] : Sync](implicit F: Sync[F]): F[OpenTelemetry] =
    F.delay(GlobalOpenTelemetry.get())


  def consumer[F[_]](consumer: Consumer[F]): Consumer[F] =
    consumer(GlobalOpenTelemetry)(consumer)

  def producer[F[_]](producer: Producer[F]): Producer[F] =
    producer(GlobalOpenTelemetry)(producer)

  def consumer[F[_]](otel: OpenTelemetry)(consumer: Consumer[F])(implicit F: Sync[F]): Consumer[F] = Kleisli { record =>
    for {
      tracer <- global
    } yield ???
  }

  def producer[F[_]](otel: OpenTelemetry)(producer: Producer[F]): Producer[F] = Kleisli { record =>
    for {
      tracer <- F.delay(otel.getTracer("io.kafka4s.producer"))
    } yield ???
  }
}
