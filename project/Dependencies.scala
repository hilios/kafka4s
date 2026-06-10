import sbt.*

object Dependencies {

  val scala2_12 = "2.12.18"
  val scala2_13 = "2.13.17"
  val scala3 = "3.3.1"

  val scala: Seq[String] = Seq(scala2_12, scala2_13)

  val kafkaClients     = "org.apache.kafka"  % "kafka-clients"       % "4.1.0"
  val catsCore         = "org.typelevel"     %% "cats-core"          % "2.13.0"
  val catsEffect       = "org.typelevel"     %% "cats-effect"        % "3.6.3"
  val catsRetry        = "com.github.cb372"  %% "cats-retry"         % "3.1.3"
  val fs2              = "co.fs2"            %% "fs2-core"           % "3.12.2"
  val config           = "com.typesafe"      % "config"              % "1.4.5"
  val slf4j            = "org.slf4j"         % "slf4j-api"           % "2.0.17"
  val logback          = "ch.qos.logback"    % "logback-classic"     % "1.5.19"
  val scalaTest        = "org.scalatest"     %% "scalatest"          % "3.2.19"
  val scalaMeter       = "com.storm-enroute" %% "scalameter"         % "0.21"
  val scalaMock        = "org.scalamock"     %% "scalamock"          % "7.5.2"
  val izumiReflect     = "dev.zio"           %% "izumi-reflect"      % "3.0.6"
  val betterMonadicFor = "com.olegpy"        %% "better-monadic-for" % "0.3.1"
  val kindProjector    = "org.typelevel"     %% "kind-projector"     % "0.13.4" cross CrossVersion.full

  val testContainers = "com.dimafeng" %% "testcontainers-scala-scalatest" % "0.43.6"

  val otel = "io.opentelemetry" % "opentelemetry-api" % "1.56.0"

  val circe = Seq(
    "io.circe" %% "circe-core"    % "0.14.15" % Provided,
    "io.circe" %% "circe-parser"  % "0.14.15" % Provided,
    "io.circe" %% "circe-generic" % "0.14.15" % Test,
    "io.circe" %% "circe-literal" % "0.14.15" % Test
  )

  val vulcan = Seq(
    "com.github.fd4s" %% "vulcan"            % "1.12.0" % Provided,
    "com.github.fd4s" %% "vulcan-generic"    % "1.12.0" % Test,
    "com.github.fd4s" %% "vulcan-enumeratum" % "1.12.0" % Test
  )
}
