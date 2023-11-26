import sbt._

object Dependencies {

  val scala2_12 = "2.12.18"
  val scala2_13 = "2.13.12"
  val scala3 = "3.3.1"

  val scala: Seq[String] = Seq(scala2_12, scala2_13)

  val kafkaClients     = "org.apache.kafka"  % "kafka-clients"       % "3.4.0"
  val catsCore         = "org.typelevel"     %% "cats-core"          % "2.9.0"
  val catsEffect       = "org.typelevel"     %% "cats-effect"        % "2.5.5"
  val catsRetry        = "com.github.cb372"  %% "cats-retry"         % "2.1.1"
  val fs2              = "co.fs2"            %% "fs2-core"           % "2.5.11"
  val config           = "com.typesafe"      % "config"              % "1.4.2"
  val slf4j            = "org.slf4j"         % "slf4j-api"           % "1.7.2"
  val logback          = "ch.qos.logback"    % "logback-classic"     % "1.2.12"
  val scalaTest        = "org.scalatest"     %% "scalatest"          % "3.2.15"
  val scalaMeter       = "com.storm-enroute" %% "scalameter"         % "0.19"
  val scalaMock        = "org.scalamock"     %% "scalamock"          % "5.1.0"
  val izumiReflect     = "dev.zio"           %% "izumi-reflect"      % "2.3.6"
  val betterMonadicFor = "com.olegpy"        %% "better-monadic-for" % "0.3.1"
  val kindProjector    = "org.typelevel"     %% "kind-projector"     % "0.13.2" cross CrossVersion.full

  val circe = Seq(
    "io.circe" %% "circe-core"    % "0.14.6" % Provided,
    "io.circe" %% "circe-parser"  % "0.14.6" % Provided,
    "io.circe" %% "circe-generic" % "0.14.6" % Test,
    "io.circe" %% "circe-literal" % "0.14.6" % Test
  )

  val vulcan = Seq(
    "com.github.fd4s" %% "vulcan"            % "1.8.4" % Provided,
    "com.github.fd4s" %% "vulcan-generic"    % "1.8.4" % Test,
    "com.github.fd4s" %% "vulcan-enumeratum" % "1.8.4" % Test
  )
}
