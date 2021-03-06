import sbt._

object Dependencies {

  val scala2_12 = "2.12.13"
  val scala2_13 = "2.13.3"

  val scala = Seq(scala2_12, scala2_13)

  val kafkaClients     = "org.apache.kafka"  % "kafka-clients"       % "2.3.0"
  val catsCore         = "org.typelevel"     %% "cats-core"          % "2.2.0"
  val catsEffect       = "org.typelevel"     %% "cats-effect"        % "2.2.0"
  val catsRetry        = "com.github.cb372"  %% "cats-retry"         % "1.1.1"
  val fs2              = "co.fs2"            %% "fs2-core"           % "2.4.4"
  val config           = "com.typesafe"      % "config"              % "1.4.0"
  val slf4j            = "org.slf4j"         % "slf4j-api"           % "1.7.25"
  val logback          = "ch.qos.logback"    % "logback-classic"     % "1.2.3"
  val scalaTest        = "org.scalatest"     %% "scalatest"          % "3.2.2"
  val scalaMeter       = "com.storm-enroute" %% "scalameter"         % "0.19"
  val scalaMock        = "org.scalamock"     %% "scalamock"          % "4.4.0"
  val izumiReflect     = "dev.zio"           %% "izumi-reflect"      % "1.0.0-M9"
  val betterMonadicFor = "com.olegpy"        %% "better-monadic-for" % "0.3.1"
  val kindProjector    = "org.typelevel"     %% "kind-projector"     % "0.10.3"

  val circe = Seq(
    "io.circe" %% "circe-core"    % "0.13.0" % Provided,
    "io.circe" %% "circe-parser"  % "0.13.0" % Provided,
    "io.circe" %% "circe-generic" % "0.13.0" % Test,
    "io.circe" %% "circe-literal" % "0.13.0" % Test
  )

  val vulcan = Seq(
    "com.github.fd4s" %% "vulcan"            % "1.2.0" % Provided,
    "com.github.fd4s" %% "vulcan-generic"    % "1.2.0" % Test,
    "com.github.fd4s" %% "vulcan-enumeratum" % "1.2.0" % Test
  )
}
