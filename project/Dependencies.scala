import sbt._

object Dependencies {
  val kafkaClients     = "org.apache.kafka" % "kafka-clients" % "2.3.0"
  val catsCore         = "org.typelevel"    %% "cats-core" % "1.6.0"
  val catsEffect       = "org.typelevel"    %% "cats-effect" % "1.4.0"
  val catsRetry        = "com.github.cb372" %% "cats-retry" % "1.1.0"
  val fs2              = "co.fs2"           %% "fs2-core" % "2.3.0"
  val config           = "com.typesafe"     % "config" % "1.4.0"
  val slf4j            = "org.slf4j"        % "slf4j-api" % "1.7.25"
  val logback          = "ch.qos.logback"   % "logback-classic" % "1.2.3"
  val scalaTest        = "org.scalatest"    %% "scalatest" % "3.1.1"
  val scalaMock        = "org.scalamock"    %% "scalamock" % "4.4.0"
  val scalaReflect     = "org.scala-lang"   % "scala-reflect"
  val betterMonadicFor = "com.olegpy"       %% "better-monadic-for" % "0.3.1"
  val kindProjector    = "org.typelevel"    %% "kind-projector" % "0.10.3"
}
