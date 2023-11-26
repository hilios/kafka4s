import sbt._

ThisBuild / scalaVersion      := Dependencies.scala2_13
ThisBuild / organization      := "io.kafka4s"
ThisBuild / organizationName  := "Kafka4s"
ThisBuild / turbo             := true
ThisBuild / semanticdbEnabled := true
ThisBuild / semanticdbVersion := scalafixSemanticdb.revision

Global / concurrentRestrictions := Seq(Tags.limitAll(1))

lazy val kafka4s = project.in(file("."))
//  .enablePlugins(MicrositesPlugin)
  .aggregate(core, effect, fs2, circe, e2e)
//  .settings(Microsite.settings)
  .settings(
    // Root project
    name := "kafka4s",
    skip / publish := true,
    description := "A minimal Scala-idiomatic library for Kafka",
  )

lazy val core = project.in(file("core"))
  .settings(
    commonSettings,
    libraryDependencies ++= Seq(
      Dependencies.kafkaClients,
      Dependencies.catsCore % Provided,
      Dependencies.catsRetry,
    )
  )

lazy val effect = project.in(file("effect"))
  .dependsOn(core)
  .settings(
    commonSettings,
    libraryDependencies ++= Seq(
      Dependencies.catsEffect % Provided,
      Dependencies.config,
      Dependencies.slf4j
    )
  )

lazy val fs2 = project.in(file("fs2"))
  .dependsOn(core, effect)
  .settings(
    commonSettings,
    libraryDependencies ++= Seq(
      Dependencies.fs2 % Provided,

    )
  )

lazy val circe = project.in(file("circe"))
  .dependsOn(core)
  .settings(
    commonSettings,
    libraryDependencies ++= Dependencies.circe ++ Seq(
      Dependencies.scalaTest % Test,
    )
  )

lazy val e2e = project.in(file("e2e"))
  .dependsOn(effect, fs2)
  .settings(
    commonSettings,
    skip / publish := true,
    libraryDependencies ++= Seq(
      Dependencies.logback % Test,
      Dependencies.scalaMeter % Test
    )
  )

lazy val commonSettings = Seq(
  autoCompilerPlugins := true,
//  fork / Test := true,
//  fork / IntegrationTest := true,
//  parallelExecution / Test := false,
//  parallelExecution / IntegrationTest := false,
  libraryDependencies ++= Seq(
    Dependencies.izumiReflect,
    Dependencies.scalaMock % Test,
    Dependencies.scalaTest % Test,
  ),
  addCompilerPlugin(Dependencies.betterMonadicFor),
  addCompilerPlugin(Dependencies.kindProjector),
  scalacOptions ++= Seq(
    "-deprecation",
    "-feature",
    "-unchecked",
//    "-Xfatal-warnings",
    "-Xlint",
    "-Ywarn-dead-code",
    "-Ywarn-numeric-widen",
    "-Ywarn-value-discard",
    "-Wunused"
  ),
  javacOptions ++= Seq(
    "-source", "1.9",
    "-target", "1.9"
  )
)
