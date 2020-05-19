import sbt._

ThisBuild / scalaVersion           := "2.12.10"
ThisBuild / version                := "0.1.0"
ThisBuild / organization           := "io.kafka4s"
ThisBuild / organizationName       := "Kafka4s"
ThisBuild / turbo                  := true

Global / concurrentRestrictions := Seq(Tags.limitAll(1))

lazy val kafka4s = project.in(file("."))
  .enablePlugins(MicrositesPlugin)
  .aggregate(core, effect)
  .settings(Microsite.settings)
  .settings(
    // Root project
    name := "kafka4s",
    skip in publish := true,
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
  .configs(IntegrationTest)
  .settings(Defaults.itSettings)
  .settings(
    commonSettings,
    libraryDependencies ++= Seq(
      Dependencies.catsEffect % Provided,
      Dependencies.config,
      Dependencies.slf4j,
      Dependencies.logback % IntegrationTest,
      Dependencies.scalaTest % IntegrationTest,
    )
  )

lazy val fs2 = project.in(file("fs2"))
  .dependsOn(core, effect)
  .configs(IntegrationTest)
  .settings(Defaults.itSettings)
  .settings(
    commonSettings,
    libraryDependencies ++= Seq(
      Dependencies.fs2 % Provided,
      Dependencies.config,
      Dependencies.slf4j,
      Dependencies.logback, // % IntegrationTest,
      Dependencies.scalaTest % IntegrationTest,
    )
  )

lazy val commonSettings = Seq(
  autoCompilerPlugins := true,
  fork in Test := true,
  fork in IntegrationTest := true,
  parallelExecution in Test := false,
  parallelExecution in IntegrationTest := false,
  libraryDependencies ++= Seq(
    Dependencies.scalaReflect % scalaVersion.value,
    Dependencies.scalaMock % Test,
    Dependencies.scalaTest % Test,
    compilerPlugin(Dependencies.betterMonadicFor),
    compilerPlugin(Dependencies.kindProjector),
  ),
  scalacOptions ++= Seq(
    "-deprecation",
    "-encoding",
    "utf-8",
    "-explaintypes",
    "-feature",
    "-language:existentials",
    "-language:experimental.macros",
    "-language:higherKinds",
    "-language:implicitConversions",
    "-unchecked",
    "-Xcheckinit",
//    "-Xfatal-warnings",
    "-Xfuture",
    "-Xlint:adapted-args",
    "-Xlint:by-name-right-associative",
    "-Xlint:constant",
    "-Xlint:delayedinit-select",
    "-Xlint:doc-detached",
    "-Xlint:inaccessible",
    "-Xlint:infer-any",
    "-Xlint:missing-interpolator",
    "-Xlint:nullary-override",
    "-Xlint:nullary-unit",
    "-Xlint:option-implicit",
    "-Xlint:package-object-classes",
    "-Xlint:poly-implicit-overload",
    "-Xlint:private-shadow",
    "-Xlint:stars-align",
    "-Xlint:type-parameter-shadow",
    "-Xlint:unsound-match",
    "-Yno-adapted-args",
    "-Ypartial-unification",
    "-Ywarn-dead-code",
    "-Ywarn-extra-implicit",
    "-Ywarn-inaccessible",
    "-Ywarn-infer-any",
    "-Ywarn-nullary-override",
    "-Ywarn-nullary-unit",
    "-Ywarn-numeric-widen",
    "-Ywarn-unused:implicits",
    "-Ywarn-unused:imports",
    "-Ywarn-unused:locals",
    "-Ywarn-unused:params",
    "-Ywarn-unused:patvars",
    "-Ywarn-unused:privates",
    "-Ywarn-value-discard",
  ),
  javacOptions ++= Seq(
    "-source", "1.9",
    "-target", "1.9"
  )
)
