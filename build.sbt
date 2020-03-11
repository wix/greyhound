name := "greyhound"
organization in ThisBuild := "com.wix.greyhound"
scalaVersion in ThisBuild := "2.12.3"

ThisBuild / githubOwner := "wix-incubator"
ThisBuild / githubRepository := "greyhound"

ThisBuild / version := "0.1.0-SNAPSHOT"

publishMavenStyle := true

githubTokenSource := TokenSource.GitConfig("github.token")
githubActor := "dkarlinsky"

lazy val core = project
    .in(file("core"))
  .settings(
    name := "greyhound-core",
    githubActor := "dkarlinsky",
    libraryDependencies ++= Seq(
      "dev.zio" %% "zio" % "1.0.0-RC17",
      "org.apache.kafka" % "kafka-clients" % "1.1.1"
    )
  )

lazy val javaInterop = project
  .in(file("java-interop"))
  .settings(
    name := "greyhound-java",
    githubActor := "dkarlinsky",
  )
  .dependsOn(core, futureInterop)

lazy val futureInterop = project
  .in(file("future-interop"))
  .settings(
    name := "greyhound-scala-futures",
    githubActor := "dkarlinsky",
  )
  .dependsOn(core)
