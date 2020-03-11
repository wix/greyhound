name := "greyhound"
organization in ThisBuild := "com.wix.greyhound"
scalaVersion in ThisBuild := "2.12.3"

lazy val core = project
    .in(file("core"))
  .settings(
    name := "greyhound-core",
    libraryDependencies ++= Seq(
      "dev.zio" %% "zio" % "1.0.0-RC17",
      "org.apache.kafka" % "kafka-clients" % "1.1.1"
    )
  )

lazy val javaInterop = project
  .in(file("java-interop"))
  .settings(
    name := "greyhound-java",
  )
  .dependsOn(core, futureInterop)

lazy val futureInterop = project
  .in(file("future-interop"))
  .settings(
    name := "greyhound-scala-futures",
  )
  .dependsOn(core)
