ThisBuild / scalaVersion := "2.12.16"
ThisBuild / version := "0.1.0-SNAPSHOT"
ThisBuild / organization := "guygo"
ThisBuild / organizationName := "example"

Compile / PB.targets := Seq(
  scalapb.gen(grpc = true) -> (Compile / sourceManaged).value / "scalapb",
  scalapb.zio_grpc.ZioCodeGenerator -> (Compile / sourceManaged).value / "scalapb"
)

lazy val root = (project in file("."))
  .settings(
    name := "greyhound-sidecar",
    libraryDependencies ++= Seq(
      "dev.zio" %% "zio" % "2.0.3",
      "io.grpc" % "grpc-netty" % "1.51.0",
      "com.wix" %% "greyhound-core" % "0.3.0",
      "org.apache.kafka" %% "kafka" % "3.3.1",
      //      "org.apache.kafka" %% "kafka-client" % "3.3.1",
      "com.thesamet.scalapb" %% "scalapb-runtime-grpc" % scalapb.compiler.Version.scalapbVersion,
      "com.thesamet.scalapb" %% "scalapb-runtime" % scalapb.compiler.Version.scalapbVersion,
      "com.thesamet.scalapb" %% "scalapb-runtime" % scalapb.compiler.Version.scalapbVersion % sbtprotoc.ProtocPlugin.ProtobufConfig,
      "org.slf4j" % "slf4j-api" % "1.7.25",
      "ch.qos.logback" % "logback-classic" % "1.1.3",
      "dev.zio" %% "zio-logging-slf4j" % "2.1.3",

      // -- test -- //
      "dev.zio" %% "zio-test" % "2.0.3" % Test,
      "dev.zio" %% "zio-test-junit" % "2.0.3" % Test,
      "dev.zio" %% "zio-test-sbt" % "2.0.3" % Test,
      "dev.zio" %% "zio-test-magnolia" % "2.0.3" % Test,
      "org.specs2" %% "specs2-core" % "4.14.1" % Test,
      "org.apache.curator" % "curator-test" % "5.3.0" % Test,
      "com.wix" %% "greyhound-testkit" % "0.3.0" % Test
    ),
    packageName := "greyhound-sidecar",
    version := "1.0",
    maintainer := "wix.com",
    dockerBaseImage := "openjdk:11.0",
    dockerExposedPorts += 9000,
    Compile / mainClass := Some("greyhound.SidecarServerMain"),
    testFrameworks += new TestFramework("zio.test.sbt.ZTestFramework")
  )
  .enablePlugins(JavaAppPackaging)
  .enablePlugins(DockerPlugin)
