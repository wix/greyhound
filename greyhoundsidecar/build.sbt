ThisBuild / scalaVersion     := "2.12.16"
ThisBuild / version          := "0.1.0-SNAPSHOT"
ThisBuild / organization     := "guygo"
ThisBuild / organizationName := "example"

Compile / PB.targets := Seq(
  scalapb.gen(grpc = true) -> (Compile / sourceManaged).value,
  scalapb.zio_grpc.ZioCodeGenerator -> (Compile / sourceManaged).value
)

lazy val root = (project in file("."))
  .settings(
    name := "greyhound-sidecar",
    libraryDependencies ++= Seq(
      "dev.zio" %% "zio" % "1.0.9",
      "io.grpc" % "grpc-netty" % "1.34.0",
      "com.wix" %% "greyhound-core" % "0.2.0",
      "com.thesamet.scalapb" %% "scalapb-runtime-grpc" % scalapb.compiler.Version.scalapbVersion,
      "com.thesamet.scalapb" %% "scalapb-runtime" % scalapb.compiler.Version.scalapbVersion,
      "com.thesamet.scalapb" %% "scalapb-runtime" % scalapb.compiler.Version.scalapbVersion % sbtprotoc.ProtocPlugin.ProtobufConfig,
      "org.slf4j" % "slf4j-api" % "1.7.25",

      // -- test -- //
      "dev.zio" %% "zio-test" % "1.0.13" % Test,
      "org.specs2" %% "specs2-core" % "4.14.1" % Test,
      "com.wix" %% "greyhound-testkit" % "0.2.0",
    ),
    packageName := "greyhound-sidecar",
    version := "1.0",
    maintainer := "wix.com",
    dockerBaseImage := "openjdk:11.0",
    dockerExposedPorts += 9000,
    Compile / mainClass := Some("greyhound.SidecarServerMain")
//    testFrameworks += new TestFramework("zio.test.sbt.ZTestFramework")
  )
  .enablePlugins(JavaAppPackaging)
  .enablePlugins(DockerPlugin)