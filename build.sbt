name := "greyhound"
organization in ThisBuild := "com.wix.greyhound"
scalaVersion in ThisBuild := "2.12.3"

val publishSettings = Seq(
  publishMavenStyle := true,
  bintrayRepository := "greyhound",
  bintrayOmitLicense := true,
  git.formattedShaVersion := git.gitHeadCommit.value map { sha => s"0.1.0-${sha.slice(0,6)}" }
)
skip in publish := true

lazy val core = project
    .in(file("core"))
  .settings(publishSettings)
  .settings(
    name := "greyhound-core",
    libraryDependencies ++= Seq(
      "dev.zio" %% "zio" % "1.0.0-RC17",
      "org.apache.kafka" % "kafka-clients" % "1.1.1"
    ),
  )
  .enablePlugins(GitVersioning)

lazy val javaInterop = project
  .in(file("java-interop"))
  .settings(publishSettings)
  .settings(
    name := "greyhound-java",
  )
  .dependsOn(core, futureInterop)
  .enablePlugins(GitVersioning)

lazy val futureInterop = project
  .in(file("future-interop"))
  .settings(publishSettings)
  .settings(
    name := "greyhound-scala-futures",
  )
  .dependsOn(core)
  .enablePlugins(GitVersioning)
