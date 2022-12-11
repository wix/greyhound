addSbtPlugin("com.thesamet" % "sbt-protoc" % "1.0.6")
addSbtPlugin("com.github.sbt" % "sbt-native-packager" % "1.9.9")
//addSbtPlugin("ch.epfl.scala" % "sbt-scalafix" % "0.10.4")


libraryDependencies ++= Seq(
  "com.thesamet.scalapb" %% "compilerplugin" % "0.11.12", //-- maybe this is used for newer zio version?
  "com.thesamet.scalapb.zio-grpc" %% "zio-grpc-codegen" % "0.6.0-test5",
//  "com.thesamet.scalapb.zio-grpc" %% "zio-grpc-codegen" % "0.5.2",
//  "com.thesamet.scalapb.zio-grpc" %% "zio-grpc-core" % "0.6.0-test5"
)
