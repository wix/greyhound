addSbtPlugin("com.thesamet" % "sbt-protoc" % "1.0.6")
addSbtPlugin("com.github.sbt" % "sbt-native-packager" % "1.9.9")

libraryDependencies ++= Seq(
  "com.thesamet.scalapb.zio-grpc" %% "zio-grpc-codegen" % "0.4.2",
//  "com.thesamet.scalapb" %% "compilerplugin" % "0.11.11", -- maybe this is used for newer zio version?
)
