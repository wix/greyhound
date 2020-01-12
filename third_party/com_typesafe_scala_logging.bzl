load("@wix_oss_infra//:import_external.bzl", import_external = "safe_wix_scala_maven_import_external")

def dependencies():

    import_external(
        name = "com_typesafe_scala_logging_scala_logging_2_12",
        artifact = "com.typesafe.scala-logging:scala-logging_2.12:3.8.0",
        artifact_sha256 = "ee99e050a2c2743882fc6949708e844feb1184b492019e3ba9c05c253d8e7569",
        srcjar_sha256 = "9e6b3249662bf41e51ebea77a017fe854f8144e73af7a08cd753c194540ca794",
        deps = [
            "@org_scala_lang_scala_library",
            "@org_scala_lang_scala_reflect",
            "@org_slf4j_slf4j_api"
        ],
      # EXCLUDES *:mail
      # EXCLUDES *:jline
      # EXCLUDES *:jms
      # EXCLUDES *:jmxri
      # EXCLUDES *:jmxtools
      # EXCLUDES *:javax
    )
