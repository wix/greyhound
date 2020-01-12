load("@wix_oss_infra//:import_external.bzl", import_external = "safe_wix_scala_maven_import_external")

def dependencies():

    import_external(
        name = "org_scala_lang_scala_library",
        artifact = "org.scala-lang:scala-library:2.12.10",
        artifact_sha256 = "0a57044d10895f8d3dd66ad4286891f607169d948845ac51e17b4c1cf0ab569d",
        srcjar_sha256 = "a6f873aeb9b861848e0d0b4ec368a3f1682e33bdf11a82ce26f0bfe5fb197647",
    )


    import_external(
        name = "org_scala_lang_scala_reflect",
        artifact = "org.scala-lang:scala-reflect:2.12.4",
        artifact_sha256 = "ea70fe0e550e24d23fc52a18963b2be9c3b24283f4cb18b98327eb72746567cc",
        srcjar_sha256 = "7b4dc73dc3cb46ac9ac948a0c231ccd989bed6cefb137c302a8ec8d6811e8148",
        deps = [
            "@org_scala_lang_scala_library"
        ],
      # EXCLUDES *:mail
      # EXCLUDES *:jline
      # EXCLUDES *:jms
      # EXCLUDES *:jmxri
      # EXCLUDES *:jmxtools
      # EXCLUDES *:javax
    )
