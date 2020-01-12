load("@wix_oss_infra//:import_external.bzl", import_external = "safe_wix_scala_maven_import_external")

def dependencies():

    import_external(
        name = "com_101tec_zkclient",
        artifact = "com.101tec:zkclient:0.10",
        artifact_sha256 = "26e988b8bba838c724fd8350b331ee8b5ffc59c3a9c074df115c4c3a6c843878",
        srcjar_sha256 = "29e2577b3e45735a825304bb796fc12ede9145e93afd8ada29632d6149a8cb8d",
        deps = [
            "@org_slf4j_slf4j_api"
        ],
      # EXCLUDES *:mail
      # EXCLUDES *:jline
      # EXCLUDES *:jms
      # EXCLUDES *:jmxri
      # EXCLUDES *:jmxtools
      # EXCLUDES *:javax
      # EXCLUDES *:zookeeper
    )
