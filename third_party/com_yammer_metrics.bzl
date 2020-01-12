load("@wix_oss_infra//:import_external.bzl", import_external = "safe_wix_scala_maven_import_external")

def dependencies():

    import_external(
        name = "com_yammer_metrics_metrics_core",
        artifact = "com.yammer.metrics:metrics-core:2.2.0",
        artifact_sha256 = "6b7a14a6f34c10f8683f7b5e2f39df0f07b58c7dff0e468ebbc713905c46979c",
        srcjar_sha256 = "4837a6781690cb1feff50be92f67835e4608ef76dea957ca1323cbdb3f5fd918",
        deps = [
            "@org_slf4j_slf4j_api"
        ],
      # EXCLUDES *:mail
      # EXCLUDES *:jline
      # EXCLUDES *:jms
      # EXCLUDES *:jmxri
      # EXCLUDES *:jmxtools
      # EXCLUDES *:javax
    )
