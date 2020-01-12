load("@wix_oss_infra//:import_external.bzl", import_external = "safe_wix_scala_maven_import_external")

def dependencies():

    import_external(
        name = "org_apache_zookeeper_zookeeper",
        artifact = "org.apache.zookeeper:zookeeper:3.4.10",
        artifact_sha256 = "caa38ce6b2f52c59c10b80f89abb544cc4279257805fc0c969010cbab1a11079",
        srcjar_sha256 = "15126013736d3a8c24310af9be4f65dd1d9b7c4a2a4c888879ff0bf7b9aa90bb",
        deps = [
            "@org_slf4j_slf4j_api"
        ],
      # EXCLUDES *:netty
      # EXCLUDES *:log4j
      # EXCLUDES *:mail
      # EXCLUDES *:jline
      # EXCLUDES *:slf4j-log4j12
      # EXCLUDES *:jms
      # EXCLUDES *:jmxri
      # EXCLUDES *:jmxtools
      # EXCLUDES *:javax
    )
