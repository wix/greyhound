load("@wix_oss_infra//:import_external.bzl", import_external = "safe_wix_scala_maven_import_external")

def dependencies():

    import_external(
        name = "org_apache_kafka_kafka_2_12",
        artifact = "org.apache.kafka:kafka_2.12:1.1.1",
        artifact_sha256 = "d7b77e3b150519724d542dfb5da1584b9cba08fb1272ff1e3b3d735937e22632",
        srcjar_sha256 = "2a1a9ed91ad065bf62be64dbb4fd5e552ff90c42fc07e67ace4f82413304c3dd",
        deps = [
            "@com_101tec_zkclient",
            "@com_fasterxml_jackson_core_jackson_databind",
            "@com_typesafe_scala_logging_scala_logging_2_12",
            "@com_yammer_metrics_metrics_core",
            "@net_sf_jopt_simple_jopt_simple",
            "@org_apache_kafka_kafka_clients",
            "@org_apache_zookeeper_zookeeper",
            "@org_scala_lang_scala_library",
            "@org_scala_lang_scala_reflect",
            "@org_slf4j_slf4j_api"
        ],
    )


    import_external(
        name = "org_apache_kafka_kafka_clients",
        artifact = "org.apache.kafka:kafka-clients:1.1.1",
        artifact_sha256 = "a92dd1c76be061ad40e1d2421c8a6a3f148c6c3b82af2f2259ad40e7c400d7bc",
        srcjar_sha256 = "cb7e491fcfd41f2bc9806bd8f583f370fe4b1ef6c4ec378957e45574881be9ae",
        deps = [
            "@org_lz4_lz4_java",
            "@org_slf4j_slf4j_api",
            "@org_xerial_snappy_snappy_java"
        ],
      # EXCLUDES *:mail
      # EXCLUDES *:jline
      # EXCLUDES *:jms
      # EXCLUDES *:jmxri
      # EXCLUDES *:jmxtools
      # EXCLUDES *:javax
    )
