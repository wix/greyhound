load("//:third_party/org_scala_sbt.bzl", org_scala_sbt_deps = "dependencies")

load("//:third_party/org_hamcrest.bzl", org_hamcrest_deps = "dependencies")

load("//:third_party/junit.bzl", junit_deps = "dependencies")

load("//:third_party/org_portable_scala.bzl", org_portable_scala_deps = "dependencies")

load("//:third_party/dev_zio.bzl", dev_zio_deps = "dependencies")

load("//:third_party/ch_qos_logback.bzl", ch_qos_logback_deps = "dependencies")

load("//:third_party/org_scala_lang_modules.bzl", org_scala_lang_modules_deps = "dependencies")

load("//:third_party/org_xerial_snappy.bzl", org_xerial_snappy_deps = "dependencies")

load("//:third_party/org_slf4j.bzl", org_slf4j_deps = "dependencies")

load("//:third_party/org_lz4.bzl", org_lz4_deps = "dependencies")

load("//:third_party/org_apache_zookeeper.bzl", org_apache_zookeeper_deps = "dependencies")

load("//:third_party/org_apache_kafka.bzl", org_apache_kafka_deps = "dependencies")

load("//:third_party/net_sf_jopt_simple.bzl", net_sf_jopt_simple_deps = "dependencies")

load("//:third_party/com_yammer_metrics.bzl", com_yammer_metrics_deps = "dependencies")

load("//:third_party/com_typesafe_scala_logging.bzl", com_typesafe_scala_logging_deps = "dependencies")

load("//:third_party/com_fasterxml_jackson_core.bzl", com_fasterxml_jackson_core_deps = "dependencies")

load("//:third_party/com_101tec.bzl", com_101tec_deps = "dependencies")

load("//:third_party/org_specs2.bzl", org_specs2_deps = "dependencies")

load("//:third_party/org_scala_lang.bzl", org_scala_lang_deps = "dependencies")

def third_party_dependencies():

  org_scala_lang_deps()


  org_specs2_deps()


  com_101tec_deps()


  com_fasterxml_jackson_core_deps()


  com_typesafe_scala_logging_deps()


  com_yammer_metrics_deps()


  net_sf_jopt_simple_deps()


  org_apache_kafka_deps()


  org_apache_zookeeper_deps()


  org_lz4_deps()


  org_slf4j_deps()


  org_xerial_snappy_deps()


  org_scala_lang_modules_deps()


  ch_qos_logback_deps()


  dev_zio_deps()


  org_portable_scala_deps()


  junit_deps()


  org_hamcrest_deps()


  org_scala_sbt_deps()
