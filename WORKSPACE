workspace(name = "greyhound")

load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")

wix_oss_infra_version = "e1591c6fb45e665418225bbd034d3e5163278aa1"

wix_oss_infra_version_sha256 = "399a68073420a3920069bd8220296df9c3c11f4cb983fd3f6f71e5e8efff8133"

http_archive(
    name = "wix_oss_infra",
    sha256 = wix_oss_infra_version_sha256,
    strip_prefix = "wix-oss-infra-%s" % wix_oss_infra_version,
    url = "https://github.com/wix/wix-oss-infra/archive/%s.tar.gz" % wix_oss_infra_version,
)

http_archive(
    name = "rules_cc",
    sha256 = "29daf0159f0cf552fcff60b49d8bcd4f08f08506d2da6e41b07058ec50cfeaec",
    strip_prefix = "rules_cc-b7fe9697c0c76ab2fd431a891dbb9a6a32ed7c3e",
    urls = ["https://github.com/bazelbuild/rules_cc/archive/b7fe9697c0c76ab2fd431a891dbb9a6a32ed7c3e.tar.gz"],
)

skylib_version = "1.0.3"

http_archive(
    name = "bazel_skylib",
    sha256 = "1c531376ac7e5a180e0237938a2536de0c54d93f5c278634818e0efc952dd56c",
    type = "tar.gz",
    url = "https://mirror.bazel.build/github.com/bazelbuild/bazel-skylib/releases/download/{}/bazel-skylib-{}.tar.gz".format(skylib_version, skylib_version),
)

load("//dependencies:rules_scala.bzl", "rules_scala")

rules_scala()

# Stores Scala version and other configuration
# 2.12 is a default version, other versions can be use by passing them explicitly:
# scala_config(scala_version = "2.11.12")

scala_version = "2.12.12"

load("@io_bazel_rules_scala//:scala_config.bzl", "scala_config")

scala_config(scala_version = scala_version)

load("@io_bazel_rules_scala//scala:scala.bzl", "scala_repositories")

scala_repositories()

load("//dependencies:rules_proto.bzl", "rules_proto")

rules_proto()

load("@rules_proto//proto:repositories.bzl", "rules_proto_dependencies", "rules_proto_toolchains")

rules_proto_dependencies()

load("//dependencies:google_protobuf.bzl", "google_protobuf")

google_protobuf()

register_toolchains("//:scala_toolchain")

load("@wix_oss_infra//test-agent/src/shared:tests_external_repository.bzl", "tests_external_repository")

tests_external_repository(
    name = "tests",
    jdk_version = "11",
)

load("@io_bazel_rules_scala//testing:specs2_junit.bzl", "specs2_junit_repositories", "specs2_junit_toolchain")

specs2_junit_repositories()

specs2_junit_toolchain()

register_toolchains("//:testing_toolchain")

load("@greyhound//central-sync:dependencies.bzl", "graknlabs_bazel_distribution")

graknlabs_bazel_distribution()

RULES_JVM_EXTERNAL_TAG = "4.2"

RULES_JVM_EXTERNAL_SHA = "cd1a77b7b02e8e008439ca76fd34f5b07aecb8c752961f9640dea15e9e5ba1ca"

http_archive(
    name = "rules_jvm_external",
    sha256 = RULES_JVM_EXTERNAL_SHA,
    strip_prefix = "rules_jvm_external-%s" % RULES_JVM_EXTERNAL_TAG,
    url = "https://github.com/bazelbuild/rules_jvm_external/archive/%s.zip" % RULES_JVM_EXTERNAL_TAG,
)

load("//:third_party.bzl", "dependencies")

dependencies()

#load("@maven//:defs.bzl", "pinned_maven_install")
#
#pinned_maven_install()

load("@maven//:compat.bzl", "compat_repositories")

compat_repositories()

http_archive(
    name = "io_buildbuddy_toolchain",
    sha256 = "d00a2ba3aa689cced99fcfe4442a7ee7741ce6de744e4c0ccde2cfe47c5aa86f",
    strip_prefix = "toolchain-6a50799da26f34e9aab6b85dc187aa7fed4b127c",
    urls = ["https://github.com/buildbuddy-io/toolchain/archive/6a50799da26f34e9aab6b85dc187aa7fed4b127c.tar.gz"],
)

load("@io_buildbuddy_toolchain//:rules.bzl", "register_buildbuddy_toolchain")

register_buildbuddy_toolchain(name = "buildbuddy_toolchain")
