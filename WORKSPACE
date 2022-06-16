workspace(name = "greyhound")

load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")
load("@bazel_tools//tools/build_defs/repo:git.bzl","git_repository")

skylib_version = "0.8.0"
http_archive(
    name = "bazel_skylib",
    type = "tar.gz",
    url = "https://github.com/bazelbuild/bazel-skylib/releases/download/{}/bazel-skylib.{}.tar.gz".format (skylib_version, skylib_version),
    sha256 = "2ef429f5d7ce7111263289644d233707dba35e39696377ebab8b0bc701f7818e",
)

load("//dependencies:rules_scala.bzl", "rules_scala")
rules_scala()
register_toolchains("//:scala_toolchain")
# Stores Scala version and other configuration
# 2.12 is a default version, other versions can be use by passing them explicitly:
# scala_config(scala_version = "2.11.12")
scala_version = "2.12.6"

load("@io_bazel_rules_scala//:scala_config.bzl", "scala_config")
scala_config(scala_version)

load("@io_bazel_rules_scala//scala:scala.bzl", "scala_repositories")
scala_repositories()



load("//dependencies:rules_proto.bzl", "rules_proto")
rules_proto()

load("@rules_proto//proto:repositories.bzl", "rules_proto_dependencies", "rules_proto_toolchains")

rules_proto_dependencies()

rules_proto_toolchains()




load("//test-agent/src/shared:tests_external_repository.bzl", "tests_external_repository")
tests_external_repository(name = "tests", jdk_version="11")

load("//dependencies:google_protobuf.bzl", "google_protobuf")
google_protobuf()



#load("@io_bazel_rules_scala//testing:specs2_junit.bzl", "specs2_junit_repositories", "specs2_junit_toolchain")
#specs2_junit_repositories()
#specs2_junit_toolchain()

register_toolchains("//:testing_toolchain")

load("@greyhound//central-sync:dependencies.bzl", "graknlabs_bazel_distribution")
graknlabs_bazel_distribution()

RULES_JVM_EXTERNAL_TAG = "3.1"
RULES_JVM_EXTERNAL_SHA = "e246373de2353f3d34d35814947aa8b7d0dd1a58c2f7a6c41cfeaff3007c2d14"

http_archive(
    name = "rules_jvm_external",
    strip_prefix = "rules_jvm_external-%s" % RULES_JVM_EXTERNAL_TAG,
    sha256 = RULES_JVM_EXTERNAL_SHA,
    url = "https://github.com/bazelbuild/rules_jvm_external/archive/%s.zip" % RULES_JVM_EXTERNAL_TAG,
)

load("//:third_party.bzl", "dependencies")
dependencies()

load("@maven//:defs.bzl", "pinned_maven_install")
pinned_maven_install()

load("@maven//:compat.bzl", "compat_repositories")
compat_repositories()

http_archive(
    name = "io_buildbuddy_toolchain",
    strip_prefix = "toolchain-6a50799da26f34e9aab6b85dc187aa7fed4b127c",
    urls = ["https://github.com/buildbuddy-io/toolchain/archive/6a50799da26f34e9aab6b85dc187aa7fed4b127c.tar.gz"],
    sha256 = "d00a2ba3aa689cced99fcfe4442a7ee7741ce6de744e4c0ccde2cfe47c5aa86f",
)

load("@io_buildbuddy_toolchain//:rules.bzl", "register_buildbuddy_toolchain")

register_buildbuddy_toolchain(name = "buildbuddy_toolchain")
