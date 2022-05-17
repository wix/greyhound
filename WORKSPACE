workspace(name = "greyhound")

load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")
load("@bazel_tools//tools/build_defs/repo:git.bzl","git_repository")

# TODO: move to wix_oss_infra
skylib_version = "1.0.3"
http_archive(
    name = "bazel_skylib",
    type = "tar.gz",
    url = "https://github.com/bazelbuild/bazel-skylib/releases/download/{}/bazel-skylib.{}.tar.gz".format (skylib_version, skylib_version),
    sha256 = "1c531376ac7e5a180e0237938a2536de0c54d93f5c278634818e0efc952dd56c",
)

wix_oss_infra_version="e1591c6fb45e665418225bbd034d3e5163278aa1"
wix_oss_infra_version_sha256="399a68073420a3920069bd8220296df9c3c11f4cb983fd3f6f71e5e8efff8133"

http_archive(
    name = "wix_oss_infra",
    url = "https://github.com/wix/wix-oss-infra/archive/%s.tar.gz" % wix_oss_infra_version,
    strip_prefix = "wix-oss-infra-%s" % wix_oss_infra_version,
     sha256 = wix_oss_infra_version_sha256,
)

rules_scala_version = "5df8033f752be64fbe2cedfd1bdbad56e2033b15"

http_archive(
    name = "io_bazel_rules_scala",
    sha256 = "b7fa29db72408a972e6b6685d1bc17465b3108b620cb56d9b1700cf6f70f624a",
    strip_prefix = "rules_scala-%s" % rules_scala_version,
    type = "zip",
    url = "https://github.com/bazelbuild/rules_scala/archive/%s.zip" % rules_scala_version,
)
load("@wix_oss_infra//test-agent/src/shared:tests_external_repository.bzl", "tests_external_repository")
tests_external_repository(name = "tests", jdk_version="11")

load("@wix_oss_infra//dependencies/google_protobuf:google_protobuf.bzl", "google_protobuf")
google_protobuf()


load("@io_bazel_rules_scala//:scala_config.bzl", "scala_config")
scala_config(scala_version = "2.13.6")

load("@io_bazel_rules_scala//scala:scala.bzl", "scala_repositories")
scala_repositories()

# TODO: move to wix_oss_infra
load("@io_bazel_rules_scala//specs2:specs2_junit.bzl", "specs2_junit_repositories")
specs2_junit_repositories(scala_version)

load("@io_bazel_rules_scala//scala:toolchains.bzl", "scala_register_toolchains")
scala_register_toolchains()

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
