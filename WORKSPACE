workspace(name = "greyhound")

load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")
load("@bazel_tools//tools/build_defs/repo:git.bzl", "git_repository")

# TODO: move to wix_oss_infra
skylib_version = "0.8.0"

http_archive(
    name = "bazel_skylib",
    sha256 = "2ef429f5d7ce7111263289644d233707dba35e39696377ebab8b0bc701f7818e",
    type = "tar.gz",
    url = "https://github.com/bazelbuild/bazel-skylib/releases/download/{}/bazel-skylib.{}.tar.gz".format(skylib_version, skylib_version),
)

wix_oss_infra_version = "e1591c6fb45e665418225bbd034d3e5163278aa1"

wix_oss_infra_version_sha256 = "399a68073420a3920069bd8220296df9c3c11f4cb983fd3f6f71e5e8efff8133"

http_archive(
    name = "wix_oss_infra",
    sha256 = wix_oss_infra_version_sha256,
    strip_prefix = "wix-oss-infra-%s" % wix_oss_infra_version,
    url = "https://github.com/wix/wix-oss-infra/archive/%s.tar.gz" % wix_oss_infra_version,
)

load("@wix_oss_infra//dependencies/rules_scala:rules_scala.bzl", "rules_scala")

rules_scala()

load("@wix_oss_infra//test-agent/src/shared:tests_external_repository.bzl", "tests_external_repository")

tests_external_repository(
    name = "tests",
    jdk_version = "11",
)

load("//dependencies/google_protobuf:google_protobuf.bzl", "google_protobuf")

google_protobuf()

# TODO: move to wix_oss_infra
scala_version = "2.12.6"

load("@io_bazel_rules_scala//scala:scala.bzl", "scala_repositories")

scala_repositories((
    scala_version,
    {
        "scala_compiler": "3023b07cc02f2b0217b2c04f8e636b396130b3a8544a8dfad498a19c3e57a863",
        "scala_library": "f81d7144f0ce1b8123335b72ba39003c4be2870767aca15dd0888ba3dab65e98",
        "scala_reflect": "ffa70d522fc9f9deec14358aa674e6dd75c9dfa39d4668ef15bb52f002ce99fa",
    },
))

# TODO: move to wix_oss_infra
load("@io_bazel_rules_scala//specs2:specs2_junit.bzl", "specs2_junit_repositories")

specs2_junit_repositories(scala_version)

register_toolchains("@wix_oss_infra//toolchains:wix_defaults_global_toolchain")

load("@greyhound//central-sync:dependencies.bzl", "graknlabs_bazel_distribution")

graknlabs_bazel_distribution()

RULES_JVM_EXTERNAL_TAG = "3.1"

RULES_JVM_EXTERNAL_SHA = "e246373de2353f3d34d35814947aa8b7d0dd1a58c2f7a6c41cfeaff3007c2d14"

http_archive(
    name = "rules_jvm_external",
    sha256 = RULES_JVM_EXTERNAL_SHA,
    strip_prefix = "rules_jvm_external-%s" % RULES_JVM_EXTERNAL_TAG,
    url = "https://github.com/bazelbuild/rules_jvm_external/archive/%s.zip" % RULES_JVM_EXTERNAL_TAG,
)

load("//:third_party.bzl", "dependencies")

dependencies()

load("@maven//:defs.bzl", "pinned_maven_install")

pinned_maven_install()

load("@maven//:compat.bzl", "compat_repositories")

compat_repositories()

http_archive(
    name = "io_buildbuddy_buildbuddy_toolchain",
    sha256 = "656985ebceaa0116adcae081b19becf41f246893759ac41f86267cb2220f966e",
    strip_prefix = "buildbuddy-toolchain-2a9769e75878519bf48c12334214501d4859154b",
    urls = ["https://github.com/buildbuddy-io/buildbuddy-toolchain/archive/2a9769e75878519bf48c12334214501d4859154b.tar.gz"],
)

load("@io_buildbuddy_buildbuddy_toolchain//:rules.bzl", "register_buildbuddy_toolchain")

register_buildbuddy_toolchain(name = "buildbuddy_toolchain")
