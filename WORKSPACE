workspace(name = "greyhound")

load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")
load("@bazel_tools//tools/build_defs/repo:git.bzl", "git_repository")

skylib_version = "0.8.0"

http_archive(
    name = "bazel_skylib",
    sha256 = "2ef429f5d7ce7111263289644d233707dba35e39696377ebab8b0bc701f7818e",
    type = "tar.gz",
    url = "https://github.com/bazelbuild/bazel-skylib/releases/download/{}/bazel-skylib.{}.tar.gz".format(skylib_version, skylib_version),
)

load("//dependencies/google_protobuf:google_protobuf.bzl", "google_protobuf")

google_protobuf()

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

load("//dependencies/rules_scala:rules_scala.bzl", "rules_scala")

rules_scala()

load("@io_bazel_rules_scala//:scala_config.bzl", "scala_config")

scala_config()

# Following binds are needed because test macros refer to rules_scala internal labels
bind(
    name = "io_bazel_rules_scala/dependency/junit/junit",
    actual = "@maven//:junit_junit",
)

bind(
    name = "io_bazel_rules_scala/dependency/hamcrest/hamcrest_core",
    actual = "@maven//:org_hamcrest_hamcrest_core",
)

register_toolchains(
    "//dependencies/rules_scala:scala_toolchain",
    "//dependencies/rules_scala:scala_test_toolchain",
)

http_archive(
    name = "io_buildbuddy_buildbuddy_toolchain",
    sha256 = "656985ebceaa0116adcae081b19becf41f246893759ac41f86267cb2220f966e",
    strip_prefix = "buildbuddy-toolchain-2a9769e75878519bf48c12334214501d4859154b",
    urls = ["https://github.com/buildbuddy-io/buildbuddy-toolchain/archive/2a9769e75878519bf48c12334214501d4859154b.tar.gz"],
)

load("@io_buildbuddy_buildbuddy_toolchain//:deps.bzl", "buildbuddy_deps")

buildbuddy_deps()

load("@io_buildbuddy_buildbuddy_toolchain//:rules.bzl", "buildbuddy")

buildbuddy(name = "buildbuddy_toolchain")
