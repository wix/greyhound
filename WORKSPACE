workspace(name = "greyhound")

load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")
load("@bazel_tools//tools/build_defs/repo:git.bzl", "git_repository")

skylib_version = "0.8.0"

http_archive(
    name = "bazel_skylib",
    sha256 = "2ef429f5d7ce7111263289644d233707dba35e39696377ebab8b0bc701f7818e",
    type = "tar.gz",
    url = "https://github.com/bazelbuild/bazel-skylib/releases/download/0.8.0/bazel-skylib.0.8.0.tar.gz",
)

load("//dependencies/google_protobuf:google_protobuf.bzl", "google_protobuf")

google_protobuf()

load("@greyhound//central-sync:dependencies.bzl", "rules_kotlin", "vaticle_bazel_distribution")

rules_kotlin()

RULES_JVM_EXTERNAL_TAG = "5.2"
RULES_JVM_EXTERNAL_SHA ="f86fd42a809e1871ca0aabe89db0d440451219c3ce46c58da240c7dcdc00125f"

http_archive(
    name = "rules_jvm_external",
    strip_prefix = "rules_jvm_external-%s" % RULES_JVM_EXTERNAL_TAG,
    sha256 = RULES_JVM_EXTERNAL_SHA,
    url = "https://github.com/bazelbuild/rules_jvm_external/releases/download/%s/rules_jvm_external-%s.tar.gz" % (RULES_JVM_EXTERNAL_TAG, RULES_JVM_EXTERNAL_TAG)
)

load("@rules_jvm_external//:repositories.bzl", "rules_jvm_external_deps")

rules_jvm_external_deps()

load("@rules_jvm_external//:setup.bzl", "rules_jvm_external_setup")

rules_jvm_external_setup()

load("@io_bazel_rules_kotlin//kotlin:kotlin.bzl", "kotlin_repositories", "kt_register_toolchains")

kotlin_repositories()

kt_register_toolchains()

vaticle_bazel_distribution()

load("@vaticle_bazel_distribution//maven:deps.bzl", "maven_artifacts_with_versions")

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
