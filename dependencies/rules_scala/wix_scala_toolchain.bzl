load("@io_bazel_rules_scala//scala:scala_toolchain.bzl", "scala_toolchain")
load("@io_bazel_rules_scala//scala:providers.bzl", "declare_deps_provider")
load("@io_bazel_rules_scala_config//:config.bzl", "SCALA_MAJOR_VERSION", "SCALA_VERSION")

_SCALA_DEP_PROVIDERS = [
    "@greyhound//dependencies/rules_scala:scala_xml_provider",
    "@greyhound//dependencies/rules_scala:parser_combinators_provider",
    "@greyhound//dependencies/rules_scala:scala_compile_classpath_provider",
    "@greyhound//dependencies/rules_scala:scala_library_classpath_provider",
    "@greyhound//dependencies/rules_scala:scala_macro_classpath_provider",
]

_SCALAC_OPTS = [
    "-unchecked",
    "-deprecation",
    "-feature",
] + (
    [
        "-Xmax-classfile-name",
        "240",
        "-Ywarn-unused-import",
    ] if SCALA_MAJOR_VERSION == "2.12" else []
) + [
    "-Ywarn-unused",
    "-Ywarn-macros:after",
]

_SCALAC_JVM_FLAGS = [
    "-Xss2m",
]

DEFAULT_STRICT_DEPS_MODE = "warn"

def wix_scala_toolchain(
        name,
        strict_deps_mode = "off",
        compiler_deps_mode = "off",
        unused_dependency_checker_mode = "off",
        dependency_tracking_strict_deps_patterns = ["//"],
        dependency_tracking_unused_deps_patterns = ["//"],
        dependency_tracking_method = "ast",
        visibility = ["//visibility:public"]):
    scala_toolchain(
        name = name,
        dep_providers = _SCALA_DEP_PROVIDERS,
        scalacopts = _SCALAC_OPTS,
        scalac_jvm_flags = _SCALAC_JVM_FLAGS,
        dependency_mode = "plus-one",
        dependency_tracking_method = dependency_tracking_method,
        compiler_deps_mode = compiler_deps_mode,
        strict_deps_mode = strict_deps_mode,
        dependency_tracking_strict_deps_patterns = dependency_tracking_strict_deps_patterns,
        unused_dependency_checker_mode = unused_dependency_checker_mode,
        dependency_tracking_unused_deps_patterns = dependency_tracking_unused_deps_patterns,
        enable_stats_file = False,
        visibility = visibility,
    )
