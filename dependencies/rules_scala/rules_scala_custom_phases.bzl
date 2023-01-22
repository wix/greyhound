load(":custom_phases.bzl", "phase_providers")
load(":scala_meta.bzl", "with_meta")
load("@io_bazel_rules_scala//scala/private:rules/scala_library.bzl", "make_scala_library", "make_scala_macro_library")
load("@io_bazel_rules_scala//scala/private:rules/scala_binary.bzl", "make_scala_binary")
load("@io_bazel_rules_scala//scala/private:rules/scala_junit_test.bzl", "make_scala_junit_test")
load("@io_bazel_rules_scala//scala/private:rules/scala_test.bzl", "make_scala_test")
load(
    "@io_bazel_rules_scala//specs2:specs2_junit.bzl",
    _specs2_junit_dependencies = "specs2_junit_dependencies",
)

# The variable names must match the hardcoded values in
# https://github.com/wix-playground/intellij/blob/6667357d56450980386a40f892fcd7e0fc07efc6/scala/src/com/google/idea/blaze/scala/ScalaBlazeRules.java#L32-L37
# If they don't, Intellij won't recognise Scala targets, and tooling,
# such as debuggers, will stop working
scala_library = make_scala_library(
    phase_providers(["@greyhound//dependencies/rules_scala:dep_tracking_sanitize_scalacopts"]),
)
scala_binary = make_scala_binary(
    phase_providers(["@greyhound//dependencies/rules_scala:dep_tracking_sanitize_scalacopts"]),
)
scala_macro_library = make_scala_macro_library(
    phase_providers(["@greyhound//dependencies/rules_scala:dep_tracking_sanitize_scalacopts"]),
)
scala_test = make_scala_test(
    phase_providers(["@greyhound//dependencies/rules_scala:dep_tracking_sanitize_scalacopts"]),
)
scala_junit_test = make_scala_junit_test(
    phase_providers(["@greyhound//dependencies/rules_scala:dep_tracking_sanitize_scalacopts"]),
)

def scala_specs2_junit_test(name, **kwargs):
    scala_junit_test_with_meta(
        name = name,
        deps = _specs2_junit_dependencies() + kwargs.pop("deps", []),
        unused_dependency_checker_ignored_targets =
            _specs2_junit_dependencies() + kwargs.pop("unused_dependency_checker_ignored_targets", []),
        suite_label = Label(
            "@io_bazel_rules_scala//src/java/io/bazel/rulesscala/specs2:specs2_test_discovery",
        ),
        suite_class = "io.bazel.rulesscala.specs2.Specs2DiscoveredTestSuite",
        **kwargs
    )

def scala_library_with_meta(*args, **kwargs):
    with_meta(scala_library, *args, **kwargs)

def scala_binary_with_meta(*args, **kwargs):
    with_meta(scala_binary, *args, **kwargs)

def scala_junit_test_with_meta(*args, **kwargs):
    with_meta(scala_junit_test, *args, **kwargs)

def scala_macro_library_with_meta(*args, **kwargs):
    with_meta(scala_macro_library, *args, **kwargs)

def scala_test_with_meta(*args, **kwargs):
    with_meta(scala_test, *args, **kwargs)