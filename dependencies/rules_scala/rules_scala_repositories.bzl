load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")
load("@io_bazel_rules_scala//:scala_config.bzl", "scala_config")

def rules_scala_repositories(scala_version):
    scala_config(scala_version, enable_compiler_dependency_tracking = True)

    # Using this and not the bazel regular one due to issue when classpath is too long
    # until https://github.com/bazelbuild/bazel/issues/6955 is resolved
    if native.existing_rule("java_stub_template") == None:
        http_archive(
            name = "java_stub_template",
            sha256 = "48b65607ef8c61d7d663b1d03215616c665443906bb0f14c5a9e7f5e131349e1",
            urls = ["https://github.com/wix/rules_scala/archive/7033d11e43038f194b2e2c60ceecd4aa800aecc5.tar.gz"],
            strip_prefix = "rules_scala-7033d11e43038f194b2e2c60ceecd4aa800aecc5/java_stub_template",
        )
    
    patch = "@io_bazel_rules_scala//dt_patches:dt_compiler_2.12.patch"

    http_archive(
        name = "scala_compiler_source",
        build_file_content = "\n".join([
            "package(default_visibility = [\"//visibility:public\"])",
            "filegroup(",
            "    name = \"src\",",
            "    srcs=[\"scala/tools/nsc/symtab/SymbolLoaders.scala\"],",
            ")",
        ]),
        patches = [patch],
        url = "https://repo1.maven.org/maven2/org/scala-lang/scala-compiler/%s/scala-compiler-%s-sources.jar" % (scala_version, scala_version),
    )


JUNIT_DEPS = [
    "@greyhound//dependencies/rules_scala:junit",
]

SPECS2_DEPS = [
    "@greyhound//dependencies/rules_scala:specs2",
]

SPECS2_JUNIT_DEPS = [
    "@greyhound//dependencies/rules_scala:specs2_junit",
]

TESTING_DEPS = JUNIT_DEPS + SPECS2_DEPS + SPECS2_JUNIT_DEPS
