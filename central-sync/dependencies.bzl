load("@bazel_tools//tools/build_defs/repo:git.bzl", "git_repository")
load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")

def vaticle_bazel_distribution():
    git_repository(
        name = "vaticle_bazel_distribution",
        remote = "https://github.com/vaticle/bazel-distribution",
        commit = "149f581bf41305563b9f15f4186293644215b622",
    )

def rules_kotlin():
    http_archive(
        name = "io_bazel_rules_kotlin",
        urls = ["https://github.com/vaticle/rules_kotlin/archive/c2519b00299cff9df22267e8359784e9948dba67.zip"],
        type = "zip",
        strip_prefix = "rules_kotlin-c2519b00299cff9df22267e8359784e9948dba67",
        sha256 = "1455f2ec4bf7ea12d2c90b0dfd6402553c3bb6cbc0271023e2e01ccdefb4a49a",
    )
