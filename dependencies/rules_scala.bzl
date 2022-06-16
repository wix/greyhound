load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")
load("@wix_oss_infra//dependencies/test_network_sandboxing:download_network_sandboxing.bzl", "download_network_sandboxing_according_to_os")

rules_scala_version = "088c0878a836682bfd95287299613fdfab208e2f"  # update this as needed
rules_scala_version_sha256 = "dafb6732efb9252f187e2e27ae36ef04d2d5f7fbf53683b0b422c63358223962"

def rules_scala():
    if native.existing_rule("test_network_sandboxing") == None:
        download_network_sandboxing_according_to_os(name = "test_network_sandboxing")

    if native.existing_rule("io_bazel_rules_scala") == None:
        http_archive(
            name = "io_bazel_rules_scala",
            url = "https://github.com/bazelbuild/rules_scala/archive/%s.tar.gz" % rules_scala_version,
            type = "tar.gz",
            strip_prefix = "rules_scala-%s" % rules_scala_version,
            sha256 = rules_scala_version_sha256,
        )
