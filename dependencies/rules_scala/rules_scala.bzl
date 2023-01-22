load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")
load("@wix_oss_infra//dependencies/test_network_sandboxing:download_network_sandboxing.bzl", "download_network_sandboxing_according_to_os")

def rules_scala():
    if native.existing_rule("test_network_sandboxing") == None:
        download_network_sandboxing_according_to_os(name = "test_network_sandboxing")

    rules_scala_version = "2ed3a13ed1c30bb097d124cb583752fddbfc2939"

    http_archive(
        name = "io_bazel_rules_scala",
        sha256 = "e501289134b1ee6b36eadd466168f591c11499ea412f2af3d0ee16710474db21",
        strip_prefix = "rules_scala-%s" % rules_scala_version,
        type = "zip",
        url = "https://github.com/bazelbuild/rules_scala/archive/%s.zip" % rules_scala_version,
    )
