load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")

def rules_scala():

    rules_scala_version = "c96948c77825e3d5ce00b9711bff6c349477e37f"

    http_archive(
        name = "io_bazel_rules_scala",
        sha256 = "bd74d16491c79e9da1cbb2b07a35a0782dd7dfbe5acd8c749225db4e6a665944",
        strip_prefix = "rules_scala-%s" % rules_scala_version,
        type = "zip",
        url = "https://github.com/bazelbuild/rules_scala/archive/%s.zip" % rules_scala_version,
    )
