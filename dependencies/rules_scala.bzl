load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")

rules_scala_version="681dedd5b101f689bd8f8730a6958387197ab896" # update this as needed
rules_scala_version_sha256="309105f378c85e12001f6fa4d11ea959211c0f57679dcb12f412ee201326b549"

def rules_scala():
  if native.existing_rule("io_bazel_rules_scala") == None:
      http_archive(
                 name = "io_bazel_rules_scala",
                 url = "https://github.com/bazelbuild/rules_scala/archive/%s.zip"%rules_scala_version,
                 type = "zip",
                 strip_prefix= "rules_scala-%s" % rules_scala_version,
                 sha256 = rules_scala_version_sha256,
      )
