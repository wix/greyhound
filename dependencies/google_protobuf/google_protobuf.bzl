load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")
load("@bazel_tools//tools/build_defs/repo:utils.bzl", "maybe")

_PROTOC_BINARIES = {
    "com_google_protobuf_protoc_linux_x86_64": {
        "sha256": "0d13805474b85611c74f00c738a85ad00a25828dbf6e501de29d7f56b3dbcc03",
        "url": "https://github.com/protocolbuffers/protobuf/releases/download/v21.10/protoc-21.10-linux-x86_64.zip",
    },
    "com_google_protobuf_protoc_macos_x86_64": {
        "sha256": "96126be6f421b2417e54cd4cf79afeea98a4ca035fa39fa2bd7bf29e6c5afe0b",
        "url": "https://github.com/protocolbuffers/protobuf/releases/download/v21.10/protoc-21.10-osx-x86_64.zip",
    },
    "com_google_protobuf_protoc_macos_arm64": {
        "sha256": "dfa3e0a72f7eeec8c0a52de82aa4846ec06a784975c91849e264891f279fdddf",
        "url": "https://github.com/protocolbuffers/protobuf/releases/download/v21.10/protoc-21.10-osx-aarch_64.zip",
    },
}

def google_protobuf():
    """
    Defines a @com_google_protobuf repository which points to precompiled protoc.
    Repository acts as a proxy (contains only aliases) to platform specific binary in com_google_protobuf_protoc_OS_CPU.
    Correct binary repository is selected by host/execution platform.
    Rationale of this is to reduce dependencies brought by @rules_proto as it tries to target languages we don't need.
    """

    for name, args in _PROTOC_BINARIES.items():
        http_archive(
            name = name,
            build_file = "//dependencies/google_protobuf:BUILD.binary",
            **args
        )

    _protoc_proxy(name = "com_google_protobuf")

    # Care should be taken when updating this dependency as it uses own BUILD files.
    # Current version contains only proto_library and java_proto_library definitions.
    # Later versions adds cc_proto_library, py_proto_library and so on.
    # It might introduce unexpected dependencies to the build graph.
    maybe(
        repo_rule = http_archive,
        name = "google_api_protos",
        urls = ["https://github.com/googleapis/googleapis/archive/c911062bb7a1c41a208957bed923b8750f3b6f28.zip"],
        strip_prefix = "googleapis-c911062bb7a1c41a208957bed923b8750f3b6f28",
        sha256 = "9ecae675e5f169f93bf07d8ff80b51444c8eb57b10beba2e0657a317bdf378a4",
    )

def _protoc_proxy_impl(ctx):
    ctx.symlink(ctx.attr._build, "BUILD")

_protoc_proxy = repository_rule(
    implementation = _protoc_proxy_impl,
    attrs = {
        "_build": attr.label(default = "//dependencies/google_protobuf:BUILD.proxy"),
    },
)
