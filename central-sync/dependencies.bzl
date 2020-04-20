load("@bazel_tools//tools/build_defs/repo:git.bzl", "git_repository")

def graknlabs_bazel_distribution():
    git_repository(
        name = "graknlabs_bazel_distribution",
        remote = "https://github.com/natansil/bazel-distribution",
        commit = "ac45096de78bd7f936a6f51fbc9cc8b957be68c4"
    )