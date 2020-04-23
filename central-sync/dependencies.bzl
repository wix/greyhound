load("@bazel_tools//tools/build_defs/repo:git.bzl", "git_repository")

def graknlabs_bazel_distribution():
    git_repository(
        name = "graknlabs_bazel_distribution",
        remote = "https://github.com/natansil/bazel-distribution",
        commit = "4d07cdbb4dfaf3059e14ab8815f15b036c1b8cf3"
    )