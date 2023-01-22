load("@greyhound//toolchains:jdk_repos.bzl", "repo_jdk_version")

def _write_tests_macro(repository_ctx):

    setups = [repository_ctx.path(t) for t in repository_ctx.attr._agent_setups]
    setup = [t for t in setups if t.basename == "jdk%s_agent_setup.bzl" % repository_ctx.attr.jdk_version]

    if not setup:
        fail("Failed to find agent setup for jdk %s" % repository_ctx.attr.jdk_version)

    repository_ctx.symlink(repository_ctx.path(repository_ctx.attr._tests_macro), "tests.bzl")
    repository_ctx.symlink(setup[0], "agent_setup.bzl")

    repository_ctx.file("BUILD.bazel", "")
    repository_ctx.file("WORKSPACE", "")


tests_external_repository = repository_rule(
    implementation = _write_tests_macro,
    attrs = {
        "jdk_version": attr.string(default = repo_jdk_version),
        "_tests_macro": attr.label(default = "//test-agent/src/shared:tests.bzl"),
        "_agent_setups": attr.label_list(default = [
            "//test-agent/src/shared:jdk11_agent_setup.bzl"
        ]),
    }
)