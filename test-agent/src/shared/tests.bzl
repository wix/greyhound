load("@greyhound//dependencies/rules_scala:rules_scala_custom_phases.bzl", "scala_library_with_meta")
load(
    "@greyhound//dependencies/rules_scala:rules_scala_dt_off.bzl",
    "scala_library_dt_off",
    "scala_specs2_junit_test_dt_off",
)
load(
    "@greyhound//dependencies/rules_scala:rules_scala_custom_phases.bzl",
    scala_specs2_junit_test_customized = "scala_specs2_junit_test",
)
load("@test_network_sandboxing//:network_sandboxing.bzl", "network_sandboxing")
load("@greyhound//dependencies/rules_scala:rules_scala_repositories.bzl", "TESTING_DEPS")
load("@greyhound//toolchains:jdk_repos.bzl", "repo_jdk_version")
load("//test-agent/src/shared:jdk11_agent_setup.bzl", "agent_setup_flags")

target_test_classes = "target/test-classes"

_unit_prefixes = ["Test"]
_unit_suffixes = _unit_prefixes
_unit_tags = ["UT"]

_it_prefixes = ["IT", "E2E"]
_it_suffixes = _it_prefixes
_it_tags = ["IT", "E2E"]

_mixed_prefixes = _unit_prefixes + _it_prefixes
_mixed_suffixes = _mixed_prefixes
_mixed_tags = _unit_tags + _it_tags

def specs2_unit_test(
        extra_runtime_dirs = [target_test_classes],
        extra_runtime_entries = [target_test_classes],
        **kwargs):
    size = kwargs.pop("size", "small")
    rbe_pool = kwargs.pop("rbe_pool", None)
    rbe_docker_user = kwargs.pop("rbe_docker_user", None)

    # TODO: Consider not enabling the rbe_docker_host_networking attribute on all tests
    #       Action item for future - disable this flag and find all tests that are failing
    #       and add it explicitly for each and every one of them
    rbe_docker_host_networking = kwargs.pop("rbe_docker_host_networking", True)

    timeout = kwargs.pop("timeout", None)
    rbe_allow_network_and_cause_flakiness = kwargs.pop("rbe_allow_network_and_cause_flakiness", True)
    _add_test_target(
        _unit_prefixes,
        _unit_suffixes,
        _unit_tags,
        True,
        size,
        timeout,
        extra_runtime_dirs,
        extra_runtime_entries,
        rbe_allow_network_and_cause_flakiness,
        rbe_pool,
        rbe_docker_user,
        rbe_docker_host_networking,
        **kwargs
    )

def specs2_ite2e_test(
        block_network = True,
        extra_runtime_dirs = [target_test_classes],
        extra_runtime_entries = [target_test_classes],
        **kwargs):
    timeout = kwargs.pop("timeout", _default_moderate_timeout_or_implied_from_size_attr(kwargs))
    size = kwargs.pop("size", "large")
    rbe_pool = kwargs.pop("rbe_pool", None)
    rbe_docker_user = kwargs.pop("rbe_docker_user", None)

    # TODO: Consider not enabling the rbe_docker_host_networking attribute on all tests
    #       Action item for future - disable this flag and find all tests that are failing
    #       and add it explicitly for each and every one of them
    rbe_docker_host_networking = kwargs.pop("rbe_docker_host_networking", True)

    rbe_allow_network_and_cause_flakiness = kwargs.pop("rbe_allow_network_and_cause_flakiness", True)
    _add_test_target(
        _it_prefixes,
        _it_suffixes,
        _it_tags,
        block_network,
        size,
        timeout,
        extra_runtime_dirs,
        extra_runtime_entries,
        rbe_allow_network_and_cause_flakiness,
        rbe_pool,
        rbe_docker_user,
        rbe_docker_host_networking,
        **kwargs
    )

def specs2_mixed_test(
        block_network = True,
        extra_runtime_dirs = [target_test_classes],
        extra_runtime_entries = [target_test_classes],
        **kwargs):
    timeout = kwargs.pop("timeout", _default_moderate_timeout_or_implied_from_size_attr(kwargs))
    size = kwargs.pop("size", "large")
    rbe_pool = kwargs.pop("rbe_pool", None)
    rbe_docker_user = kwargs.pop("rbe_docker_user", None)

    # TODO: Consider not enabling the rbe_docker_host_networking attribute on all tests
    #       Action item for future - disable this flag and find all tests that are failing
    #       and add it explicitly for each and every one of them
    rbe_docker_host_networking = kwargs.pop("rbe_docker_host_networking", True)

    rbe_allow_network_and_cause_flakiness = kwargs.pop("rbe_allow_network_and_cause_flakiness", True)
    _add_test_target(
        _mixed_prefixes,
        _mixed_suffixes,
        _mixed_tags,
        block_network,
        size,
        timeout,
        extra_runtime_dirs,
        extra_runtime_entries,
        rbe_allow_network_and_cause_flakiness,
        rbe_pool,
        rbe_docker_user,
        rbe_docker_host_networking,
        **kwargs
    )

def _add_test_target(
        prefixes,
        suffixes,
        test_tags,
        block_network,
        size,
        timeout,
        extra_runtime_dirs,
        extra_runtime_entries,
        rbe_allow_network_and_cause_flakiness,
        rbe_pool,
        rbe_docker_user,
        rbe_docker_host_networking,
        **kwargs):
    #extract attribute(s) common to both test and scala_library
    name = kwargs.pop("name")
    user_test_tags = kwargs.pop("tags", test_tags)

    #Bazel idiomatic wise `data` is needed in both.
    #(scala_library for other tests that might need the data in the runfiles and the test needs it so that it can do $location expansion)
    data = kwargs.pop("data", [])[:]

    #extract attributes which are only for the test runtime
    end_prefixes = kwargs.pop("prefixes", prefixes)
    end_suffixes = kwargs.pop("suffixes", suffixes)
    jvm_flags = kwargs.pop("jvm_flags", [])
    flaky = kwargs.pop("flaky", None)
    shard_count = kwargs.pop("shard_count", None)
    args = kwargs.pop("args", None)
    local = kwargs.pop("local", None)
    deps = kwargs.pop("deps", [])

    data.append("@greyhound//test-agent/src/main/java/com/wixpress/agent:test-agent_deploy.jar")

    agent_flags = agent_setup_flags(extra_runtime_dirs, extra_runtime_entries)
    jvm_flags.extend(agent_flags)
    jvm_flags.extend([
        "-Dcom.google.testing.junit.runner.shouldInstallTestSecurityManager=false",
        # read by wix testing framework to support different test environments 'CI' serves as a default.
        "-Dwix.environment=CI",
    ])
    #jvm_flags.extend(["-Ddummy.invalidate=cache"])

    if repo_jdk_version == "11":
        jvm_flags.extend(["-Djava.locale.providers=COMPAT,SPI,CLDR"])

    #mitigate issue where someone explicitly adds testonly in their kwargs and so we get it twice
    testonly = kwargs.pop("testonly", 1)

    unused_dependency_checker_ignored_targets = kwargs.pop("unused_dependency_checker_ignored_targets", [])

    # Cycle to avoid duplicates, TODO: fix repos and use simple extend
    for dep in TESTING_DEPS:
        if dep not in unused_dependency_checker_ignored_targets:
            unused_dependency_checker_ignored_targets.append(dep)

    dep_tracking_on = kwargs.pop("dep_tracking_on", True)

    library_rule, test_rule = [
        scala_library_with_meta,
        scala_specs2_junit_test_customized,
    ] if dep_tracking_on else [
        scala_library_dt_off,
        scala_specs2_junit_test_dt_off,
    ]

    tests_from = kwargs.pop("tests_from", ":" + name)

    library_rule(
        name = name,
        tags = user_test_tags,
        data = data,
        testonly = testonly,
        deps = TESTING_DEPS + deps,
        unused_dependency_checker_ignored_targets = unused_dependency_checker_ignored_targets,
        **kwargs
    )
    test_rule(
        name = name + "_test_runner",
        prefixes = end_prefixes,
        suffixes = end_suffixes,
        deps = deps,
        runtime_deps = [":" + name],
        tests_from = [tests_from],
        jvm_flags = jvm_flags,
        size = size,
        timeout = timeout,
        flaky = flaky,
        shard_count = shard_count,
        args = args,
        local = local,
        data = data,
        tags = _test_tags(user_test_tags, block_network),
        exec_compatible_with = generate_exec_compatible_with_constraints(
            size = size,
            rbe_pool = rbe_pool,
            rbe_docker_user = rbe_docker_user,
            rbe_docker_host_networking = rbe_docker_host_networking,
            rbe_allow_network_and_cause_flakiness = rbe_allow_network_and_cause_flakiness,
        ),
    )

def _constraint_by_size(size):
    if size in ["large", "enormous"]:
        return ["@greyhound//platforms_common:machine_size_large"]
    else:
        return ["@greyhound//platforms_common:machine_size_small"]

def _constraint_by_rbe_pool(rbe_pool):
    if rbe_pool in ["firecracker"]:
        return ["@greyhound//platforms_common:rbe_pool_firecracker"]
    else:
        return ["@greyhound//platforms_common:rbe_pool_generic"]

def _constraint_by_rbe_docker_user(rbe_docker_user):
    if rbe_docker_user in ["nobody"]:
        return ["@greyhound//platforms_common:docker_user_privileged"]
    else:
        return ["@greyhound//platforms_common:docker_user_unprivileged"]

def _constraint_by_rbe_docker_host_networking(rbe_docker_host_networking):
    if rbe_docker_host_networking:
        return ["@greyhound//platforms_common:host_docker_networking_mode_enabled"]
    else:
        return ["@greyhound//platforms_common:host_docker_networking_mode_disabled"]

def _constraint_by_network(rbe_allow_network_and_cause_flakiness):
    if rbe_allow_network_and_cause_flakiness:
        return ["@greyhound//platforms_common:network_mode_enabled"]
    else:
        return ["@greyhound//platforms_common:network_mode_disabled"]

def _test_tags(test_tags, block_network):
    tags = []
    if (block_network):
        tags = network_sandboxing

    return tags + test_tags

def _default_moderate_timeout_or_implied_from_size_attr(kwargs):
    if "size" in kwargs:
        #let bazel imply timeout from the size
        default_timeout = None
    else:
        default_timeout = "moderate"

    return default_timeout

def generate_exec_compatible_with_constraints(
        size = None,
        rbe_allow_network_and_cause_flakiness = True,
        rbe_pool = None,
        rbe_docker_user = None,
        rbe_docker_host_networking = True):
    result = []
    result.extend(_constraint_by_size(size))
    result.extend(_constraint_by_network(rbe_allow_network_and_cause_flakiness))
    result.extend(_constraint_by_rbe_pool(rbe_pool))
    result.extend(_constraint_by_rbe_docker_user(rbe_docker_user))
    result.extend(_constraint_by_rbe_docker_host_networking(rbe_docker_host_networking))
    return result
