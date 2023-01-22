ALL_CONSTRAINTS = [
    (machine_size, docker_user, docker_host_networking, network_mode, rbe_pool)
    for machine_size in [
        "machine_size_large",
        "machine_size_small",
    ]
    for docker_user in [
        "docker_user_privileged",
        "docker_user_unprivileged",
    ]
    for docker_host_networking in [
        "host_docker_networking_mode_enabled",
        "host_docker_networking_mode_disabled",
    ]
    for network_mode in [
        "network_mode_enabled",
        "network_mode_disabled",
    ]
    for rbe_pool in [
        "rbe_pool_firecracker",
        "rbe_pool_generic",
    ]
]

def build_platforms(common_exec_properties = {}, per_config_exec_properties = {}):
    native.platform(
        name = "rbe_default",
        exec_properties = common_exec_properties,
        parents = ["@core_server_build_tools//toolchains/generated/config:platform"],
        tags = ["manual"],
    )
    for machine_size, docker_user, docker_host_networking, network_mode, rbe_pool in ALL_CONSTRAINTS:
        exec_props = {}
        constraint_values = []
        name = "rbe"
        for config_value in machine_size, docker_user, docker_host_networking, network_mode, rbe_pool:
            exec_props.update(**per_config_exec_properties["//platforms_common:%s" % config_value])
            constraint_values.append("@core_server_build_tools//platforms_common:%s" % config_value)
            name = name + "_" + config_value
        native.platform(
            name = name,
            constraint_values = constraint_values,
            exec_properties = exec_props,
            parents = [":rbe_default"],
            tags = ["manual"],
        )
