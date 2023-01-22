def _append_parameters_to_list(kwargs, name, values_to_add):
    values = kwargs.pop(name, [])
    kwargs[name] = values + values_to_add

def _add_semanticdb_support(**kwargs):
    semanticdb_plugins = ["@org_scalameta_semanticdb_scalac_2_12_15"]
    semanticdb_scalacopts = [
        "-P:semanticdb:synthetics:on",
    ]

    is_main_repo = native.repository_name() == "@"
    build_semanticdb = kwargs.pop("build_semanticdb", None)

    if build_semanticdb:
        scalacopts = semanticdb_scalacopts
        plugins = semanticdb_plugins
    elif build_semanticdb == False or not is_main_repo:
        scalacopts = []
        plugins = []
    else:
        plugins = select({
            "@greyhound//dependencies/rules_scala:with_semanticdb": semanticdb_plugins,
            "//conditions:default": [],
        })

        scalacopts = select({
            "@greyhound//dependencies/rules_scala:with_semanticdb": semanticdb_scalacopts,
            "//conditions:default": [],
        })

    _append_parameters_to_list(kwargs, "plugins", plugins)
    _append_parameters_to_list(kwargs, "scalacopts", scalacopts)

    return kwargs

def with_meta(scala_rule, *args, **kwargs):
    scala_rule(*args, **_add_semanticdb_support(**kwargs))
