_ROOT_BUILD = """
alias(
    name = "{repository_name}",
    actual = "{actual}",
    visibility = ["//visibility:public"]
)
"""

def _import_external_alias_impl(repository_ctx):
    repository_ctx.file(
        "BUILD",
        _ROOT_BUILD.format(
            repository_name = repository_ctx.name,
            actual = repository_ctx.attr.actual,
        ),
        executable = False,
    )

import_external_alias = repository_rule(
    implementation = _import_external_alias_impl,
    attrs = {
        "actual": attr.string(mandatory = True),
    },
)
