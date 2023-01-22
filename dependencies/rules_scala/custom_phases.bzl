load("@io_bazel_rules_scala//scala:advanced_usage/providers.bzl", "ScalaRulePhase")
load("@io_bazel_rules_scala//scala/private:dependency.bzl", "new_dependency_info")

# Scala rules customization which disables dependency tracking (strict/unused) and is meant
# to be used only in Starlark macros like prime_app.
# Motivation: dep tracking is disabled due to invalid buildozer messages, which cannot be correctly applied
# Usage: must be used only in Starlark macros, in other usecases always use regular rules
def _disabled_dep_tracking_phase(ctx, p):
    return new_dependency_info(
        dependency_mode = "plus-one",
        unused_deps_mode = "off",
        strict_deps_mode = "off",
        compiler_deps_mode = "off",
        dependency_tracking_method = "ast",
    )

def _dep_tracking_phase_config(ctx):
    return [
        ScalaRulePhase(
            custom_phases = [
                ("=", "dependency", "dependency", _disabled_dep_tracking_phase),
            ],
        ),
    ]

dep_tracking_phase_config = rule(
    implementation = _dep_tracking_phase_config,
)

# when dep tracking is in warn mode, we want to continue the build to be able to grep for all buildozer commands in
# one run, -Xfatal-warnings prevents from doing that as it fails the build on any scalac warning. This phase removes
# -Xfatal-warnings flag from scalacopts if dep tracking is in warn mode
def _remove_x_fatal_warnings(ctx, p):
    opt_to_remove = "-Xfatal-warnings"
    toolchain = ctx.toolchains["@io_bazel_rules_scala//scala:toolchain_type"]
    scalacopts = toolchain.scalacopts + ctx.attr.scalacopts

    dt_in_warn_mode = p.dependency.strict_deps_mode == "warn" or p.dependency.unused_deps_mode == "warn"

    if dt_in_warn_mode and opt_to_remove in scalacopts:
        print("Scalac opt %s is not compatible with dependency tracking and is ignored. Please remove it." % opt_to_remove)
        return [opt for opt in scalacopts if opt != opt_to_remove]
    else:
        return scalacopts

def _dep_tracking_scalacopts_sanitization_config(ctx):
    return [
        ScalaRulePhase(
            custom_phases = [
                ("=", "scalacopts", "scalacopts", _remove_x_fatal_warnings),
            ],
        ),
    ]

dep_tracking_scalacopts_sanitization_config = rule(
    implementation = _dep_tracking_scalacopts_sanitization_config,
)

def phase_providers(providers = []):
    return {
        "phase_providers": providers,
    }
