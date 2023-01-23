load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")

def resources(name = "resources", runtime_deps=[], testonly = 0, tags = [], visibility=None):
    native.java_library(
        name = name,
        resources = native.glob(["**"],exclude=["BUILD"]),
        resource_strip_prefix = "%s/" % native.package_name(),
        runtime_deps = runtime_deps,
        testonly = testonly,
        tags = tags,
        visibility = visibility
    )


def _package_visibility(pacakge_name):
    return ["//{p}:__pkg__".format(p=pacakge_name)]
 

def sources(visibility = None):
    if visibility == None:
      visibility = _package_visibility(native.package_name())
    native.filegroup(
       name = "sources",
       srcs = native.glob(["*.java"], allow_empty=True) + native.glob(["*.scala"], allow_empty=True),
       visibility = visibility,
    )