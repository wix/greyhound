load("@wix_oss_infra//:import_external.bzl", import_external = "safe_wix_scala_maven_import_external")

def dependencies():

    import_external(
        name = "org_portable_scala_portable_scala_reflect_2_12",
        artifact = "org.portable-scala:portable-scala-reflect_2.12:0.1.0",
        artifact_sha256 = "f7aa93d19eac0151759c27393ce739af4ef29035f38799376709e5cd6fcb9fae",
        srcjar_sha256 = "93e521282c14d09858d555a4f2293716b78de230761b4014c7c61e8cba68c734",
        deps = [
            "@org_scala_lang_scala_library",
            "@org_scala_lang_scala_reflect"
        ],
    )
