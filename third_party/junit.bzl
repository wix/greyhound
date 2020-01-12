load("@wix_oss_infra//:import_external.bzl", import_external = "safe_wix_scala_maven_import_external")

def dependencies():

    import_external(
        name = "junit_junit",
        artifact = "junit:junit:4.13",
        artifact_sha256 = "4b8532f63bdc0e0661507f947eb324a954d1dbac631ad19c8aa9a00feed1d863",
        srcjar_sha256 = "3d5451031736d4904582b211858a09eeefdb26eb08f0633ca8addf04fde3e0fc",
        deps = [
            "@org_hamcrest_hamcrest_core"
        ],
    )
