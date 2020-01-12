load("@wix_oss_infra//:import_external.bzl", import_external = "safe_wix_scala_maven_import_external")

def dependencies():

    import_external(
        name = "org_xerial_snappy_snappy_java",
        artifact = "org.xerial.snappy:snappy-java:1.1.7.1",
        artifact_sha256 = "bb52854753feb1919f13099a53475a2a8eb65dbccd22839a9b9b2e1a2190b951",
        srcjar_sha256 = "a01c58c2af4bf16d2b841c74f76d98489bc03b5f9cf63aea33a8cb14ce376258",
    )
