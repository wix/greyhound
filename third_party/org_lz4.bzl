load("@wix_oss_infra//:import_external.bzl", import_external = "safe_wix_scala_maven_import_external")

def dependencies():

    import_external(
        name = "org_lz4_lz4_java",
        artifact = "org.lz4:lz4-java:1.4.1",
        artifact_sha256 = "f0efa5ce1318f0e3e734f35238dacc441c6510cb6f3fee6d1cfd3ebae15e2bef",
        srcjar_sha256 = "f239c4dcb907781dd5c4d4ce7497dfd627dadcc653277ae1b4c3694c44b6d61b",
    )
