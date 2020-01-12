load("@wix_oss_infra//:import_external.bzl", import_external = "safe_wix_scala_maven_import_external")

def dependencies():

    import_external(
        name = "ch_qos_logback_logback_classic",
        artifact = "ch.qos.logback:logback-classic:1.1.11",
        artifact_sha256 = "86a0268c3c96888d4e49d8a754b5b2173286aee100559e803efcbb0df676c66e",
        srcjar_sha256 = "590d0b4f20e42b0750f42bbbdd580053162d732bf1dd9ed3e6baac409d7d4c3b",
        deps = [
            "@ch_qos_logback_logback_core",
            "@org_slf4j_slf4j_api"
        ],
    )


    import_external(
        name = "ch_qos_logback_logback_core",
        artifact = "ch.qos.logback:logback-core:1.1.11",
        artifact_sha256 = "58738067842476feeae5768e832cd36a0e40ce41576ba5739c3632d376bd8c86",
        srcjar_sha256 = "045a7f572b8432062ce735451930569824bc1bec53a6cb51c73cf8ee1985f5a8",
    )
