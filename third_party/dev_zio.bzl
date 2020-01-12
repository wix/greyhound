load("@wix_oss_infra//:import_external.bzl", import_external = "safe_wix_scala_maven_import_external")

def dependencies():

    import_external(
        name = "dev_zio_zio_2_12",
        artifact = "dev.zio:zio_2.12:1.0.0-RC17",
        artifact_sha256 = "35ade72b393ff4dc6f43f00a7c8a3e7e6c9fe4b09135c587380d361ce87ffdfb",
        srcjar_sha256 = "0fb84fd9ed5dbf8b1faf903b349842b11cad86ea61e93b04e7e9e75f17bac537",
        deps = [
            "@dev_zio_zio_stacktracer_2_12",
            "@org_scala_lang_scala_library"
        ],
    )


    import_external(
        name = "dev_zio_zio_stacktracer_2_12",
        artifact = "dev.zio:zio-stacktracer_2.12:1.0.0-RC17",
        artifact_sha256 = "dd43181922c418d5ab896c7f1f5f1e95ee2b5c23087a88f36e1e8979418fc36b",
        srcjar_sha256 = "87bb8a7c61f8d6359af08eb0635ece6f27805ebbe49ee747976d2b92c7f1e153",
        deps = [
            "@org_scala_lang_scala_library"
        ],
    )


    import_external(
        name = "dev_zio_zio_streams_2_12",
        artifact = "dev.zio:zio-streams_2.12:1.0.0-RC17",
        artifact_sha256 = "640cfa72edc83708b51d454046f4e87bd2a0357141ccf68bf82aaccc736d2a65",
        srcjar_sha256 = "e77ebd39bbeca257f0a774e5d10015cffc0b0b2728f574640b2e6c53da269120",
        deps = [
            "@dev_zio_zio_2_12",
            "@org_scala_lang_scala_library"
        ],
    )


    import_external(
        name = "dev_zio_zio_test_2_12",
        artifact = "dev.zio:zio-test_2.12:1.0.0-RC17",
        artifact_sha256 = "d5415b493da334da445ee1bc829550cb5c32f76e9035c43ce3ed781f43249c7e",
        srcjar_sha256 = "c16dad2b0064cd7afed2d485c422ab0666a9ad76fea2519050d9e4fc636f3c9b",
        deps = [
            "@dev_zio_zio_2_12",
            "@dev_zio_zio_streams_2_12",
            "@org_portable_scala_portable_scala_reflect_2_12",
            "@org_scala_lang_scala_library"
        ],
    )
