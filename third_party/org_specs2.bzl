load("@wix_oss_infra//:import_external.bzl", import_external = "safe_wix_scala_maven_import_external")

def dependencies():

    import_external(
        name = "org_specs2_specs2_fp_2_12",
        artifact = "org.specs2:specs2-fp_2.12:4.8.3",
        artifact_sha256 = "777962ca58054a9ea86e294e025453ecf394c60084c28bd61956a00d16be31a7",
        srcjar_sha256 = "6b8bd1e7210754b768b68610709271c0dac29447936a976a2a9881389e6404ca",
        deps = [
            "@org_scala_lang_scala_library"
        ],
    )


    import_external(
        name = "org_specs2_specs2_common_2_12",
        artifact = "org.specs2:specs2-common_2.12:4.8.3",
        artifact_sha256 = "3b08fecb9e21d3903e48b62cd95c19ea9253d466e03fd4cf9dc9227e7c368708",
        srcjar_sha256 = "b2f148c75d3939b3cd0d58afddd74a8ce03077bb3ccdc93dae55bd9c3993e9c3",
        deps = [
            "@org_scala_lang_modules_scala_parser_combinators_2_12",
            "@org_scala_lang_modules_scala_xml_2_12",
            "@org_scala_lang_scala_library",
            "@org_scala_lang_scala_reflect",
            "@org_specs2_specs2_fp_2_12"
        ],
    )


    import_external(
        name = "org_specs2_specs2_matcher_2_12",
        artifact = "org.specs2:specs2-matcher_2.12:4.8.3",
        artifact_sha256 = "aadf27b6d015572b2e3842627c09bf0797153dbb329262ea3bcbbce129d51ad8",
        srcjar_sha256 = "01251acc28219aa17aabcb9a26a84e1871aa64980d335cd8f83c2bcea6f4f1be",
        deps = [
            "@org_scala_lang_scala_library",
            "@org_specs2_specs2_common_2_12"
        ],
    )


    import_external(
        name = "org_specs2_specs2_core_2_12",
        artifact = "org.specs2:specs2-core_2.12:4.8.3",
        artifact_sha256 = "f73f32156a711a4e83e696dc83e269c5a165d62cc3dd7c652617cb03d140d063",
        srcjar_sha256 = "0e3cebfc7410051b70e627e35f13978add3d061b8f1233741f9b397638f193e9",
        deps = [
            "@org_scala_lang_scala_library",
            "@org_scala_sbt_test_interface",
            "@org_specs2_specs2_common_2_12",
            "@org_specs2_specs2_matcher_2_12"
        ],
    )


    import_external(
        name = "org_specs2_specs2_junit_2_12",
        artifact = "org.specs2:specs2-junit_2.12:4.8.3",
        artifact_sha256 = "5d7ad2c0b0bc142ea064edb7a1ea75ab7b17ad37e1a621ac7e578823845098e8",
        srcjar_sha256 = "84edd1cd6291f6686638225fcbaff970ae36da006efabf2228255c2127b2290c",
        deps = [
            "@junit_junit",
            "@org_scala_lang_scala_library",
            "@org_scala_sbt_test_interface",
            "@org_specs2_specs2_core_2_12"
        ],
    )
