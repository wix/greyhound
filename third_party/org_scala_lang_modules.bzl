load("@wix_oss_infra//:import_external.bzl", import_external = "safe_wix_scala_maven_import_external")

def dependencies():

    import_external(
        name = "org_scala_lang_modules_scala_parser_combinators_2_12",
        artifact = "org.scala-lang.modules:scala-parser-combinators_2.12:1.1.2",
        artifact_sha256 = "24985eb43e295a9dd77905ada307a850ca25acf819cdb579c093fc6987b0dbc2",
        srcjar_sha256 = "8fbe3fa9e748f24aa6d6868c0c2be30d41a02d20428ed5429bb57a897cb756e3",
        deps = [
            "@org_scala_lang_scala_library"
        ],
    )


    import_external(
        name = "org_scala_lang_modules_scala_xml_2_12",
        artifact = "org.scala-lang.modules:scala-xml_2.12:1.2.0",
        artifact_sha256 = "1b48dc206f527b7604ef32492ada8e71706c63a65d999e0cabdafdc5793b4d63",
        srcjar_sha256 = "288fdce0b296df28725707cc87cff0f65ef435c54e93e0c1fbc5a7fcc8e19ade",
        deps = [
            "@org_scala_lang_scala_library"
        ],
    )
