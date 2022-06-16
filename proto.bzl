def proto_binds():
    native.bind(
        name = "io_bazel_rules_scala/dependency/proto/scalapb_plugin",
        actual = "@com_thesamet_scalapb_compilerplugin_2_12",
      )

    native.bind(
            name = "io_bazel_rules_scala/dependency/proto/protoc_bridge",
            actual = "@com_thesamet_scalapb_protoc_bridge_2_12",
          )

    native.bind(
                name = "io_bazel_rules_scala/dependency/proto/scalapbc",
                actual = "@com_thesamet_scalapb_protoc_bridge_2_12",
              )

    native.bind(
                  name = "io_bazel_rules_scala/dependency/proto/implicit_compile_deps",
                  actual = "@com_thesamet_scalapb_protoc_bridge_2_12",
                )

    native.bind(
                      name = "io_bazel_rules_scala/dependency/proto/grpc_deps",
                      actual = "@com_thesamet_scalapb_protoc_bridge_2_12",
                    )