workspace(name = "greyhound")

load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")
load("@bazel_tools//tools/build_defs/repo:git.bzl","git_repository")

# TODO: move to wix_oss_infra
skylib_version = "0.8.0"
http_archive(
    name = "bazel_skylib",
    type = "tar.gz",
    url = "https://github.com/bazelbuild/bazel-skylib/releases/download/{}/bazel-skylib.{}.tar.gz".format (skylib_version, skylib_version),
    sha256 = "2ef429f5d7ce7111263289644d233707dba35e39696377ebab8b0bc701f7818e",
)

wix_oss_infra_version="864e2eb81fcd14bd002f3368be6b3187b90df978"
wix_oss_infra_version_sha256="0790f5778b436814cf48945d3a167b239f6bb9161f21d0f03030b1914c9aa952"

http_archive(
    name = "wix_oss_infra",
    url = "https://github.com/wix/wix-oss-infra/archive/%s.tar.gz" % wix_oss_infra_version,
    strip_prefix = "wix-oss-infra-%s" % wix_oss_infra_version,
     sha256 = wix_oss_infra_version_sha256,
)

load("@wix_oss_infra//dependencies/rules_scala:rules_scala.bzl", "rules_scala")
rules_scala()

load("@wix_oss_infra//test-agent/src/shared:tests_external_repository.bzl", "tests_external_repository")
tests_external_repository(name = "tests", jdk_version="11")

load("@wix_oss_infra//dependencies/google_protobuf:google_protobuf.bzl", "google_protobuf")
google_protobuf()


# TODO: move to wix_oss_infra
scala_version = "2.12.6"
load("@io_bazel_rules_scala//scala:scala.bzl", "scala_repositories")
scala_repositories((scala_version, {
    "scala_compiler": "3023b07cc02f2b0217b2c04f8e636b396130b3a8544a8dfad498a19c3e57a863",
    "scala_library": "f81d7144f0ce1b8123335b72ba39003c4be2870767aca15dd0888ba3dab65e98",
    "scala_reflect": "ffa70d522fc9f9deec14358aa674e6dd75c9dfa39d4668ef15bb52f002ce99fa"
}))

# TODO: move to wix_oss_infra
load("@io_bazel_rules_scala//specs2:specs2_junit.bzl", "specs2_junit_repositories")
specs2_junit_repositories(scala_version)

register_toolchains("@wix_oss_infra//toolchains:wix_defaults_global_toolchain")

load("@wix_oss_infra//:third_party.bzl", "managed_third_party_dependencies")
managed_third_party_dependencies()