workspace(name = "greyhound")

load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")
load("@bazel_tools//tools/build_defs/repo:git.bzl","git_repository")








skylib_version = "0.8.0"
http_archive(
    name = "bazel_skylib",
    type = "tar.gz",
    url = "https://github.com/bazelbuild/bazel-skylib/releases/download/{}/bazel-skylib.{}.tar.gz".format (skylib_version, skylib_version),
    sha256 = "2ef429f5d7ce7111263289644d233707dba35e39696377ebab8b0bc701f7818e",
)





## remove when finished adding 3rd party!!!!!!!!!!!!!!!
#git_repository(
#    name = "core_server_build_tools",
#    remote = "git@github.com:wix-private/core-server-build-tools.git",
#    branch= "master",
#)
#git_repository(branch= "master",
#                       name= "bazel_tooling",
#                       remote= "git@github.com:wix-private/bazel-tooling.git")


wix_oss_infra_version="91f89eb1f05b9cb21a8a20caa7d34678d5d5db66"
# wix_oss_infra_version_sha256="416212e14481cff8fd4849b1c1c1200a7f34808a54377e22d7447efdf54ad758"

http_archive(
    name = "wix_oss_infra",
    url = "https://github.com/wix/wix-oss-infra/archive/%s.tar.gz" % wix_oss_infra_version,
    strip_prefix = "wix-oss-infra-%s" % wix_oss_infra_version,
    # sha256 = wix_oss_infra_version_sha256,
)

# local_repository(name = "wix_oss_infra",
# path = "/Users/natans/Work-bazel/wix-oss-infra")

load("@wix_oss_infra//dependencies/rules_scala:rules_scala.bzl", "rules_scala")

rules_scala()

load("@wix_oss_infra//test-agent/src/shared:tests_external_repository.bzl", "tests_external_repository")
tests_external_repository(name = "tests")

load("@wix_oss_infra//dependencies/google_protobuf:google_protobuf.bzl", "google_protobuf")

google_protobuf()

load("@io_bazel_rules_scala//specs2:specs2_junit.bzl", "specs2_junit_repositories")





scala_version = "2.12.6"
load("@io_bazel_rules_scala//scala:scala.bzl", "scala_repositories")
scala_repositories((scala_version, {
    "scala_compiler": "3023b07cc02f2b0217b2c04f8e636b396130b3a8544a8dfad498a19c3e57a863",
    "scala_library": "f81d7144f0ce1b8123335b72ba39003c4be2870767aca15dd0888ba3dab65e98",
    "scala_reflect": "ffa70d522fc9f9deec14358aa674e6dd75c9dfa39d4668ef15bb52f002ce99fa"
}))

specs2_junit_repositories(scala_version)





load("@io_bazel_rules_scala//scala:toolchains.bzl", "scala_register_toolchains")
scala_register_toolchains()

load("//:third_party.bzl", "third_party_dependencies")

third_party_dependencies()