def java(jdk, jvm, target = 8):
    return {"jdk": str(jdk), "jvm": str(jvm), "target": str(target)}

default = java(11, 11, 11)

repos_jdk_versions = {
    "restaurants_bazel_jdk8": java(11, 11, 8),
}

def _calc_repo_jdk_version():
    version = java(11, 11, 11)
    print("Using JDK {} & JVM {} & target {}".format(version["jdk"], version["jvm"], version["target"]))
    return version

repo_versions = _calc_repo_jdk_version()
repo_jdk_version = repo_versions["jdk"]
repo_jvm_version = repo_versions["jvm"]
repo_jdk_target = repo_versions["target"]

