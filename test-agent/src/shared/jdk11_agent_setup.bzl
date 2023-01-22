def agent_setup_flags(extra_runtime_dirs, extra_runtime_entries):
    flags = [
        "-javaagent:$(rootpath @greyhound//test-agent/src/main/java/com/wixpress/agent:test-agent_deploy.jar)",
        "-Dextra.dirs=" + ":".join(extra_runtime_dirs),
        "-Dextra.runtime.dirs=" + ":".join(extra_runtime_entries),
        # this is needed to allow TestsJavaAgent to access internal fields and methods
        "--add-opens=java.base/jdk.internal.loader=ALL-UNNAMED",
        "--add-opens=java.base/java.lang=ALL-UNNAMED"
    ]

    # test-agent-deploy jar is added to bootcalsspath becasue when the test classpath is very long
    # and the test is executed using a manifest jar for some unknown reason the JVM fails to find
    # the agent jar
    agent_path = "$(rootpath @greyhound//test-agent/src/main/java/com/wixpress/agent:test-agent_deploy.jar)"

    flags.append("-Xbootclasspath/a:\"" + agent_path + "\"")
    return flags