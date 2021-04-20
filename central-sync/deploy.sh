#!/bin/bash

#make sure you have a secret gpg key: gpg --list-secret-keys
#Set version in VERSION file
#export DEPLOY_MAVEN_USERNAME=YYY
#export DEPLOY_MAVEN_PASSWORD=XXX
bazel run //data-streams/greyhound/core:deploy-maven  -- release --gpg
bazel run //data-streams/greyhound/core:deploy-maven-testkit  -- release --gpg
bazel run //data-streams/greyhound/future-interop:deploy-maven  -- release --gpg
bazel run //data-streams/greyhound/java-interop:deploy-maven-java-core  -- release --gpg
bazel run //data-streams/greyhound/java-interop:deploy-maven  -- release --gpg
#go to https://oss.sonatype.org/#stagingRepositories, close then release