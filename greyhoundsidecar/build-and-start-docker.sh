#!/usr/bin/env bash
sbt compile
sbt docker:publishLocal
docker-compose up