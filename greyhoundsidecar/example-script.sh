#!/usr/bin/env bash
sbt docker:publishLocal
docker-compose up