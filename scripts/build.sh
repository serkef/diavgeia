#!/bin/bash

git pull
BUILD_TAG=$(git rev-parse HEAD)
docker build -t diavgeia-daily:${BUILD_TAG} .
