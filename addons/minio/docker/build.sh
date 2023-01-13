#!/usr/bin/env bash
set +x

docker build . --network=host -f Dockerfile -t fluidcloudnative/minio-goofys:v0.1

docker push fluidcloudnative/minio-goofys:v0.1