#!/bin/bash

function syslog() {
    echo ">>> $1"
}

function check_control_plane_status() {
    while true; do
        total_pods=$(kubectl get pod -n fluid-system --no-headers | grep -cv "Completed")
        running_pods=$(kubectl get pod -n fluid-system --no-headers | grep -c "Running")

        if [[ $total_pods -ne 0 ]]; then
            if [[ $total_pods -eq $running_pods ]]; then
                break
            fi
        fi
        sleep 5
    done
    syslog "Fluid control plane is ready!"
}

function alluxio_e2e() {
    set -e
    # docker pull alluxio/alluxio-dev:2.9.0
    # kind load docker-image alluxio/alluxio-dev:2.9.0 --name ${KIND_CLUSTER}
    bash test/gha-e2e/alluxio/test.sh
}

function juicefs_e2e() {
    set -e
    echo ">>> System disk usage before pulling juicefs"
    docker info
    docker system df
    docker ps
    docker container prune -f
    docker images
    docker image prune -a -f
    docker builder prune -a -f
    docker buildx prune -a -f
    df -h
    bash test/gha-e2e/juicefs/test.sh

    # docker pull juicedata/juicefs-fuse:ce-v1.1.0-rc1
    # echo ">>> System disk usage after pulling juicefs"
    # df -h
    # kind load docker-image juicedata/juicefs-fuse:ce-v1.1.0-rc1 --name ${KIND_CLUSTER}
    # docker image prune -a -f

}

check_control_plane_status
alluxio_e2e
juicefs_e2e
