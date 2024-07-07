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

check_control_plane_status
bash test/gha-e2e/alluxio/test.sh