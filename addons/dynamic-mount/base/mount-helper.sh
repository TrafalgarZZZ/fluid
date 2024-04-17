#!/bin/bash
set -ex

function help() {
    echo "Usage: "
    echo "  bash mount-helper.sh mount|umount [args...]"
    echo "Examples: "
    echo "  1. mount filesystem [mount_src] to [mount_target] with options defined in [mount_opt_file]"
    echo "     bash mount-helper.sh mount [mount_src] [mount_target] [mount_opt_file]"
    echo "  2. umount filesystem mounted at [mount_target]"
    echo "     bash mount-helper.sh umount [mount_target]"
}

function error_msg() {
    help
    echo
    echo $1
    exit 1
}

function mount_fn() {
    if [[ $# -ne 3 ]]; then
        error_msg "Error: mount-helper.sh mount expects 3 arguments, but got $# arguments."
    fi
    mount_src=$1
    mount_target=$2
    fs_type=$3
    mount_opt_file=$4

    # NOTES.1: umount $mount_target here to avoid [[ -d $mount_target ]] returning "Transport Endpoint is not connected" error.
    # NOTES.2: Use "cat /proc/self/mountinfo" instead of the "mount" command because Alpine has some issue on printing mount info with "mount".
    if cat /proc/self/mountinfo | grep " ${mount_target} " > /dev/null; then
        echo "found mount point on ${mount_target}, umount it before re-mount."
        umount ${mount_target}
    fi

    if [[ ! -d "$mount_target" ]]; then
        mkdir -p "$mount_target"
    fi

    # exec to make supervisord monitor this process.
    exec /opt/mount.sh $mount_src $mount_target $fs_type $mount_opt_file
}

function umount_fn() {
    if [[ $# -ne 1 ]]; then
        error_msg "Error: mount-helper.sh umount expects 1 argument, but got $# arguments."
    fi
    umount $1 || true
}

function main() {
    if [[ $# -eq 0 ]]; then
        error_msg "Error: not enough arguments, require at least 1 argument"
    fi

    if [[ $# -gt 0 ]]; then
        case $1 in
            mount)
                shift
                mount_fn $@
                ;;
            unmount|umount)
                shift
                umount_fn $@
                ;;
            *)
                error_msg "Error: unknown option: $1"
                ;;
        esac
    fi 
}

main $@

