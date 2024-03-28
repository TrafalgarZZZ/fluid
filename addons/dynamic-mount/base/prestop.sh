#!/bin/bash
set -e

echo "prestop.sh: umounting mountpoints under ${MOUNT_POINT}"

mount_points=$(mount | grep "on ${MOUNT_POINT}" | awk '{print $3}')

for mount_point in ${mount_points}; do
    echo ">> mount-helper.sh umount ${mount_point}"
    mount-helper.sh umount ${mount_point}
done
