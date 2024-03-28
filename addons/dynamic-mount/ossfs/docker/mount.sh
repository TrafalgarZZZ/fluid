#!/bin/bash

set -e

if [[ $# -ne 3 ]]; then
    echo "Error: require 3 arguments, but got $# arguments"
    exit 1
fi

mount_src=$1        # e.g. oss://mybucket
mount_target=$2     # e.g. /runtime-mnt/thin/default/thin-demo/thin-fuse/mybucket
mount_opt_file=$3   # e.g. /etc/fluid/mount-opts/mybucket.opts (mount options in json format)

url_opt=$(cat $mount_opt_file | jq -r '.["url"]' )
access_key_file=$(cat $mount_opt_file | jq -r '.["oss-access-key"]')
access_secret_file=$(cat $mount_opt_file | jq -r '.["oss-access-secret"]')

if cat /etc/passwd-ossfs | grep ${mount_src#oss://} > /dev/null; then
    echo "found existing access information at /etc/passwd-ossfs"
else
    echo "${mount_src#oss://}:`cat $access_key_file`:`cat $access_secret_file`" >> /etc/passwd-ossfs
fi
chmod 640 /etc/passwd-ossfs

exec ossfs -f -ourl=$url_opt ${mount_src#oss://} $mount_target 