#!/usr/bin/env bash
set +x

python3 /fluid_config_init.py

chmod u+x ./mount-minio.sh

bash ./mount-minio.sh

