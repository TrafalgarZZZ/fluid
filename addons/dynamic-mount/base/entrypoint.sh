#!/bin/bash

set -e

trap "/usr/local/bin/prestop.sh" SIGTERM 

tini -- supervisord -n