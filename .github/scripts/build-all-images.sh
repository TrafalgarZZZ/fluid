#!/bin/bash
set -e

make docker-build-all

images=(
${IMG_REPO}/dataset-controller
${IMG_REPO}/application-controller
${IMG_REPO}/alluxioruntime-controller
${IMG_REPO}/jindoruntime-controller
${IMG_REPO}/goosefsruntime-controller
${IMG_REPO}/juicefsruntime-controller
${IMG_REPO}/thinruntime-controller
${IMG_REPO}/efcruntime-controller
${IMG_REPO}/vineyardruntime-controller
)

for img in ${images[@]}; do
    echo "Loading image $img to kind cluster..."
    kind load docker-image $img --name ${KIND_CLUSTER}
done