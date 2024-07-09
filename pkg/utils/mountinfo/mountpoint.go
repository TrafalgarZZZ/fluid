/*
Copyright 2021 The Fluid Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package mountinfo

import (
	"fmt"
	"path"
	"strings"

	"github.com/fluid-cloudnative/fluid/pkg/utils"
	"github.com/golang/glog"
)

type MountPoint struct {
	SourcePath            string
	MountPath             string
	FilesystemType        string
	ReadOnly              bool
	Count                 int
	NamespacedDatasetName string // <namespace>-<dataset>
	SubPaths              []*Mount
}

func GetBrokenMountPoints() ([]MountPoint, error) {
	// get mountinfo from proc
	mountByPath, err := loadMountInfo()
	if err != nil {
		return nil, err
	}

	// get global mount set in map
	globalMountByName, err := getGlobalMounts(mountByPath)
	if err != nil {
		return nil, err
	}

	// get bind mount
	rootPathByVolumeName, subPathByVolumeName := getBindMounts(mountByPath)

	// get broken bind mount
	return getBrokenBindMounts(globalMountByName, rootPathByVolumeName, subPathByVolumeName), nil
}

func getGlobalMounts(mountByPath map[string]*Mount) (globalMountByName map[string]*Mount, err error) {
	globalMountByName = make(map[string]*Mount)
	// get fluid MountRoot
	mountRoot, err := utils.GetMountRoot()
	if err != nil {
		return nil, err
	}

	for k, v := range mountByPath {
		if strings.Contains(k, mountRoot) {
			fields := strings.Split(k, "/")
			if len(fields) < 6 {
				continue
			}
			// fluid global mount path is: /{rootPath}/{runtimeType}/{namespace}/{datasetName}/{runtimeTypeFuse}
			namespace, datasetName := fields[3], fields[4]
			namespacedName := fmt.Sprintf("%s-%s", namespace, datasetName)
			globalMountByName[namespacedName] = v
		}
	}
	return
}

func getBindMounts(mountByPath map[string]*Mount) (targetPathByVolumeName map[string][]*Mount, subPathByPodUid map[string][]*Mount) {
	targetPathByVolumeName = make(map[string][]*Mount)
	subPathByPodUid = make(map[string][]*Mount)
	for k, m := range mountByPath {
		var datasetNamespacedName string
		if strings.Contains(k, "kubernetes.io~csi") && strings.Contains(k, "mount") {
			// root target path for a fluid volume is like: /{kubeletRootDir}(default: /var/lib/kubelet)/pods/{podUID}/volumes/kubernetes.io~csi/{namespace}-{datasetName}/mount
			fields := strings.Split(k, "/")
			if len(fields) < 3 {
				continue
			}
			datasetNamespacedName = fields[len(fields)-2]
			targetPathByVolumeName[datasetNamespacedName] = append(targetPathByVolumeName[datasetNamespacedName], m)
		}
		if strings.Contains(k, "volume-subpaths") {
			// pod using subPath which is like: /{kubeletRootDir}(default: /var/lib/kubelet)/pods/{podUID}/volume-subpaths/{namespace}-{datasetName}/{containerName}/{volumeIndex}
			fields := strings.Split(k, "/")
			if len(fields) < 6 {
				continue
			}
			podUid := fields[len(fields)-5]
			subPathByPodUid[podUid] = append(subPathByPodUid[datasetNamespacedName], m)
		}
	}
	return
}

func getBrokenBindMounts(globalMountByName map[string]*Mount, targetPathByVolumeName map[string][]*Mount, subPathByPodUid map[string][]*Mount) (brokenMounts []MountPoint) {
	for volName, targetPaths := range targetPathByVolumeName {
		globalMount, ok := globalMountByName[volName]
		if !ok {
			// globalMount is unmount, ignore
			glog.V(6).Infof("ignoring mountpoint %s because of not finding its global mount point", volName)
			continue
		}
		for _, targetPath := range targetPaths {
			// In case of not sharing same peer group in mount info, meaning it a broken mount point
			if len(utils.IntersectIntegerSets(targetPath.PeerGroups, globalMount.PeerGroups)) == 0 {
				// root target path for a fluid volume is like: /{kubeletRootDir}(default: /var/lib/kubelet)/pods/{podUID}/volumes/kubernetes.io~csi/{namespace}-{datasetName}/mount
				fields := strings.Split(targetPath.MountPath, "/")
				if len(fields) < 6 {
					glog.V(0).Infof("Warning: found a targetPath not containing pod uid (targetPath: %s), skipping recover it", targetPath.MountPath)
					continue
				}
				podUid := fields[len(fields)-5]

				subPathsToRecover := []*Mount{}
				if subPaths, exists := subPathByPodUid[podUid]; exists {
					for _, subPath := range subPaths {
						if strings.Contains(subPath.MountPath, volName) {
							subPathsToRecover = append(subPathsToRecover, subPath)
						}
					}
				}

				brokenMounts = append(brokenMounts, MountPoint{
					SourcePath:            path.Join(globalMount.MountPath, targetPath.Subtree),
					MountPath:             targetPath.MountPath,
					FilesystemType:        targetPath.FilesystemType,
					ReadOnly:              targetPath.ReadOnly,
					Count:                 targetPath.Count,
					NamespacedDatasetName: volName,
					SubPaths:              subPathsToRecover,
				})
			}
		}
	}
	return
}
