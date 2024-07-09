/*
Copyright 2022 The Fluid Authors.

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

package recover

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/fluid-cloudnative/fluid/pkg/common"
	"github.com/fluid-cloudnative/fluid/pkg/utils"
	"github.com/fluid-cloudnative/fluid/pkg/utils/dataset/volume"
	"github.com/fluid-cloudnative/fluid/pkg/utils/kubelet"
	"github.com/fluid-cloudnative/fluid/pkg/utils/mountinfo"
	"github.com/golang/glog"
	"github.com/pkg/errors"
	"golang.org/x/sys/unix"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/record"
	k8sexec "k8s.io/utils/exec"
	"k8s.io/utils/mount"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/manager"
)

const (
	defaultKubeletTimeout          = 10
	defaultFuseRecoveryPeriod      = 5 * time.Second
	defaultRecoverWarningThreshold = 50
	serviceAccountTokenFile        = "/var/run/secrets/kubernetes.io/serviceaccount/token"
	FuseRecoveryPeriod             = "RECOVER_FUSE_PERIOD"
	RecoverWarningThreshold        = "REVOCER_WARNING_THRESHOLD"
)

const (
	// syscall.Openat flags used to traverse directories not following symlinks
	nofollowFlags = unix.O_RDONLY | unix.O_NOFOLLOW
	// flags for getting file descriptor without following the symlink
	openFDFlags = unix.O_NOFOLLOW | unix.O_PATH
)

var _ manager.Runnable = &FuseRecover{}

type FuseRecover struct {
	mount.SafeFormatAndMount
	KubeClient client.Client
	ApiReader  client.Reader
	// KubeletClient *kubelet.KubeletClient
	Recorder record.EventRecorder

	recoverFusePeriod       time.Duration
	recoverWarningThreshold int

	locks *utils.VolumeLocks
}

func initializeKubeletClient() (*kubelet.KubeletClient, error) {
	// get CSI sa token
	tokenByte, err := os.ReadFile(serviceAccountTokenFile)
	if err != nil {
		return nil, errors.Wrap(err, "in cluster mode, find token failed")
	}
	token := string(tokenByte)

	glog.V(3).Infoln("start kubelet client")
	nodeIp := os.Getenv("NODE_IP")
	kubeletClientCert := os.Getenv("KUBELET_CLIENT_CERT")
	kubeletClientKey := os.Getenv("KUBELET_CLIENT_KEY")
	var kubeletTimeout int
	if os.Getenv("KUBELET_TIMEOUT") != "" {
		if kubeletTimeout, err = strconv.Atoi(os.Getenv("KUBELET_TIMEOUT")); err != nil {
			return nil, errors.Wrap(err, "got error when parsing kubelet timeout")
		}
	} else {
		kubeletTimeout = defaultKubeletTimeout
	}
	glog.V(3).Infof("get node ip: %s", nodeIp)
	kubeletClient, err := kubelet.NewKubeletClient(&kubelet.KubeletClientConfig{
		Address: nodeIp,
		Port:    10250,
		TLSClientConfig: rest.TLSClientConfig{
			ServerName: "kubelet",
			CertFile:   kubeletClientCert,
			KeyFile:    kubeletClientKey,
		},
		BearerToken: token,
		HTTPTimeout: time.Duration(kubeletTimeout) * time.Second,
	})

	if err != nil {
		return nil, err
	}

	return kubeletClient, nil
}

func NewFuseRecover(kubeClient client.Client, recorder record.EventRecorder, apiReader client.Reader, locks *utils.VolumeLocks) (*FuseRecover, error) {
	glog.V(3).Infoln("start csi recover")
	mountRoot, err := utils.GetMountRoot()
	if err != nil {
		return nil, errors.Wrap(err, "got err when getting mount root")
	}
	glog.V(3).Infof("Get mount root: %s", mountRoot)

	if err != nil {
		return nil, errors.Wrap(err, "got error when creating kubelet client")
	}

	recoverFusePeriod := utils.GetDurationValueFromEnv(FuseRecoveryPeriod, defaultFuseRecoveryPeriod)
	recoverWarningThreshold, found := utils.GetIntValueFromEnv(RecoverWarningThreshold)
	if !found {
		recoverWarningThreshold = defaultRecoverWarningThreshold
	}
	return &FuseRecover{
		SafeFormatAndMount: mount.SafeFormatAndMount{
			Interface: mount.New(""),
			Exec:      k8sexec.New(),
		},
		KubeClient:              kubeClient,
		ApiReader:               apiReader,
		Recorder:                recorder,
		recoverFusePeriod:       recoverFusePeriod,
		recoverWarningThreshold: recoverWarningThreshold,
		locks:                   locks,
	}, nil
}

func (r *FuseRecover) Start(ctx context.Context) error {
	// do recovering at beginning
	// recover set containerStat in memory, it's none when start
	r.recover()
	r.run(wait.NeverStop)

	return nil
}

func (r *FuseRecover) run(stopCh <-chan struct{}) {
	go wait.Until(r.runOnce, r.recoverFusePeriod, stopCh)
	<-stopCh
	glog.V(3).Info("Shutdown CSI recover.")
}

func (r *FuseRecover) runOnce() {
	r.recover()
}

func (r *FuseRecover) recover() {
	brokenMounts, err := mountinfo.GetBrokenMountPoints()
	if err != nil {
		glog.Error(err)
		return
	}

	for _, point := range brokenMounts {
		r.doRecover(point)
	}
}

func (r *FuseRecover) recoverBrokenMount(point mountinfo.MountPoint) (err error) {
	// recovery for each bind mount path
	mountOption := []string{"bind"}
	if point.ReadOnly {
		mountOption = append(mountOption, "ro")
	}

	glog.V(3).Infof("FuseRecovery: Start exec cmd: mount %s %s -o %v \n", point.SourcePath, point.MountPath, mountOption)
	if err := r.Mount(point.SourcePath, point.MountPath, "none", mountOption); err != nil {
		glog.Errorf("FuseRecovery: exec cmd: mount -o bind %s %s with err :%v", point.SourcePath, point.MountPath, err)
	}
	return
}

// check mountpoint count
// umount duplicate mountpoint util 1 avoiding very large mountinfo file.
// don't umount all item, 'mountPropagation' will lose efficacy.
func (r *FuseRecover) umountDuplicate(point mountinfo.MountPoint) {
	for i := point.Count; i > 1; i-- {
		glog.V(3).Infof("FuseRecovery: count: %d, start exec cmd: umount %s", i, point.MountPath)
		if err := r.Unmount(point.MountPath); err != nil {
			glog.Errorf("FuseRecovery: exec cmd: umount %s with err: %v", point.MountPath, err)
		}
	}
}

func (r *FuseRecover) eventRecord(point mountinfo.MountPoint, eventType, eventReason string) {
	namespacedName := point.NamespacedDatasetName
	strs := strings.Split(namespacedName, "-")
	if len(strs) < 2 {
		glog.V(3).Infof("can't parse dataset from namespacedName: %s", namespacedName)
		return
	}
	namespace, datasetName, err := volume.GetNamespacedNameByVolumeId(r.ApiReader, namespacedName)
	if err != nil {
		glog.Errorf("error get namespacedName by volume id %s: %v", namespacedName, err)
		return
	}

	dataset, err := utils.GetDataset(r.KubeClient, datasetName, namespace)
	if err != nil {
		glog.Errorf("error get dataset %s namespace %s: %v", datasetName, namespace, err)
		return
	}
	glog.V(4).Infof("record to dataset: %s, namespace: %s", dataset.Name, dataset.Namespace)
	switch eventReason {
	case common.FuseRecoverSucceed:
		r.Recorder.Eventf(dataset, eventType, eventReason, "Fuse recover %s succeed", point.MountPath)
	case common.FuseRecoverFailed:
		r.Recorder.Eventf(dataset, eventType, eventReason, "Fuse recover %s failed", point.MountPath)
	case common.FuseUmountDuplicate:
		r.Recorder.Eventf(dataset, eventType, eventReason, "Mountpoint %s has been mounted %v times, unmount duplicate mountpoint to avoid large /proc/self/mountinfo file, this may potential make data access connection broken", point.MountPath, point.Count)
	}
}

func (r *FuseRecover) shouldRecover(mountPath string) (should bool, err error) {
	mounter := mount.New("")
	notMount, err := mounter.IsLikelyNotMountPoint(mountPath)
	if os.IsNotExist(err) || (err == nil && notMount) {
		// Perhaps the mountPath has been cleaned up in other goroutine
		return false, nil
	}
	if err != nil && !mount.IsCorruptedMnt(err) {
		// unexpected error
		return false, err
	}

	return true, nil
}

func (r *FuseRecover) doRecover(point mountinfo.MountPoint) {
	if lock := r.locks.TryAcquire(point.MountPath); !lock {
		glog.V(4).Infof("FuseRecovery: fail to acquire lock on path %s, skip recovering it", point.MountPath)
		return
	}
	defer r.locks.Release(point.MountPath)

	should, err := r.shouldRecover(point.MountPath)
	if err != nil {
		glog.Warningf("FuseRecovery: found path %s which is unable to recover due to error %v, skip it", point.MountPath, err)
		return
	}

	if !should {
		glog.V(3).Infof("FuseRecovery: path %s has already been cleaned up, skip recovering it", point.MountPath)
		return
	}

	glog.V(3).Infof("FuseRecovery: recovering broken mount point: %v", point)
	// if app container restart, umount duplicate mount may lead to recover successed but can not access data
	// so we only umountDuplicate when it has mounted more than the recoverWarningThreshold
	// please refer to https://github.com/fluid-cloudnative/fluid/issues/3399 for more information
	if point.Count > r.recoverWarningThreshold {
		glog.Warningf("FuseRecovery: Mountpoint %s has been mounted %v times, exceeding the recoveryWarningThreshold %v, unmount duplicate mountpoint to avoid large /proc/self/mountinfo file, this may potentially make data access connection broken", point.MountPath, point.Count, r.recoverWarningThreshold)
		r.eventRecord(point, corev1.EventTypeWarning, common.FuseUmountDuplicate)
		r.umountDuplicate(point)
	}
	if err := r.recoverBrokenMount(point); err != nil {
		r.eventRecord(point, corev1.EventTypeWarning, common.FuseRecoverFailed)
		return
	}

	for _, subPath := range point.SubPaths {
		recoverErr := r.doRecoverSubPath(point.MountPath, *subPath)
		if recoverErr != nil {
			glog.Errorf("failed to recover subPath %s: %v", subPath.MountPath, err)
		}
	}

	r.eventRecord(point, corev1.EventTypeNormal, common.FuseRecoverSucceed)
}

// doRecoverSubPath recovers sub path by following how kubelet prepares subPaths for pods.
func (r *FuseRecover) doRecoverSubPath(volumePath string, subPath mountinfo.Mount) (err error) {
	// todo: umountDuplicate when over threshold
	if strings.HasSuffix(subPath.Subtree, "//deleted") {
		subPath.Subtree = strings.TrimSuffix(subPath.Subtree, "//deleted")
	}
	fullSubPath := filepath.Join(volumePath, subPath.Subtree)
	fd, err := doSafeOpen(filepath.Join(volumePath, subPath.Subtree), volumePath)
	if err != nil {
		return fmt.Errorf("error opening subpath %v: %v", fullSubPath, err)
	}
	defer syscall.Close(fd)

	pluginPid := os.Getpid()
	mountSource := fmt.Sprintf("/proc/%d/fd/%v", pluginPid, fd)

	// Do the bind mount
	options := []string{"bind"}
	glog.V(0).Infof("FuseRecovery: bind mounting %s at %s", mountSource, subPath.MountPath)

	if err = r.Mount(mountSource, subPath.MountPath, "", options); err != nil {
		return errors.Wrapf(err, "failed to bind mount %s at %s", mountSource, subPath.MountPath)
	}
	return
}

// This implementation is shared between Linux and NsEnterMounter
// Open path and return its fd.
// Symlinks are disallowed (pathname must already resolve symlinks),
// and the path must be within the base directory.
func doSafeOpen(pathname string, base string) (int, error) {
	pathname = filepath.Clean(pathname)
	base = filepath.Clean(base)

	// Calculate segments to follow
	subpath, err := filepath.Rel(base, pathname)
	if err != nil {
		return -1, err
	}
	segments := strings.Split(subpath, string(filepath.Separator))

	// Assumption: base is the only directory that we have under control.
	// Base dir is not allowed to be a symlink.
	parentFD, err := syscall.Open(base, nofollowFlags|unix.O_CLOEXEC, 0)
	if err != nil {
		return -1, fmt.Errorf("cannot open directory %s: %s", base, err)
	}
	defer func() {
		if parentFD != -1 {
			if err = syscall.Close(parentFD); err != nil {
				glog.V(4).Infof("Closing FD %v failed for safeopen(%v): %v", parentFD, pathname, err)
			}
		}
	}()

	childFD := -1
	defer func() {
		if childFD != -1 {
			if err = syscall.Close(childFD); err != nil {
				glog.V(4).Infof("Closing FD %v failed for safeopen(%v): %v", childFD, pathname, err)
			}
		}
	}()

	currentPath := base

	// Follow the segments one by one using openat() to make
	// sure the user cannot change already existing directories into symlinks.
	for _, seg := range segments {
		var deviceStat unix.Stat_t

		currentPath = filepath.Join(currentPath, seg)
		if !mount.PathWithinBase(currentPath, base) {
			return -1, fmt.Errorf("path %s is outside of allowed base %s", currentPath, base)
		}

		// Trigger auto mount if it's an auto-mounted directory, ignore error if not a directory.
		// Notice the trailing slash is mandatory, see "automount" in openat(2) and open_by_handle_at(2).
		unix.Fstatat(parentFD, seg+"/", &deviceStat, unix.AT_SYMLINK_NOFOLLOW)

		glog.V(5).Infof("Opening path %s", currentPath)
		childFD, err = syscall.Openat(parentFD, seg, openFDFlags|unix.O_CLOEXEC, 0)
		if err != nil {
			return -1, fmt.Errorf("cannot open %s: %s", currentPath, err)
		}

		err := unix.Fstat(childFD, &deviceStat)
		if err != nil {
			return -1, fmt.Errorf("error running fstat on %s with %v", currentPath, err)
		}
		fileFmt := deviceStat.Mode & syscall.S_IFMT
		if fileFmt == syscall.S_IFLNK {
			return -1, fmt.Errorf("unexpected symlink found %s", currentPath)
		}

		// Close parentFD
		if err = syscall.Close(parentFD); err != nil {
			return -1, fmt.Errorf("closing fd for %q failed: %v", filepath.Dir(currentPath), err)
		}
		// Set child to new parent
		parentFD = childFD
		childFD = -1
	}

	// We made it to the end, return this fd, don't close it
	finalFD := parentFD
	parentFD = -1

	return finalFD, nil
}
