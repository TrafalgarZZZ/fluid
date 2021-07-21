/*

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

package csi

import (
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	dockerapi "github.com/docker/docker/api/types"
	dockercontainer "github.com/docker/docker/api/types/container"
	dockerstrslice "github.com/docker/docker/api/types/strslice"
	dockerclient "github.com/docker/docker/client"
	"github.com/pkg/errors"
	"io"
	appsv1 "k8s.io/api/apps/v1"
	v1 "k8s.io/api/core/v1"
	apierrs "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"strings"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/golang/glog"
	csicommon "github.com/kubernetes-csi/drivers/pkg/csi-common"
	"golang.org/x/net/context"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const AlluxioFuseImage = "registry.aliyuncs.com/alluxio/alluxio-fuse:release-2.5.0-2-SNAPSHOT-52ad95c"

type controllerServer struct {
	*csicommon.DefaultControllerServer
	client client.Client
}

func (cs *controllerServer) CreateVolume(ctx context.Context, req *csi.CreateVolumeRequest) (*csi.CreateVolumeResponse, error) {
	volumeID := sanitizeVolumeID(req.GetName())

	glog.Infof("volumeID %v", volumeID)

	if err := cs.Driver.ValidateControllerServiceRequest(csi.ControllerServiceCapability_RPC_CREATE_DELETE_VOLUME); err != nil {
		glog.Infof("invalid create volume req: %v", req)
		return nil, err
	}

	// Check arguments
	if len(volumeID) == 0 {
		return nil, status.Error(codes.InvalidArgument, "Name missing in request")
	}
	if req.GetVolumeCapabilities() == nil {
		return nil, status.Error(codes.InvalidArgument, "Volume Capabilities missing in request")
	}

	capacityBytes := int64(req.GetCapacityRange().GetRequiredBytes())

	glog.V(4).Infof("Creating volume %s", volumeID)
	return &csi.CreateVolumeResponse{
		Volume: &csi.Volume{
			VolumeId:      volumeID,
			CapacityBytes: capacityBytes,
			VolumeContext: req.GetParameters(),
		},
	}, nil
}

func (cs *controllerServer) DeleteVolume(ctx context.Context, req *csi.DeleteVolumeRequest) (*csi.DeleteVolumeResponse, error) {
	volumeID := req.GetVolumeId()

	// Check arguments
	if len(volumeID) == 0 {
		return nil, status.Error(codes.InvalidArgument, "Volume ID missing in request")
	}

	if err := cs.Driver.ValidateControllerServiceRequest(csi.ControllerServiceCapability_RPC_CREATE_DELETE_VOLUME); err != nil {
		glog.V(3).Infof("Invalid delete volume req: %v", req)
		return nil, err
	}
	glog.V(4).Infof("Deleting volume %s", volumeID)

	return &csi.DeleteVolumeResponse{}, nil
}

func (cs *controllerServer) ValidateVolumeCapabilities(ctx context.Context, req *csi.ValidateVolumeCapabilitiesRequest) (*csi.ValidateVolumeCapabilitiesResponse, error) {

	// Check arguments
	if len(req.GetVolumeId()) == 0 {
		return nil, status.Error(codes.InvalidArgument, "Volume ID missing in request")
	}
	if req.GetVolumeCapabilities() == nil {
		return nil, status.Error(codes.InvalidArgument, "Volume capabilities missing in request")
	}

	// We currently only support RWO
	supportedAccessMode := &csi.VolumeCapability_AccessMode{
		Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER,
	}

	for _, cap := range req.VolumeCapabilities {
		if cap.GetAccessMode().GetMode() != supportedAccessMode.GetMode() {
			return &csi.ValidateVolumeCapabilitiesResponse{Message: "Only single node writer is supported"}, nil
		}
	}

	return &csi.ValidateVolumeCapabilitiesResponse{
		Confirmed: &csi.ValidateVolumeCapabilitiesResponse_Confirmed{
			VolumeCapabilities: []*csi.VolumeCapability{
				{
					AccessMode: supportedAccessMode,
				},
			},
		},
	}, nil
}

func (cs *controllerServer) ControllerExpandVolume(ctx context.Context, req *csi.ControllerExpandVolumeRequest) (*csi.ControllerExpandVolumeResponse, error) {
	return &csi.ControllerExpandVolumeResponse{}, status.Error(codes.Unimplemented, "ControllerExpandVolume is not implemented")
}

func (cs *controllerServer) ControllerPublishVolume(ctx context.Context, req *csi.ControllerPublishVolumeRequest) (*csi.ControllerPublishVolumeResponse, error) {
	glog.Infof("ControllerPublishVolume: try to start a FUSE container, %v", req)
	cli, err := dockerclient.NewClientWithOpts(dockerclient.FromEnv)
	if err != nil {
		return nil, errors.Wrap(err, "Can't new docker client")
	}

	_, err = cli.ImagePull(ctx, AlluxioFuseImage, dockerapi.ImagePullOptions{})
	if err != nil {
		return nil, errors.Wrap(err, fmt.Sprintf("Can't pull image(%s)", AlluxioFuseImage))
	}

	namespacedName := strings.Split(req.GetVolumeId(), "-")
	glog.Infof("Making container run config with namespace: %s and name: %s", namespacedName[0], namespacedName[1])
	containerConfig, err := cs.makeContainerRunConfig(namespacedName[0], namespacedName[1])
	if err != nil {
		return nil, errors.Wrap(err, fmt.Sprintf("Can't make container run config"))
	}

	glog.Info(">>>> container config", containerConfig)

	//io.Copy(os.Stdout, reader)
	resp, err := cli.ContainerCreate(ctx, containerConfig, nil, nil, fmt.Sprintf("%s-fuse", namespacedName))

	if err != nil {
		return nil, errors.Wrap(err, fmt.Sprintf("Can't create container, runConfig: %v", containerConfig))
	}

	if err := cli.ContainerStart(ctx, resp.ID, dockerapi.ContainerStartOptions{}); err != nil {
		return nil, errors.Wrap(err, "Can't start container")
	}

	return &csi.ControllerPublishVolumeResponse{}, nil
}

func (cs *controllerServer) ControllerUnpublishVolume(ctx context.Context, req *csi.ControllerUnpublishVolumeRequest) (*csi.ControllerUnpublishVolumeResponse, error) {
	glog.Infof("ControllerUnpublishVolume: try to destroy a FUSE container, %v", req)
	return &csi.ControllerUnpublishVolumeResponse{}, nil
}

func sanitizeVolumeID(volumeID string) string {
	volumeID = strings.ToLower(volumeID)
	if len(volumeID) > 63 {
		h := sha1.New()
		_, err := io.WriteString(h, volumeID)
		if err != nil {
			glog.Warningf("writeString error %v", err)
		}
		volumeID = hex.EncodeToString(h.Sum(nil))
	}
	return volumeID
}

func (cs *controllerServer) makeContainerRunConfig(namespace, name string) (*dockercontainer.Config, error) {
	fuseDaemonsetName := name + "-fuse"

	daemonset := &appsv1.DaemonSet{}
	err := cs.client.Get(context.TODO(), types.NamespacedName{
		Name:      fuseDaemonsetName,
		Namespace: namespace,
	}, daemonset)

	if err != nil {
		return nil, err
	}

	containerToStart := daemonset.Spec.Template.Spec.Containers[0]
	envs, err := cs.makeEnvironmentVariables(namespace, &containerToStart)

	return &dockercontainer.Config{
		Env:        envs,
		Image:      containerToStart.Image,
		Entrypoint: dockerstrslice.StrSlice(containerToStart.Command),
		Cmd:        dockerstrslice.StrSlice(containerToStart.Args),
		WorkingDir: containerToStart.WorkingDir,
		OpenStdin:  containerToStart.Stdin,
		StdinOnce:  containerToStart.StdinOnce,
		Tty:        containerToStart.TTY,
		Healthcheck: &dockercontainer.HealthConfig{
			Test: []string{"NONE"},
		},
	}, nil
}

func (cs *controllerServer) makeEnvironmentVariables(namespace string, container *v1.Container) ([]string, error) {
	var result []string
	var err error
	var (
		configMaps = make(map[string]*v1.ConfigMap)
		//secrets = make(map[string]*v1.Secret)
		tmpEnv = make(map[string]string)
	)

	for _, envFrom := range container.EnvFrom {
		switch {
		case envFrom.ConfigMapRef != nil:
			cm := envFrom.ConfigMapRef
			name := cm.Name
			configMap, ok := configMaps[name]
			if !ok {
				if cs.client == nil {
					return result, fmt.Errorf("couldn't get configMap %v/%v, no kubeClient defined", namespace, name)
				}
				optional := cm.Optional != nil && *cm.Optional
				configMap = &v1.ConfigMap{}
				err = cs.client.Get(context.TODO(), types.NamespacedName{
					Namespace: name,
					Name:      namespace,
				}, configMap)

				if err != nil {
					if apierrs.IsNotFound(err) && optional {
						continue
					}
					return result, err
				}
				configMaps[name] = configMap
			}

			for k, v := range configMap.Data {
				if len(envFrom.Prefix) > 0 {
					k = envFrom.Prefix + k
				}
				tmpEnv[k] = v
			}
		}
	}

	for _, envVar := range container.Env {
		runtimeVal := envVar.Value
		if runtimeVal != "" {
			tmpEnv[envVar.Name] = runtimeVal
		} else if envVar.ValueFrom != nil {
			// Currently we ignore such env for PoC
			continue
		}
	}

	for k, v := range tmpEnv {
		result = append(result, fmt.Sprintf("%s=%s", k, v))
	}

	return result, nil
}
