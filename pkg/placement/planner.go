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

package placement

import (
	"context"
	"github.com/golang/glog"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/util/wait"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sort"
	"time"
)

type Planner struct {
	client     client.Client
	jobResList []JobRes
}

func (p *Planner) Start(i <-chan struct{}) error {
	p.Run()
	<-i
	return nil
}

func NewPlanner(client client.Client, jobResList []JobRes) *Planner {
	return &Planner{
		client:     client,
		jobResList: jobResList,
	}
}

func (p *Planner) Run() {
	stopCh := make(chan struct{})
	go wait.Until(p.PlanOnce, time.Second*30, stopCh)
}

func (p *Planner) PlanOnce() {
	nodes, err := p.ListAllNodes()
	if err != nil {
		glog.Errorf("can't list all nodes due to error: %v", err)
		return
	}

	availableMem := resource.NewQuantity(0, resource.BinarySI)
	for _, node := range nodes {
		availableMem.Add(node.Status.Allocatable[corev1.ResourceMemory])
	}

	availableMemInGiB := availableMem.Value() / 1024.0 / 1024.0 / 1024.0
	glog.Infof("Getting available memory %v GiB", availableMemInGiB)

	available := availableMem.Value()
	toSchedule := []JobRes{}
	for _, jr := range p.jobResList {
		total := jr.JobResRequest.Value() + jr.DataResRequest.Value()
		if total < available {
			available -= total
			toSchedule = append(toSchedule, jr)
		}
	}
	glog.Infof("%d jobs to be scheduled, they are %+v", len(toSchedule), toSchedule)

	sort.SliceStable(toSchedule, func(i, j int) bool {
		iTotal := toSchedule[i].DataResRequest.Value() + toSchedule[i].JobResRequest.Value()
		jTotal := toSchedule[j].DataResRequest.Value() + toSchedule[j].JobResRequest.Value()
		if iTotal != jTotal {
			return iTotal > jTotal
		} else {
			return toSchedule[i].DataResRequest.Value() > toSchedule[j].DataResRequest.Value()
		}
	})

	glog.Infof("jobs to scheduled after sorting: %+v", toSchedule)

	for _, jr := range toSchedule {
		glog.Infof("Scheduling job id %d data cache(request: %+v)", jr.JobId, jr.DataResRequest)

		dCandidateNode := ""
		var dCandidateAllocatable int64 = 0
		for _, node := range nodes {
			nodeAllocatableMem := node.Status.Allocatable[corev1.ResourceMemory]
			glog.Infof("Node %s has allocatable memory %+v", node.Name, nodeAllocatableMem)
			if nodeAllocatableMem.Cmp(jr.DataResRequest) > 0 {
				// Allocatable is larger than data res request
				if nodeAllocatableMem.Value() > dCandidateAllocatable {
					dCandidateNode = node.Name
					dCandidateAllocatable = nodeAllocatableMem.Value()
				}
			}
		}

		if dCandidateNode == "" {
			glog.Infof("Can't find node with enough resource for job's data (job id: %d, data res: %+v)", jr.JobId, jr.DataResRequest.String())
			continue
		}

		for _, node := range nodes {
			if node.Name == dCandidateNode {
				nodeAllocatableMem := node.Status.Allocatable[corev1.ResourceMemory]
				nodeAllocatableMem.Sub(jr.DataResRequest)
				node.Status.Allocatable[corev1.ResourceMemory] = nodeAllocatableMem
			}
		}

		glog.Infof("Scheduling job id %d DL worker(request: %+v)", jr.JobId, jr.JobResRequest)
		wCandidateNode := ""
		var wCandidateNodeAllocatable int64 = 0
		for _, node := range nodes {
			nodeAllocatableMem := node.Status.Allocatable[corev1.ResourceMemory]
			glog.Infof("Node %s has allocatable memory %+v", node.Name, nodeAllocatableMem)
			if nodeAllocatableMem.Cmp(jr.JobResRequest) > 0 {
				if nodeAllocatableMem.Value() > wCandidateNodeAllocatable {
					wCandidateNode = node.Name
					wCandidateNodeAllocatable = nodeAllocatableMem.Value()
				}
			}
		}

		if wCandidateNode == "" {
			glog.Infof("Can't find node with enough resource for job's worker (job id: %d, worker res: %+v)", jr.JobId, jr.JobResRequest)
			continue
		}

		for _, node := range nodes {
			if node.Name == wCandidateNode {
				nodeAllocatableMem := node.Status.Allocatable[corev1.ResourceMemory]
				nodeAllocatableMem.Sub(jr.JobResRequest)
				node.Status.Allocatable[corev1.ResourceMemory] = nodeAllocatableMem
			}
		}

		glog.Infof("Job id %d is already scheduled. Data cache to node %s, job worker to node %s", jr.JobId, dCandidateNode, wCandidateNode)
	}
}

func (p *Planner) ListAllNodes() ([]corev1.Node, error) {
	nodes := corev1.NodeList{}
	if err := p.client.List(context.TODO(), &nodes); err != nil {
		return nil, err
	}

	return nodes.Items, nil
}
