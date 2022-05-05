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
	"fmt"
	"github.com/golang/glog"
	"k8s.io/apimachinery/pkg/api/resource"
	"sort"
)

type VirtualPlanner struct {
	jobResList  []JobRes
	nodeResList []NodeRes
}

func NewVirtualPlanner(jobResList []JobRes, nodeResList []NodeRes) *VirtualPlanner {
	return &VirtualPlanner{
		jobResList:  jobResList,
		nodeResList: nodeResList,
	}
}

func (v *VirtualPlanner) PlanOnce() {
	availableMem := resource.NewQuantity(0, resource.BinarySI)
	for _, node := range v.nodeResList {
		availableMem.Add(node.AvailableRes)
	}

	availableMemInGiB := availableMem.Value() / 1024.0 / 1024.0 / 1024.0
	glog.Infof("Getting available memory %v GiB", availableMemInGiB)

	available := availableMem.Value()
	toSchedule := []JobRes{}
	for _, jr := range v.jobResList {
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

	dataPlaces := []string{}
	workerPlaces := []string{}

	for _, jr := range toSchedule {
		//preferAffinityScheduling := v.IsSufficientResourceAvailable(0.5)
		preferAffinityScheduling := v.IsSufficientResourceAvailable(0.40)

		if preferAffinityScheduling {
			candidateNode := ""
			var candidateAllocatable int64 = 9223372036854775806
			totalResRequest := resource.NewQuantity(0, resource.BinarySI)
			totalResRequest.Add(jr.DataResRequest)
			totalResRequest.Add(jr.JobResRequest)

			for _, node := range v.nodeResList {
				nodeAllocatableMem := node.AvailableRes
				if nodeAllocatableMem.Cmp(*totalResRequest) >= 0 {
					if nodeAllocatableMem.Value() < candidateAllocatable {
						candidateNode = node.Name
						candidateAllocatable = nodeAllocatableMem.Value()
					}
				}
			}

			if candidateNode == "" {
				glog.Infof("can't find node with enough resource for both data cache and job worker (job id: %d, total res: %+v), fall backing", jr.JobId, totalResRequest)
			} else {
				for idx, node := range v.nodeResList {
					if node.Name == candidateNode {
						v.nodeResList[idx].AvailableRes.Sub(*totalResRequest)
					}
				}
				dataPlaces = append(dataPlaces, candidateNode)
				workerPlaces = append(workerPlaces, candidateNode)

				glog.Infof("Job id %d is already scheduled. Data cache to node %s, job worker to node %s", jr.JobId, candidateNode, candidateNode)
				continue
			}
		}

		glog.Infof("Scheduling job id %d data cache(request: %+v)", jr.JobId, jr.DataResRequest)
		dCandidateNode := ""
		var dCandidateAllocatable int64 = 9223372036854775806
		for _, node := range v.nodeResList {
			nodeAllocatableMem := node.AvailableRes
			//glog.Infof("Node %s has allocatable memory %+v", node.Name, nodeAllocatableMem)
			if nodeAllocatableMem.Cmp(jr.DataResRequest) >= 0 {
				// Allocatable is larger than data res request
				if nodeAllocatableMem.Value() < dCandidateAllocatable {
					dCandidateNode = node.Name
					dCandidateAllocatable = nodeAllocatableMem.Value()
				}
			}
		}

		if dCandidateNode == "" {
			glog.Infof("Can't find node with enough resource for job's data (job id: %d, data res: %+v)", jr.JobId, jr.DataResRequest.String())
			continue
		}

		for idx, node := range v.nodeResList {
			if node.Name == dCandidateNode {
				v.nodeResList[idx].AvailableRes.Sub(jr.DataResRequest)
			}
		}

		glog.Infof("Scheduling job id %d DL worker(request: %+v)", jr.JobId, jr.JobResRequest)
		wCandidateNode := ""
		var wCandidateNodeAllocatable int64 = 9223372036854775806
		for _, node := range v.nodeResList {
			nodeAllocatableMem := node.AvailableRes
			//glog.Infof("Node %s has allocatable memory %+v", node.Name, nodeAllocatableMem)
			if nodeAllocatableMem.Cmp(jr.JobResRequest) >= 0 {
				if nodeAllocatableMem.Value() < wCandidateNodeAllocatable {
					wCandidateNode = node.Name
					wCandidateNodeAllocatable = nodeAllocatableMem.Value()
				}
			}
		}

		if wCandidateNode == "" {
			glog.Infof("Can't find node with enough resource for job's worker (job id: %d, worker res: %+v)", jr.JobId, jr.JobResRequest)
			continue
		}

		for idx, node := range v.nodeResList {
			if node.Name == wCandidateNode {
				v.nodeResList[idx].AvailableRes.Sub(jr.JobResRequest)
			}
		}

		dataPlaces = append(dataPlaces, dCandidateNode)
		workerPlaces = append(workerPlaces, wCandidateNode)
		glog.Infof("Job id %d is already scheduled. Data cache to node %s, job worker to node %s", jr.JobId, dCandidateNode, wCandidateNode)
	}

	for idx := range dataPlaces {
		fmt.Printf("Job id %d scheduled to (%s, %s)\n", idx, dataPlaces[idx], workerPlaces[idx])
	}
}

func (v *VirtualPlanner) IsSufficientResourceAvailable(threshold float64) bool {
	var totalCapacity int64 = 0
	var totalAvailable int64 = 0

	for _, node := range v.nodeResList {
		totalAvailable += node.AvailableRes.Value()
		totalCapacity += node.CapacityRes.Value()
	}

	if float64(totalAvailable)/float64(totalCapacity) > threshold {
		return true
	}

	return false
}
