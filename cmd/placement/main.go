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

package main

import (
	"context"
	"flag"
	"fmt"
	"github.com/fluid-cloudnative/fluid"
	datav1alpha1 "github.com/fluid-cloudnative/fluid/api/v1alpha1"
	"github.com/fluid-cloudnative/fluid/pkg/placement"
	"github.com/spf13/cobra"
	zapOpt "go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"os"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	"time"
)

var (
	scheme      = runtime.NewScheme()
	development bool
	short       bool
	setupLog    = ctrl.Log.WithName("setup")
)

var cmd = &cobra.Command{
	Use:   "placement",
	Short: "Scheduler placement for Fluid",
}

var planCmd = &cobra.Command{
	Use:   "plan",
	Short: "plan once for data and job placements",
	Run: func(cmd *cobra.Command, args []string) {
		plan()
	},
}

var nodeResourceSummaryCmd = &cobra.Command{
	Use:   "node-summary",
	Short: "Get node available resource memory",
	Run: func(cmd *cobra.Command, args []string) {
		getNodeInfo()
	},
}

func init() {
	if err := flag.Set("logtostderr", "true"); err != nil {
		fmt.Printf("Failed to flag.set due to %v", err)
		os.Exit(1)
	}

	_ = clientgoscheme.AddToScheme(scheme)
	_ = datav1alpha1.AddToScheme(scheme)

	planCmd.Flags().BoolVarP(&development, "development", "", true, "Enable development mode for fluid controller.")
	planCmd.Flags().BoolVar(&short, "short", false, "print just the short version info")

	cmd.AddCommand(planCmd)
	cmd.AddCommand(nodeResourceSummaryCmd)

	//flag.Parse()
}

func plan() {
	fluid.LogVersion()

	nodeResList := []placement.NodeRes{}
	for i := 0; i < 6; i++ {
		nodeResList = append(nodeResList, placement.NodeRes{
			Name:         fmt.Sprintf("node-%d", i),
			AvailableRes: resource.MustParse("200Gi"),
			CapacityRes:  resource.MustParse("200Gi"),
		})
	}

	//dataResRequestNumbers := []int{41, 58, 46, 86, 58, 62, 52, 39, 58, 50}
	//jobResRequestNumbers := []int{64, 52, 56, 42, 67, 42, 60, 61, 71, 60}
	dataResRequestNumbers := []int{70, 79, 63, 65, 60, 78, 78, 62, 66, 63}
	jobResRequestNumbers := []int{35, 25, 52, 44, 53, 28, 30, 51, 50, 52}

	jobResList := []placement.JobRes{}

	for i := range dataResRequestNumbers {
		jobResList = append(jobResList, placement.JobRes{
			JobId:          i,
			DataResRequest: resource.MustParse(fmt.Sprintf("%dGi", dataResRequestNumbers[i])),
			JobResRequest:  resource.MustParse(fmt.Sprintf("%dGi", jobResRequestNumbers[i])),
		})
	}
	//jobResList := []placement.JobRes{
	//	{
	//		JobId:          0,
	//		DataResRequest: resource.MustParse("140Gi"),
	//		JobResRequest:  resource.MustParse("80Gi"),
	//	},
	//	{
	//		JobId:          1,
	//		DataResRequest: resource.MustParse("70Gi"),
	//		JobResRequest:  resource.MustParse("60Gi"),
	//	},
	//}

	//planner := placement.NewPlanner(mgr.GetClient(), jobResList)
	planner := placement.NewVirtualPlanner(jobResList, nodeResList)

	planner.PlanOnce()
}

func getNodeInfo() {
	// Initialize Node capacity.
	// TODO: should be input from some file to indicate the capacity for each node
	nodeCapacity := map[string]resource.Quantity{}

	nodeCapacity["cn-beijing.172.16.2.247"] = resource.MustParse("200Gi")
	nodeCapacity["cn-beijing.172.16.3.99"] = resource.MustParse("200Gi")

	// initialize kubernetes client
	ctrl.SetLogger(zap.New(func(o *zap.Options) {
		o.Development = development
	}, func(o *zap.Options) {
		o.ZapOpts = append(o.ZapOpts, zapOpt.AddCaller())
	}, func(o *zap.Options) {
		if !development {
			encCfg := zapOpt.NewProductionEncoderConfig()
			encCfg.EncodeLevel = zapcore.CapitalLevelEncoder
			encCfg.EncodeTime = zapcore.ISO8601TimeEncoder
			o.Encoder = zapcore.NewConsoleEncoder(encCfg)
		}
	}))

	mgr, err := ctrl.NewManager(ctrl.GetConfigOrDie(), ctrl.Options{
		Scheme: scheme,
	})

	if err != nil {
		panic(err)
	}

	go func() {
		if err := mgr.Start(ctrl.SetupSignalHandler()); err != nil {
			setupLog.Error(err, "problem when starting fluid scheduler")
			os.Exit(1)
		}
	}()

	// Wait 3 seconds to let manager fully start.
	time.Sleep(3 * time.Second)
	client := mgr.GetClient()

	// List all pods in Kubernetes.
	pl := &v1.PodList{}
	if err = client.List(context.TODO(), pl); err != nil {
		panic(err)
	}

	pods := pl.Items
	for _, pod := range pods {
		// Only check pods in default namespace
		if pod.Namespace != "default" {
			continue
		}

		// Only calculate pods with the "experiment" label
		if _, ok := pod.ObjectMeta.Labels["experiment"]; !ok {
			continue
		}

		// Completed or Failed pods do not count in the calculation
		if pod.Status.Phase != v1.PodRunning {
			continue
		}

		// Only check pods that already scheduled to some node
		if len(pod.Spec.NodeName) == 0 {
			continue
		}

		nodeName := pod.Spec.NodeName
		if avail, ok := nodeCapacity[nodeName]; ok {
			for _, c := range pod.Spec.Containers {
				reqMem := c.Resources.Requests[v1.ResourceMemory]
				avail.Sub(reqMem)
				nodeCapacity[nodeName] = avail
			}
		}
	}

	for k, v := range nodeCapacity {
		fmt.Printf("node %s has available memory resource %s\n", k, v.String())
	}
}

func main() {
	if err := cmd.Execute(); err != nil {
		fmt.Fprintf(os.Stderr, "%s", err.Error())
		os.Exit(1)
	}
}
