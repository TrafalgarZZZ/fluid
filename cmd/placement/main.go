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
	"flag"
	"fmt"
	"github.com/fluid-cloudnative/fluid"
	datav1alpha1 "github.com/fluid-cloudnative/fluid/api/v1alpha1"
	"github.com/fluid-cloudnative/fluid/pkg/placement"
	"github.com/spf13/cobra"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"os"
	ctrl "sigs.k8s.io/controller-runtime"
)

var (
	scheme      = runtime.NewScheme()
	development bool
	short       bool
	setupLog    = ctrl.Log.WithName("setup")
)

var cmd = &cobra.Command{
	Use:   "fluid-scheduler-placement",
	Short: "Scheduler placement for Fluid",
}

var startCmd = &cobra.Command{
	Use:   "start",
	Short: "start fluid-scheduler-placement in Kubernetes",
	Run: func(cmd *cobra.Command, args []string) {
		handle()
	},
}

func init() {
	if err := flag.Set("logtostderr", "true"); err != nil {
		fmt.Printf("Failed to flag.set due to %v", err)
		os.Exit(1)
	}

	_ = clientgoscheme.AddToScheme(scheme)
	_ = datav1alpha1.AddToScheme(scheme)

	startCmd.Flags().BoolVarP(&development, "development", "", true, "Enable development mode for fluid controller.")
	startCmd.Flags().BoolVar(&short, "short", false, "print just the short version info")

	cmd.AddCommand(startCmd)

	//flag.Parse()
}

func handle() {
	fluid.LogVersion()

	//ctrl.SetLogger(zap.New(func(o *zap.Options) {
	//	o.Development = development
	//}, func(o *zap.Options) {
	//	o.ZapOpts = append(o.ZapOpts, zapOpt.AddCaller())
	//}, func(o *zap.Options) {
	//	if !development {
	//		encCfg := zapOpt.NewProductionEncoderConfig()
	//		encCfg.EncodeLevel = zapcore.CapitalLevelEncoder
	//		encCfg.EncodeTime = zapcore.ISO8601TimeEncoder
	//		o.Encoder = zapcore.NewConsoleEncoder(encCfg)
	//	}
	//}))

	//mgr, err := ctrl.NewManager(ctrl.GetConfigOrDie(), ctrl.Options{
	//	Scheme: scheme,
	//})
	//if err != nil {
	//	setupLog.Error(err, "unable to start alluxioruntime manager")
	//	os.Exit(1)
	//}
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

	//if err = mgr.Add(planner); err != nil {
	//	panic(err)
	//}
	//
	//setupLog.Info("starting fluid scheduler placement planner")
	//if err := mgr.Start(ctrl.SetupSignalHandler()); err != nil {
	//	setupLog.Error(err, "problem when starting fluid scheduler")
	//	os.Exit(1)
	//}
}

func main() {
	//if err := cmd.Execute(); err != nil {
	//	fmt.Fprintf(os.Stderr, "%s", err.Error())
	//	os.Exit(1)
	//}
	handle()
}
