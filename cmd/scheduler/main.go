package main

import (
	"fmt"
	"github.com/fluid-cloudnative/fluid"
	datav1alpha1 "github.com/fluid-cloudnative/fluid/api/v1alpha1"
	"github.com/fluid-cloudnative/fluid/pkg/scheduler"
	"github.com/fluid-cloudnative/fluid/pkg/scheduler/queue"
	"github.com/fluid-cloudnative/fluid/pkg/utils/helm"
	"github.com/spf13/cobra"
	zapOpt "go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"gopkg.in/yaml.v2"
	"io/ioutil"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"os"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
)

var (
	scheme      = runtime.NewScheme()
	setupLog    = ctrl.Log.WithName("setup")
	development bool
	short       bool
)

var cmd = &cobra.Command{
	Use:   "fluid-scheduler",
	Short: "Scheduler for Fluid",
}

var startCmd = &cobra.Command{
	Use:   "start",
	Short: "start fluid-scheduler in Kubernetes",
	Run: func(cmd *cobra.Command, args []string) {
		handle()
		//testInstall()
	},
}

func init() {
	_ = clientgoscheme.AddToScheme(scheme)
	_ = datav1alpha1.AddToScheme(scheme)

	startCmd.Flags().BoolVarP(&development, "development", "", true, "Enable development mode for fluid controller.")
	startCmd.Flags().BoolVar(&short, "short", false, "print just the short version info")

	cmd.AddCommand(startCmd)
}

func testInstall() {
	compSpec := scheduler.CompSpec{
		Command: []string{"sh", "-c", "export PYTHONUNBUFFERED=1 && bash train.sh"},
		Env: []v1.EnvVar{
			{
				Name:  "BATCH_SIZE",
				Value: "128",
			},
			{
				Name:  "EPOCH_NUM",
				Value: "1",
			},
		},
		Image:      "registry-vpc.cn-beijing.aliyuncs.com/fluid-namespace/pytorch:remote-imagenet-202111162029",
		DataClaim:  "hbase",
		WorkingDir: "/scripts/imagenet",
	}
	value := &scheduler.PyTorchJobArgs{
		Metadata: scheduler.Metadata{
			Name:      "xxx",
			Namespace: "default",
		},
		Master: scheduler.Master{
			Replicas: 1,
			Spec:     compSpec,
		},
		Worker: scheduler.Worker{
			Replicas: 3,
			Spec:     compSpec,
		},
	}

	data, err := yaml.Marshal(value)
	if err != nil {
		panic(err)
	}

	valueFile, err := ioutil.TempFile(os.TempDir(), fmt.Sprintf("testInstall-values.yaml"))
	if err != nil {
		panic(err)
	}

	valueFileName := valueFile.Name()
	err = ioutil.WriteFile(valueFileName, data, 0400)
	if err != nil {
		panic(fmt.Errorf("%v", err))
	}
	fmt.Println(valueFileName)

	err = helm.InstallRelease("xxx", "default", valueFileName, "/charts/pytorchjob")
	if err != nil {
		panic(fmt.Errorf("%v", err))
	}
}

func handle() {
	fluid.LogVersion()

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
		setupLog.Error(err, "unable to start alluxioruntime manager")
		os.Exit(1)
	}

	//schedulerQueue := make(chan *datav1alpha1.FluidJob, 100)
	//schedulerQueue := queue.NewQueue(&queue.FIFOStrategy{})
	schedulerQueue := queue.NewQueue(queue.NewDatasetAwareStrategy(mgr.GetClient()))

	reconciler := &scheduler.FluidJobReconciler{
		Client:         mgr.GetClient(),
		Log:            ctrl.Log.WithName("fluidjobctl").WithName("FluidJob"),
		Scheme:         mgr.GetScheme(),
		SchedulerQueue: schedulerQueue,
	}

	sched := &scheduler.Scheduler{
		Client:         mgr.GetClient(),
		SchedulerQueue: schedulerQueue,
		Log:            ctrl.Log.WithName("fluidjob scheduler"),
	}

	sched.Run()

	if err = reconciler.SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "unable to create controller", "controller", "FluidJob")
		os.Exit(1)
	}

	setupLog.Info("starting fluid scheduler")
	if err := mgr.Start(ctrl.SetupSignalHandler()); err != nil {
		setupLog.Error(err, "problem when starting fluid scheduler")
		os.Exit(1)
	}
}

func main() {
	if err := cmd.Execute(); err != nil {
		fmt.Fprintf(os.Stderr, "%s", err.Error())
		os.Exit(1)
	}
}
