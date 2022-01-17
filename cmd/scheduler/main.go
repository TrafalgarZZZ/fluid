package main

import (
	"fmt"
	"github.com/fluid-cloudnative/fluid"
	datav1alpha1 "github.com/fluid-cloudnative/fluid/api/v1alpha1"
	"github.com/fluid-cloudnative/fluid/pkg/scheduler"
	"github.com/spf13/cobra"
	zapOpt "go.uber.org/zap"
	"go.uber.org/zap/zapcore"
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
	},
}

func init() {
	_ = clientgoscheme.AddToScheme(scheme)
	_ = datav1alpha1.AddToScheme(scheme)

	startCmd.Flags().BoolVarP(&development, "development", "", true, "Enable development mode for fluid controller.")
	startCmd.Flags().BoolVar(&short, "short", false, "print just the short version info")

	cmd.AddCommand(startCmd)
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

	reconciler := &scheduler.FluidJobReconciler{
		Client: mgr.GetClient(),
		Log:    ctrl.Log.WithName("fluidjobctl").WithName("FluidJob"),
		Scheme: mgr.GetScheme(),
	}

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
