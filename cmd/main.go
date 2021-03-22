package main

import (
	"context"
	"fmt"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/tools/clientcmd"
)

func main() {
	config, err := clientcmd.BuildConfigFromFlags("", "/root/.kube/config")
	if err != nil {
		panic(err)
	}

	dynamicClient, err := dynamic.NewForConfig(config)
	if err != nil {
		panic(err)
	}

	datasetGVR := schema.GroupVersionResource{
		Group:    "data.fluid.io",
		Version:  "v1alpha1",
		Resource: "Dataset",
	}

	unstr, err := dynamicClient.Resource(datasetGVR).List(context.TODO(), v1.ListOptions{})

	fmt.Println(unstr)
}
