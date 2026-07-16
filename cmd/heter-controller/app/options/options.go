// Copyright 2019 The Kubeflow Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package options

import (
	"flag"
	"os"

	"github.com/kuizhiqing/resilient-training-operator/pkg/apis/kubeflow/v2beta1"
)

// ServerOption is the main context object for the controller manager.
type ServerOption struct {
	Kubeconfig        string
	MasterURL         string
	Threadiness       int
	MonitoringPort    int
	PrintVersion      bool
	Namespace         string
	LockNamespace     string
	LockName          string
	QPS               int
	Burst             int
	ExcludeNamespaces string
	IncludeNamespaces string
}

// NewServerOption creates a new CMServer with a default config.
func NewServerOption() *ServerOption {
	s := ServerOption{}
	return &s
}

// AddFlags adds flags for a specific CMServer to the specified FlagSet.
func (s *ServerOption) AddFlags(fs *flag.FlagSet) {
	fs.StringVar(&s.MasterURL, "master", "",
		`The url of the Kubernetes API server,
		 will overrides any value in kubeconfig, only required if out-of-cluster.`)

	fs.StringVar(&s.Kubeconfig, "kube-config", "",
		"Path to a kubeConfig. Only required if out-of-cluster.")

	fs.StringVar(&s.Namespace, "namespace", os.Getenv(v2beta1.EnvKubeflowNamespace),
		`The namespace to monitor resilientjobs. If unset, it monitors all namespaces cluster-wide. 
                If set, it only monitors resilientjobs in the given namespace.`)

	fs.IntVar(&s.Threadiness, "threadiness", 2,
		`How many threads to process the main logic`)

	fs.BoolVar(&s.PrintVersion, "version", false, "Show version and quit")

	fs.IntVar(&s.MonitoringPort, "monitoring-port", 0,
		`Endpoint port for displaying monitoring metrics. It can be set to "0" to disable the metrics serving.`)

	fs.StringVar(&s.LockNamespace, "lock-namespace", "resilient-training-operator", "Set locked namespace name while enabling leader election.")

	fs.StringVar(&s.LockName, "lock-name", "heter-controller", "Set locked name to distinct two deployments.")

	fs.IntVar(&s.QPS, "kube-api-qps", 5, "QPS indicates the maximum QPS to the master from this client.")
	fs.IntVar(&s.Burst, "kube-api-burst", 10, "Maximum burst for throttle.")

	fs.StringVar(&s.ExcludeNamespaces, "exclude-namespaces", "",
		"The namespaces to be exclude to take place. eg. kube-system,resilient-training-operator")

	fs.StringVar(&s.IncludeNamespaces, "include-namespaces", "",
		"The namespaces to be include to take place. Empty means all namespaces.")

}
