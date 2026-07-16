# Feature Summary — kresilient-training-operator

> Snapshot date: 2026-05-25
> Repo: `github.com/kuizhiqing/resilient-training-operator` (this checkout: `kresilient-training-operator`, an
> internally-extended fork of Kubeflow's Resilient Training Operator)
> Go version: see `go.mod` · CRD version: `kubeflow.org/v2beta1`

This document inventories every user-visible and operator-visible capability
in the codebase as it stands today. It is the canonical reference used by
`roadmap.md`, `improvements.md`, and `plan.md`.

---

## 1. Mission & Scope

The Resilient Training Operator runs *allreduce-style distributed training* (Horovod,
TensorFlow, PyTorch + Horovod, custom MPI binaries) on Kubernetes. It owns the
`ResilientJob` CRD and converts it into the Pods/Services/ConfigMaps/Secrets/PodGroups
required to bootstrap an MPI ring across a launcher and N workers, with
optional heterogeneous workers.

This fork adds large-scale-training-specific capabilities on top of upstream
v2beta1: a heterogeneous "Horker" replica role, a fault-tolerant
`mpirun-recover.sh` execution shell, freeze/unfreeze reconciliation, a separate
`heter-controller` binary that wires two ResilientJobs together, and namespace
scoping for multi-tenant clusters.

---

## 2. CRD & API Surface (`pkg/apis/kubeflow/v2beta1`)

### 2.1 `ResilientJob` top-level fields

| Field | Type | Default | Notes |
|-------|------|---------|-------|
| `spec.slotsPerWorker` | `*int32` | `1` | Slots per worker hostfile entry |
| `spec.runPolicy` | `RunPolicy` | – | See §2.2 |
| `spec.mpiReplicaSpecs` | `map[MPIReplicaType]*ReplicaSpec` | required | Launcher / Worker / Horker |
| `spec.sshAuthMountPath` | `string` | `/root/.ssh` | Where the SSH secret is mounted |
| `spec.launcherCreationPolicy` | enum | `AtStartup` | `AtStartup` \| `WaitForWorkersReady` |
| `spec.mpiImplementation` | enum | `OpenMPI` | `OpenMPI` \| `Intel` \| `MPICH` |

### 2.2 `RunPolicy`

* `cleanPodPolicy` — `All` \| `Running` \| `None`
* `ttlSecondsAfterFinished` — TTL GC of finished jobs
* `activeDeadlineSeconds` — wall-clock timeout
* `backoffLimit` — max launcher restarts (per-job; clamped by operator
  `--restart-limit`)
* `suspend` — pause the job, deletes Pods/PodGroups, resets `startTime`
* `schedulingPolicy` — gang scheduling knobs

### 2.3 `SchedulingPolicy` (gang scheduling)

* `minAvailable` — `PodGroup.spec.minMember`
* `queue` — Volcano queue
* `minResources` — scheduler-plugins minimum-resource set
* `priorityClass` — Volcano priority class (also surfaced to scheduler-plugins
  via PriorityClass lookup)
* `scheduleTimeoutSeconds` — scheduler-plugins timeout

### 2.4 Replica types

* `Launcher` — singleton, runs `mpirun`
* `Worker` — homogeneous workers (DNS-named `<job>-worker-<idx>`)
* `Horker` — *fork-specific* heterogeneous workers (different hardware shape,
  e.g. CPU vs GPU mix, or pinned to a different node pool)

### 2.5 `RestartPolicy`

* `Always`, `OnFailure`, `Never`
* `ExitCode` — fork-specific:
  * exit codes 1–127 → permanent error, no restart
  * exit codes 128–255 → retryable, pod is restarted

### 2.6 Status

* Conditions: `Created` → `Running` → (`Restarting`) → `Succeeded` \| `Failed`
  \| `Suspended`
* `replicaStatuses[type]`: `active`, `succeeded`, `failed`, `selector`
* `startTime`, `completionTime`, `lastReconcileTime`

---

## 3. Controllers (`pkg/controller`)

### 3.1 `ResilientJobController` — `mpi_job_controller.go` (1858 LoC)

Primary reconciler. Watches ResilientJobs plus dependent ConfigMaps, Secrets,
Services, Pods, PodGroups, PriorityClasses.

Reconcile responsibilities:

1. Validate the spec (`pkg/apis/kubeflow/validation`).
2. Honor the `kubeflow.org/frozen` annotation — reconciler is a no-op while set
   (lets operators pause without changing spec).
3. Create the headless `Service` fronting the workers (and Intel/MPICH
   launcher).
4. Create the `mpi-job-config` `ConfigMap` (hostfile, `discover_hosts.sh`,
   environ, sshd config, ssh_config, recover marker).
5. Create the `mpi-job-config-mpirun-wrapper` ConfigMap with the embedded
   `mpirun-wrapper.sh` and `mpirun-recover.sh`.
6. Create the SSH-auth `Secret` (private/public keypair shared by launcher and
   all workers).
7. Optionally create a `PodGroup` (Volcano or scheduler-plugins) for gang
   scheduling.
8. Create worker / horker Pods up to `replicas`; delete trailing Pods on
   scale-down (Elastic Horovod).
9. Create the launcher Pod (or wait for workers to be `Ready` if
   `LauncherCreationPolicy=WaitForWorkersReady`).
10. Update conditions / replica statuses / Prometheus metrics.
11. On launcher completion: clean up dependents per `cleanPodPolicy`, schedule
    TTL GC.

Annotations (`kubeflow.org/`):

* `elastic` — enables `discover_hosts.sh` and dynamic worker addition/removal
* `recover` — runs the launcher under `mpirun-recover.sh` so the user command
  is restarted in-place on non-zero exit (with python-process kill, peer
  barrier, environ resync)
* `launcher-as-worker` — the launcher slot is also a worker (saves a pod)
* `frozen` — pause reconciliation
* `heter-job`, `heter-role`, `heter-ip-list` — see §3.2

Other heuristics (`mpi_job_controller.go:99-101`):

* `largeScaleThold = 200` — special handling above 200 workers
* `restartLimitThold = 100` — restart-limit cap
* `noRestartExitCode = 222` — sentinel exit code suppressing restart

### 3.2 `HeterJobController` — `heter_job_controller.go` (455 LoC)

Wires two ResilientJobs into a heterogeneous training pair (e.g. a CPU-launcher job
and a GPU-worker job that need each other's IP lists).

* Reads `kubeflow.org/heter-job` on each ResilientJob to find its peer.
* Reads `kubeflow.org/heter-role` to identify launcher vs. worker side.
* Copies the worker side's IP list into the launcher side's
  `kubeflow.org/heter-ip-list` annotation each tick.
* On delete-of-launcher, cascades the delete to the peer.
* Namespace-scopable via `--include-namespaces` / `--exclude-namespaces`.

### 3.3 `PodGroupControl` — `podgroup.go` (476 LoC)

Pluggable abstraction with two backends:

* `VolcanoCtrl` — `scheduling.volcano.sh/v1beta1.PodGroup`
* `SchedulerPluginsCtrl` — `scheduling.x-k8s.io/v1alpha1.PodGroup`

Selected at startup via `--gang-scheduling=volcano|scheduler-plugins|<name>`.

### 3.4 Supporting modules

| File | LoC | Purpose |
|------|-----|---------|
| `config_map.go` | 519 | Hostfile / sshd_config / ssh_config / environ / discover_hosts.sh rendering |
| `mpi_config.go` | 265 | Per-implementation env vars and command-line knobs |
| `mpi_job_host_list.go` | 199 | Hostname / slot list generation per replica type |
| `mpi_job_controller_status.go` | 174 | Condition transitions, completion logic |
| `mpi_job_utils.go` | 270 | Annotation / label helpers, namespace filtering |
| `mpirun-wrapper.sh` | 186 | Launcher entrypoint, default mode |
| `mpirun-recover.sh` | 263 | Launcher entrypoint with restart loop, used when `recover` annotation is set |

### 3.5 Embedded scripts

`mpirun-recover.sh` highlights:

* SIGTERM/SIGINT cleanup, kill-python-processes (SIGTERM then SIGKILL).
* Wait for `CHIEF_IP` to be populated by `/etc/mpi/environ`.
* Auto-extracts `consumer.tar` / `consumer.zip` if present.
* Sets up sshd and waits for all peers.
* Loops user command until success, with peer barrier between attempts.
* Re-exports a curated set of accelerator/network env vars to `/root/.bashrc`
  for SSH-launched ranks: NCCL/UCX/GLOO/NVSHMEM/DeepEP/BKCL/HCCL/RDMA/InfiniBand
  (broad multi-vendor coverage).

---

## 4. Binaries & CLI

### 4.1 `cmd/resilient-training-operator` flags

`--master`, `--kube-config`, `--namespace` (defaults to env
`KUBEFLOW_NAMESPACE`), `--threadiness` (2), `--restart-limit` (5),
`--monitoring-port`, `--gang-scheduling`, `--lock-namespace`, `--lock-name`,
`--kube-api-qps` (5), `--kube-api-burst` (10), `--exclude-namespaces`,
`--include-namespaces`, `--version`.

### 4.2 `cmd/heter-controller` flags

Subset of the above, focused on the heterogeneous reconciler.

### 4.3 Leader election

Both binaries support leader election via a Lease in `--lock-namespace` /
`--lock-name`.

---

## 5. Observability — Prometheus Metrics

| Metric | Type | Description |
|--------|------|-------------|
| `mpi_operator_jobs_created_total` | Counter | Jobs accepted |
| `mpi_operator_jobs_successful_total` | Counter | Jobs that reached `Succeeded` |
| `mpi_operator_jobs_failed_total` | Counter | Jobs that reached `Failed` |
| `mpi_operator_job_info` | Gauge | Labels: `launcher`, `namespace` |

Metrics are exposed on `--monitoring-port` (disabled when `0`).

---

## 6. Deployment Artifacts

### 6.1 `manifests/`

* `base/` — Deployment, ServiceAccount, ClusterRole, ClusterRoleBinding,
  CRD (`kubeflow.org_resilientjobs.yaml`), kustomization.
* `overlays/standalone/` — namespace + patch for standalone install.
* `overlays/kubeflow/` — overlay for Kubeflow umbrella distributions.
* `overlays/dev/` — local-dev patch (image swap via `IMAGE_NAME` /
  `IMAGE_TAG` template).

### 6.2 `deploy/v2beta1/`

* `resilient-training-operator.yaml` — single-file deployment manifest.

### 6.3 Examples (`examples/v2beta1/`)

* `tensorflow-benchmarks/` — multi-node TensorFlow CNN benchmark.
* `pi/` — π via OpenMPI / Intel MPI / MPICH (showcases all three impls).
* `horovod/` — TensorFlow MNIST + Elastic Horovod.

---

## 7. SDK (`sdk/python/v2beta1`)

A Python client generated from the OpenAPI schema. Includes:

* CRUD wrappers around `ResilientJob`.
* Generated docs.
* `tensorflow-mnist.py` end-to-end example.
* Test suite + `requirements.txt` / `test-requirements.txt`.

Generation script: `hack/python-sdk/`.

---

## 8. Build & Tooling

* **Makefile** — targets: `resilient-training-operator.v2`, `heter`, `images`, `test_images`,
  `test`, `test_e2e`, `dev_manifest`, `generate`, `verify-generate`,
  `scheduler-plugins-crd`, `volcano-scheduler-crd`, `kind`, `helm`.
* **Dockerfiles** — `Dockerfile` (amd64), `arm.dockerfile` (arm64),
  `build/base/*` family (base, OpenMPI, IntelMPI, MPICH images).
* **Codegen** — `hack/update-codegen.sh` regenerates clientset, informers,
  listers, deepcopy, defaulters, OpenAPI.
* **CRD manifests** — `controller-gen` via `hack/generate-manifest.sh`.
* **Boilerplate** — `hack/boilerplate/`.
* **CI** — `.github/workflows/` (build badge in README).

---

## 9. Testing

| Layer | Path | Notes |
|-------|------|-------|
| Unit | `pkg/controller/*_test.go` | 2,468 LoC across 6 files |
| Unit | `pkg/apis/.../validation_test.go`, `default_test.go` | API-layer correctness |
| Integration | `test/integration/` | Uses `envtest` Kubebuilder assets |
| E2E | `test/e2e/` | KIND + Volcano + scheduler-plugins; runs π examples on all 3 MPI impls |

Coverage profile: `coverage.out` / `profile.cov` are checked in (~268 KB
combined — flag for cleanup).

---

## 10. Governance & Process

* `OWNERS` — maintainer list (Kubeflow Prow conventions).
* `CONTRIBUTING.md`, `ADOPTERS.md`, `RELEASE.md`, `ROADMAP.md`.
* `prow_config.yaml` — Prow integration.
* License: Apache-2.0.

---

## 11. Fork-Specific Capabilities (vs. upstream `kuizhiqing/resilient-training-operator`)

| Feature | Upstream? | Where |
|---------|-----------|-------|
| `Horker` replica type | Fork | `types.go:192` |
| `mpirun-recover.sh` | Fork | `pkg/controller/mpirun-recover.sh` |
| `kubeflow.org/frozen` reconcile pause | Fork | `mpi_job_controller.go:576` |
| `kubeflow.org/launcher-as-worker` | Fork-extended | `mpi_job_controller.go:96` |
| `kubeflow.org/recover` annotation flow | Fork | `mpi_job_controller.go:95` |
| `RestartPolicyExitCode` | Fork | `types.go:354` |
| `heter-controller` binary + IP-list sync | Fork | `cmd/heter-controller/`, `pkg/controller/heter_job_controller.go` |
| `--include-namespaces` / `--exclude-namespaces` | Fork | `options.go:88-92` |
| `largeScaleThold` / `restartLimitThold` / `noRestartExitCode` heuristics | Fork | `mpi_job_controller.go:98-100` |
| Multi-vendor RDMA env propagation in recover script | Fork | `mpirun-recover.sh:101-119` |

---

## 12. Headline Numbers

* ~10.8 KLoC Go (controller + tests).
* 449 LoC embedded shell.
* 3 MPI implementations · 2 gang schedulers · 3 replica roles.
* 2 binaries · 1 CRD · 4 Prometheus series.
