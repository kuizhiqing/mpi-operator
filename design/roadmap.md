# Roadmap — kmpi-operator

> Companion to `feature-summary.md` and `improvements.md`.
> Horizon: Q3 2026 → Q4 2027.

This roadmap is the *forward-looking* product/engineering plan for the fork. It
takes the upstream Kubeflow `ROADMAP.md` as a baseline and overlays the
priorities specific to this internal large-scale-training fork (heterogeneous
training, recovery, multi-tenant operations).

Items are bucketed by horizon (Now / Next / Later) and by theme. Each item
links to a concrete deliverable in `plan.md`.

---

## Themes

1. **API & UX** — make the CRD honest, declarative, and Kubeflow v1-aligned.
2. **Reliability & Fault Tolerance** — recover.sh as a first-class feature,
   restart policy correctness, large-scale resilience.
3. **Scale & Performance** — informer hygiene, controller throughput, large
   job topology (>1000 ranks).
4. **Scheduling & Heterogeneity** — Horker/Heter graduation, gang-scheduler
   parity, topology-aware scheduling.
5. **Observability** — metrics, events, structured logs, traces.
6. **Security & Multi-tenancy** — RBAC scope-down, secret handling, namespace
   isolation, supply-chain.
7. **Ecosystem** — SDK, Helm, Kueue, Argo Workflows, Ray/PyTorch
   distributed training operators.
8. **Build / CI / Release** — multi-arch images, signed releases, e2e gating.

---

## Now — H2 2026 (next 3 months)

### API & UX
- **A1.** Promote `Horker` and the `recover` / `frozen` / `launcher-as-worker`
  annotations into typed CRD fields under `spec.runPolicy` and
  `spec.mpiReplicaSpecs[*]` so the contract is discoverable. Keep the
  annotations as a deprecated fallback for one release.
- **A2.** Make `slotsPerWorker` validation enforce `> 0` (currently allows
  zero via `ValidateNonnegativeField`).
- **A3.** Add `RestartPolicyExitCode` to the validator's allow-list (it's
  documented in `types.go` but rejected by `validation.go:40-43`).

### Reliability & Fault Tolerance
- **R1.** Promote `mpirun-recover.sh` from "annotation opt-in" to a `recovery`
  block in `MPIJobSpec` with knobs for: max restart attempts, kill-signal
  escalation timeout, peer-barrier timeout, environ-resync interval. Keep the
  recover script behavior identical for compatibility, but make the values
  configurable instead of hard-coded.
- **R2.** Replace the hard-coded `largeScaleThold = 200` /
  `restartLimitThold = 100` / `noRestartExitCode = 222` constants
  (`mpi_job_controller.go:99-101`) with `--flags` and document the heuristics.
- **R3.** Stable hostname-based hostfile (already used) → add unit-test
  coverage for the path where workers race their first DNS resolution
  (`barrier()` in `mpirun-recover.sh`).

### Scale & Performance
- **S1.** Split `mpi_job_controller.go` (1858 LoC) into `reconciler.go`,
  `pods.go`, `services.go`, `secrets.go`, `configmaps.go`,
  `lifecycle.go`. No behavior change.
- **S2.** Bump default `--kube-api-qps`/`--kube-api-burst` from 5/10 to
  100/200 (the controller already uses 200/2000 in its workqueue rate
  limiter — these defaults under-provision the Kubernetes client).

### Observability
- **O1.** Add per-replica-type metrics:
  `mpi_operator_pod_phase{type,phase}`, `mpi_operator_reconcile_duration`,
  `mpi_operator_reconcile_errors_total{reason}`,
  `mpi_operator_workqueue_depth`.
- **O2.** Switch logging from `k8s.io/klog` to `klog/v2` and emit structured
  key-values everywhere (the controller still mixes `klog.Infof` and `klog.V`
  styles).

### Build / CI
- **C1.** Cosign-sign release images. Publish provenance (SLSA L2).
- **C2.** Run e2e on a matrix: {OpenMPI, Intel, MPICH} × {Volcano,
  scheduler-plugins, none} × k8s {1.27, 1.29, 1.31}.

---

## Next — Q1–Q2 2027

### API & UX
- **A4.** Align with Kubeflow Training v2 API conventions (`kubeflow.org/v1`
  common types, JobSet integration if v1 lands).
- **A5.** Optional admission webhook (validating + defaulting) so users get
  immediate `kubectl apply` errors instead of post-hoc events.

### Reliability & Fault Tolerance
- **R4.** Pluggable failure classifier: instead of a single
  `noRestartExitCode = 222`, allow users to configure exit-code → action maps
  per replica type.
- **R5.** Checkpoint-aware retries: integrate with a CheckpointPolicy so a
  recover loop can include "restore checkpoint X before restart".

### Scale & Performance
- **S3.** Sharded reconcile queue per `MPIImplementation` (or per gang
  scheduler) so a large `Volcano` queue stall does not block `OpenMPI` jobs.
- **S4.** Pod adoption (vs. recreation) on operator restart for long-running
  MPIJobs.
- **S5.** Optionally back the launcher with `batch/v1.Job` (Indexed) so the
  Job controller handles backoff. Already discussed in
  `proposals/scalable-robust-operator.md`.

### Scheduling & Heterogeneity
- **H1.** First-class **Horker** semantics: scheduler-aware
  `affinity/tolerations` defaults, `topologyKey` configuration, separate
  `slotsPerHorker`.
- **H2.** Replace the cross-job IP-list sync (`heter-controller`) with a
  single-MPIJob-with-multiple-pools model, eliminating one binary and one
  failure mode.
- **H3.** Kueue integration — emit a `Workload` so the cluster admin can
  manage queueing/preemption from Kueue rather than Volcano-only.
- **H4.** Topology-aware scheduling hints (NUMA / NVLink / RDMA fabric) for
  the launcher and the worker `PodSpec`s.

### Observability
- **O3.** OpenTelemetry traces around reconcile, with span links to created
  Pods / PodGroups.
- **O4.** Per-job `events.k8s.io/v1` event quality pass — currently many
  reconciler branches log without emitting an Event.

### Security & Multi-tenancy
- **SE1.** Generate per-job SSH keys with explicit rotation (currently
  static for the lifetime of the job).
- **SE2.** Drop the `pods/exec` permission from the launcher's RBAC when
  `discover_hosts.sh` is not used (Elastic Horovod still needs it).
- **SE3.** Tighten ClusterRole on the operator itself —
  `manifests/base/cluster-role.yaml` currently grants broad pod/secret
  permissions.

### Ecosystem
- **E1.** Helm chart published to a public OCI repo (closes upstream
  `#11`).
- **E2.** SDK parity: regenerate `sdk/python/v2beta1` against the latest
  CRD; add async client and typed status helpers.

---

## Later — H2 2027

### API & UX
- **A6.** GraduateMPIJob to `v1` (stable) once the Kubeflow umbrella aligns.
- **A7.** `MPIJobTemplate` resource for reusable workload templates (blueprints
  imported from Argo).

### Reliability
- **R6.** Cluster-aware fault domains: detect node/AZ failures and
  proactively reschedule before launcher times out.
- **R7.** Live elasticity beyond Elastic Horovod — generic "shrink to N"
  signal that the controller can drive.

### Scale & Performance
- **S6.** Multi-cluster MPIJob (federation) for >10k-rank jobs that span
  clusters.
- **S7.** Rank-zero scheduling guarantees for stragglers (already partial via
  `priorityClass`).

### Ecosystem
- **E3.** Integrate with [Ray](https://github.com/ray-project/kuberay) as an
  alternate launcher backend.
- **E4.** Native PyTorch DDP / FSDP launcher (without Horovod), reusing the
  ConfigMap + sshd plumbing.

### Build / CI / Release
- **C3.** Move e2e to a real GPU runner (NVIDIA + AMD) to actually exercise
  NCCL / RCCL paths in the recover script env-var matrix.

---

## What we're explicitly *not* doing

* No HPC scheduler integrations (Slurm, PBS) — out of scope; Kubernetes is
  the only target.
* No abandoning of OpenMPI/Intel/MPICH triad in favor of a single
  implementation — broad image compatibility is a stated requirement.
* No on-host MPI daemons — everything stays in-Pod and ssh-based.

---

## How items connect

```
A1 ─┐
    ├─► R1 ─► R4 ─► R6
A2 ─┤
A3 ─┘                       S1 ─► S3 ─► S6
                                   ▲
H1 ─► H2 ─► H4                    S2 ─► S4
            ▲                          ▲
           E1                          O1 ─► O3
```

Concrete sequencing of these items is in `plan.md`.
