# kresilient-training-operator Design Docs

> Generated: 2026-05-25.
>
> This directory is the single source of truth for the engineering plan of
> the **kresilient-training-operator** fork. It captures *what we have*, *where we want to
> go*, *what's broken or under-baked*, and *the sequenced plan to get there*.

## Documents

| File | Purpose |
|------|---------|
| [`feature-summary.md`](./feature-summary.md) | Inventory of every feature in the codebase today. CRD, controllers, scripts, metrics, deployment, SDK, governance. Use it as the authoritative "what does this thing do?" reference. |
| [`roadmap.md`](./roadmap.md) | Forward-looking themes and items bucketed by horizon (Now / Next / Later). Strategy-level. |
| [`improvements.md`](./improvements.md) | Concrete, code-level fixes and refactors with file:line citations and P0/P1/P2 prioritization. Start here for "what should I pick up next?" |
| [`plan.md`](./plan.md) | Phased, executable implementation plan. Five phases, week-level estimates, deliverables, exit criteria, risks, success metrics. |

Read in this order if you're new: `feature-summary.md` → `roadmap.md`
→ `improvements.md` → `plan.md`.

---

## One-Page Executive Summary

### What kresilient-training-operator is

A Kubernetes operator that owns the `kubeflow.org/v2beta1.ResilientJob` CRD and runs
allreduce-style distributed training (Horovod, TensorFlow, PyTorch+Horovod,
custom MPI binaries). It is an **internally extended fork** of upstream
`kuizhiqing/resilient-training-operator` with three categories of additions:

1. **Heterogeneous workers** — a third replica role (`Horker`) and a
   second binary (`heter-controller`) that wires two ResilientJobs together via
   IP-list annotations.
2. **Fault-tolerant execution** — a `mpirun-recover.sh` shell that wraps the
   user command in a peer-barrier + restart loop, propagates a curated set
   of NCCL/UCX/RDMA/InfiniBand env vars to ssh-launched ranks, and
   escalates SIGTERM→SIGKILL on stuck Python processes.
3. **Operational ergonomics** — `kubeflow.org/frozen` reconcile pause,
   namespace include/exclude scoping, `RestartPolicyExitCode`,
   `LauncherCreationPolicy=WaitForWorkersReady`.

### Where it stands

* ~10.8 KLoC Go + 449 LoC embedded shell. 1858-line monolithic main
  controller. Three MPI impls (OpenMPI / Intel / MPICH). Two gang
  schedulers (Volcano, scheduler-plugins).
* Several **fork features are documented in types.go but not in the
  validator** (e.g. `RestartPolicyExitCode`), and several **annotations
  drive behavior that should be typed CRD fields** (`recover`, `frozen`,
  `launcher-as-worker`).
* No reconcile-duration / error-rate / queue-depth metrics; observability
  is limited to four Prometheus counters/gauges.
* Default `--kube-api-qps=5` / `--kube-api-burst=10` significantly
  under-provisions the controller's own internal rate limiter (200/2000).
* Test coverage is solid for happy paths but the recover script has no
  tests of its own.

### Where it's going

The roadmap commits to four near-term outcomes:

1. **Honest API** — promote the fork's annotations into typed `ResilientJobSpec`
   fields (Phase 1 of `plan.md`).
2. **Resilient at scale** — parallel Pod creation, generation-aware
   informer filtering, OTel tracing, reconcile metrics (Phase 2).
3. **First-class heterogeneity** — collapse the two-ResilientJob Horker model
   into one CRD and integrate with Kueue (Phase 3).
4. **Safe to run in shared clusters** — RBAC scope-down, signed releases,
   multi-arch images, Helm OCI delivery (Phase 4).

### Critical path (next 8 weeks)

1. Fix validator gaps (`RestartPolicyExitCode`, `slotsPerWorker=0`).
2. Embed shell scripts (`go:embed`) and add bats tests for
   `mpirun-recover.sh`.
3. Split the 1858-line `mpi_job_controller.go` (no behavior change).
4. Move magic constants (`largeScaleThold`, `restartLimitThold`,
   `noRestartExitCode`) to flags.
5. Add reconcile-duration + workqueue metrics.
6. Bump default Kube API QPS/Burst to 100/200.

These six items are P0/P1 in `improvements.md` and constitute Phase 0 +
the head of Phase 2 in `plan.md`.

---

## How to use these docs

* **Engineer picking up work?** → `improvements.md` → pick a P0 or P1 →
  cross-check against the matching phase in `plan.md`.
* **PM/lead planning a quarter?** → `plan.md` for phase-level scope;
  `roadmap.md` for the strategic context.
* **New maintainer onboarding?** → `feature-summary.md` first, then read
  `pkg/controller/mpi_job_controller.go` with the §3.1 section open.
* **Reviewer evaluating a PR?** → check it against the relevant
  improvement ID and phase deliverable.

---

## Maintenance

These documents should be updated when:

* A roadmap or improvement item is completed → mark it done in `plan.md`
  and remove from `improvements.md`.
* A new feature lands → add it to `feature-summary.md` first, then
  retire any roadmap item it implements.
* The fork rebases against upstream → re-audit the §11 "Fork-Specific
  Capabilities" table in `feature-summary.md`.

Cadence: review the four docs at the start of each quarter; rewrite
sections that drift.
