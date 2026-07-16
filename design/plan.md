# Implementation Plan — kresilient-training-operator

> Companion to `feature-summary.md`, `roadmap.md`, `improvements.md`.
> This is the *executable* plan: phased, with deliverables, exit criteria,
> and risks.

The plan organizes the roadmap items and improvements into five phases. Each
phase is sized to one quarter (≈12 weeks) for a 2-engineer team. Phases are
sequential except where called out.

---

## Phase 0 — Stabilize the Fork (Weeks 1–4)

**Theme:** stop the bleeding. Fix correctness bugs and make the fork
debuggable before adding features.

### Deliverables

| ID | Deliverable | Source | Effort |
|----|-------------|--------|--------|
| 0.1 | Validator allow-lists `RestartPolicyExitCode` and rejects `slotsPerWorker=0`. Unit tests added. | P0-1, P0-2 | 0.5d |
| 0.2 | Decide & document `frozen` semantics around finished/cleanup; lock in with tests. | P0-3 | 1d |
| 0.3 | Embed `mpirun-wrapper.sh` and `mpirun-recover.sh` via `//go:embed`. Add `make verify-shellcheck`, `make verify-shfmt` to CI. | P1-5 | 2d |
| 0.4 | Bats/shellspec test suite for `mpirun-recover.sh` covering: env-var propagation, kill-python escalation, peer barrier, environ resync, exit-code retry loop. | §4 | 5d |
| 0.5 | Move `coverage.out` / `profile.cov` to `.gitignore`; emit from CI; gate PRs on a coverage delta. | P2-8 | 1d |
| 0.6 | Standardize on `klog/v2`; remove per-dequeue noise logs. | P1-13 | 2d |
| 0.7 | Migrate from deprecated `k8s.io/utils/pointer` to `k8s.io/utils/ptr`. | §10 | 1d |
| 0.8 | Refactor `mpi_job_controller.go` into the file split listed in `improvements.md` §P1-3. **No behavior change.** Land as separate PR. | P1-3 | 5d |

### Exit criteria

* All `_test.go` pass with the new file layout.
* `make test` and `make test_e2e` green on the matrix
  {OpenMPI, Intel, MPICH} × {none, volcano} × k8s-1.29.
* Zero deprecated-API lint warnings.

### Risks

* Embedding scripts changes the build — ensure `KUBEBUILDER_ASSETS` and
  Dockerfile copy-instructions are consistent.
* Refactor merges can stall on long PRs; cap each refactor PR at 500 LoC
  diff.

---

## Phase 1 — API Promotion & Defaults (Weeks 5–10)

**Theme:** make the contract honest. Move annotations into typed CRD fields
without breaking existing users.

### Deliverables

| ID | Deliverable | Source | Effort |
|----|-------------|--------|--------|
| 1.1 | Add `ResilientJobSpec.Recovery` block: `enabled`, `maxAttempts`, `peerBarrierTimeout`, `pythonKillGracePeriod`. Defaults preserve current `mpirun-recover.sh` behavior. | A1, R1 | 4d |
| 1.2 | Add `ResilientJobSpec.RunPolicy.Frozen *bool`; treat existing `kubeflow.org/frozen` as a deprecated fallback that logs a warning. | A1 | 1d |
| 1.3 | Add `MPIReplicaSpec.LauncherAsWorker *bool` and remove the same-named annotation in favor of it. | A1 | 1d |
| 1.4 | Promote `largeScaleThold`, `restartLimitThold`, `noRestartExitCode` to `ResilientJobSpec.RunPolicy` fields **and** controller `--flag` defaults. | P1-4, R2 | 3d |
| 1.5 | Defaulting + validation for all new fields; OpenAPI regen; CRD bump. | A2, A3, P1-6, P1-7, P1-8 | 3d |
| 1.6 | Conversion / migration notes. Run a smoke test that an existing v2beta1 ResilientJob (annotation form) still works. | – | 2d |
| 1.7 | Optional admission webhook (validating). Behind a flag; off by default. | A5 | 5d |
| 1.8 | Regenerate `sdk/python/v2beta1`; bump version. | E2 | 2d |

### Exit criteria

* New typed fields covered by validator + defaulter + unit tests.
* Old annotations still accepted (compatibility); warning event recorded.
* SDK installs from sdist and runs the existing `tensorflow-mnist.py` example.
* Webhook (optional) passes its own e2e in KIND.

### Risks

* CRD-schema breakage: stage with `kubectl diff` against a real cluster
  before release.
* SDK regen typically produces noisy diffs; budget for a review pass.

---

## Phase 2 — Reliability & Performance (Weeks 11–18)

**Theme:** earn the right to scale. Make reconciliation fast, observable,
and resilient.

### Deliverables

| ID | Deliverable | Source | Effort |
|----|-------------|--------|--------|
| 2.1 | Reconcile metrics: `mpi_operator_reconcile_duration_seconds{result}`, `mpi_operator_reconcile_errors_total{reason}`, `mpi_operator_workqueue_depth`, `mpi_operator_pod_phase{type,phase}`. | O1, P1-12 | 3d |
| 2.2 | OpenTelemetry tracing hooks around reconcile and Pod-creation. Off unless `OTEL_EXPORTER_OTLP_ENDPOINT` is set. | O3 | 4d |
| 2.3 | Bump default `--kube-api-qps`/`--kube-api-burst` to 100/200; document. | P1-9, S2 | 0.5d |
| 2.4 | `errgroup`-parallel worker Pod creation, capped at 32. Add a benchmark test. | P1-11 | 3d |
| 2.5 | Replace per-pod `Delete` loop with `DeleteCollection` using job-label selector. | P2-3 | 1d |
| 2.6 | Generation/RV-aware filtering in `handleObjectUpdate` to avoid thundering herd. | P1-10 | 3d |
| 2.7 | Patch (not Update) for `heter-controller`'s annotation propagation. | P1-1 | 2d |
| 2.8 | Pod adoption on operator restart: identify orphaned worker Pods by job label and re-attach. | S4 | 5d |
| 2.9 | Failure-domain-aware retry: classify launcher failures by reason and skip restart for permanent ones. | R4 | 4d |
| 2.10 | Event-quality pass — every reconcile branch that logs also emits a `corev1.Event`. | O4 | 3d |

### Exit criteria

* Reconcile p99 ≤ 500 ms for a 1000-worker ResilientJob (measure under KIND with
  emulated workers).
* No regressions in the OpenMPI/Intel/MPICH matrix e2e.
* Metrics dashboards (Grafana JSON) committed to `manifests/observability/`.

### Risks

* Parallel Pod creation can amplify API-server pressure if QPS/Burst defaults
  aren't bumped first; sequence 2.3 before 2.4.
* `DeleteCollection` deletes are non-graceful — verify finalizers first.

---

## Phase 3 — Heterogeneity & Scheduling (Weeks 19–26)

**Theme:** make Horker first-class and modernize the scheduler integration.

### Deliverables

| ID | Deliverable | Source | Effort |
|----|-------------|--------|--------|
| 3.1 | Promote `Horker` to a typed peer of `Worker`: dedicated `slotsPerHorker`, `affinity` defaults, `topologyKey`. | H1 | 5d |
| 3.2 | Single-ResilientJob heterogeneous model: `MPIReplicaType=PrimaryWorker / Horker` colocated in one CRD. Mark the `heter-controller` binary deprecated for removal in the next minor. | H2 | 8d |
| 3.3 | Kueue `Workload` emission (behind a flag). Validation against Kueue v0.6+. | H3 | 5d |
| 3.4 | Topology-aware scheduling hints: surface NUMA/NVLink/RDMA fabric topologies via Pod labels and gang-scheduler `minResources`. | H4 | 5d |
| 3.5 | Horker example under `examples/v2beta1/heter/`. | P2-6 | 2d |
| 3.6 | E2E coverage for the Horker path on Volcano *and* scheduler-plugins. | C2 | 3d |

### Exit criteria

* `examples/v2beta1/heter/` runs end-to-end on KIND with Volcano.
* `heter-controller` migration guide published.
* Kueue integration verified against an upstream Kueue release.

### Risks

* Removing `heter-controller` may break internal users; gate on an
  explicit deprecation cycle (≥ 1 minor).
* Kueue API churn — track upstream release notes for breakage.

---

## Phase 4 — Security, Multi-tenancy, Release (Weeks 27–32)

**Theme:** make the operator safe to run in shared clusters and ship signed
artifacts.

### Deliverables

| ID | Deliverable | Source | Effort |
|----|-------------|--------|--------|
| 4.1 | Per-job SSH key rotation field (`spec.runPolicy.sshKeyRotation: { period: 24h }`); regenerate Secret on schedule. | SE1, P1-14 | 4d |
| 4.2 | Conditional `pods/exec` RBAC: only granted when `recovery.elastic = true`. | SE2, P1-15 | 2d |
| 4.3 | Tighten operator ClusterRole: drop unused verbs, scope `secrets` to `create`. | SE3, P1-16 | 2d |
| 4.4 | Cosign signing + SBOM publication in release workflow. | C1, P1-17 | 3d |
| 4.5 | Multi-arch image release: `linux/amd64,linux/arm64` for operator + base + 3 MPI impls. | C3, P1-21 | 2d |
| 4.6 | Helm chart published to OCI (`oci://ghcr.io/...`). | E1 | 3d |
| 4.7 | Documentation: README rewritten with fork-extensions section, architecture diagram, ROADMAP.md replaced with link to `design/`. | P1-18, P1-19, P2-7 | 3d |
| 4.8 | Cut a `v0.5.0` release; communicate breaking changes (deprecated annotations, deprecated `heter-controller`). | – | 2d |

### Exit criteria

* All release artifacts are signed and verifiable with `cosign verify`.
* `helm install kresilient-training-operator oci://...` works against k8s 1.27, 1.29, 1.31.
* SBOM published per image.

### Risks

* OCI Helm hosting choice is org-dependent; pick early.
* Tightening RBAC can break hidden integrations — feature-flag with
  `--legacy-rbac` for one release.

---

## Cross-cutting workstreams (run in parallel)

* **CI matrix expansion (C2)** — ongoing, owner: build/CI engineer.
* **Documentation refresh (P1-18, P1-19, P2-6, P2-7)** — ongoing, owner:
  whoever lands user-visible features.
* **Dependency upgrades (§10)** — bi-weekly Renovate sweep.

---

## Capacity assumptions

* 2 engineers, 80% on this work, 20% slippage.
* 1 SRE for Phase 4.
* Reviewer pool: 3 maintainers per `OWNERS`.

If only 1 engineer is available, halve the throughput: Phases 0+1 in
Q3 2026, Phase 2 in Q4 2026, Phases 3+4 across H1 2027.

---

## Success metrics

| Metric | Baseline (today) | Target after Phase 4 |
|--------|------------------|----------------------|
| Reconcile p99 (1000-worker job, KIND) | unmeasured | ≤ 500 ms |
| MTTR for a flapping launcher | manual restart | recover-loop with bounded attempts |
| CRD field coverage of fork features | 0% (annotations only) | ≥ 90% |
| E2E matrix size | 1 (OpenMPI, no scheduler) | 9 (3 × 3) |
| Release artifact signatures | 0 | 100% |
| Operator ClusterRole verb count | broad | ≤ 50% of today |
| Fork-specific docs in README | 0 lines | full section + diagram |

---

## Out-of-scope (parking lot)

* Slurm/PBS bridges.
* Replacing OpenMPI with a single implementation.
* Federated multi-cluster ResilientJobs (revisit in 2027).

---

## Open questions

1. Does this fork still want to track upstream `kuizhiqing/resilient-training-operator`
   releases, or fully diverge? Phase 1's CRD changes lean toward divergence.
2. Volcano vs. scheduler-plugins long-term: should one be the default and
   the other "best-effort"?
3. Recovery semantics for non-Python user commands (the recover script
   currently hard-codes `pkill python`).
4. Owner of the operator's container image registry post-Phase 4.
