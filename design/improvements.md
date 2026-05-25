# Improvements — kmpi-operator

> Companion to `feature-summary.md` and `roadmap.md`.
> Concrete, code-level improvements ranked by *impact ÷ effort*. Each item
> includes a file-level pointer so an engineer can pick it up cold.

Legend:
* **P0** — correctness / data-loss / security; do soon.
* **P1** — high-value cleanup or capability.
* **P2** — quality-of-life.

---

## 1. Correctness & API hygiene

### P0-1 — `RestartPolicyExitCode` is documented but rejected by the validator
* **Where:** `pkg/apis/kubeflow/validation/validation.go:40-43`
* **Symptom:** `validRestartPolicies` only contains `Never` and `OnFailure`;
  `RestartPolicyAlways` and `RestartPolicyExitCode` are documented in
  `types.go:347-359` but `ValidateMPIJob` will fail any spec using them.
* **Fix:** Add the missing entries (or, for `Always`, decide whether it is
  intentionally banned and remove it from the type docs).

### P0-2 — `slotsPerWorker = 0` is allowed
* **Where:** `pkg/apis/kubeflow/validation/validation.go:69-73`
* **Symptom:** `ValidateNonnegativeField` accepts `0`, which produces an
  empty hostfile and an undebuggable launcher hang.
* **Fix:** Reject `0` with a `field.Invalid`; default has been `1` since
  `default.go`.

### P0-3 — Frozen check happens *after* `DeletionTimestamp`, *before* the
launcher-finished cleanup
* **Where:** `pkg/controller/mpi_job_controller.go:568-579`
* **Symptom:** A frozen finished job will not get its TTL GC; a frozen
  failing job won't get its CompletionTime set.
* **Fix:** Decide whether `frozen` should also gate cleanup. Document
  explicitly. Add a unit test asserting current behavior either way.

### P1-1 — Heter-controller race on annotation update
* **Where:** `pkg/controller/heter_job_controller.go:263-274`
* **Symptom:** `updateLauncherJobAnnotation` does a full `Update` (not
  `Patch`), which conflicts with concurrent status updates from the main
  controller; on conflict it silently returns the error and is re-enqueued.
* **Fix:** Use `client.Patch` with `JSONPatch` for the single annotation, or
  switch to server-side apply with a stable field manager.

### P1-2 — `getStatusIPList` quietly drops empty strings
* **Where:** `pkg/controller/heter_job_controller.go:334-362`
* **Symptom:** `Selector` strings come from elsewhere; if any of the three
  replica selectors are empty, the resulting list omits the role-trio
  ordering, which may matter to downstream consumers.
* **Fix:** Either format with placeholders (`launcher=,worker=…`) or reject
  the partially-empty case loudly.

---

## 2. Refactor opportunities

### P1-3 — Split the 1858-line `mpi_job_controller.go`
* Suggested split (no behavior change):
  * `controller.go` — type, ctor, run loop, queue plumbing.
  * `reconcile.go` — `syncHandler` and its helpers.
  * `pods.go`, `services.go`, `secrets.go`, `configmaps.go`,
    `podgroups.go` — single-resource-type create/update/delete helpers.
  * `lifecycle.go` — completion / cleanup / TTL.
  * `metrics.go` — Prometheus declarations.
* Why: makes review of changes possible; makes it easy to identify dead code
  paths (we suspect a few given the controller's age).

### P1-4 — Magic numbers
* `mpi_job_controller.go:98-100` — `noRestartExitCode = 222`,
  `largeScaleThold = 200`, `restartLimitThold = 100`.
* Promote to flags (`--restart-large-scale-threshold`,
  `--no-restart-exit-code`, …) **or** to fields on `MPIJobSpec.RunPolicy`
  for per-job override.

### P1-5 — Embedded shell scripts as Go strings
* `mpirun-wrapper.sh` and `mpirun-recover.sh` are read from disk at runtime.
  Make them `//go:embed`-d so the binary is hermetic.
* Add `make verify-shfmt` and `make verify-shellcheck` to keep them honest.

### P2-1 — `controllerAgentName = "mpi-job-controller"` shared across
controllers (`mpi_job_controller.go:61`, used by `heter_job_controller.go`).
* Rename `heter`'s recorder source to `mpi-heter-controller` so events are
  attributable.

### P2-2 — Inconsistent receiver-name style
* Mix of `c *MPIJobController` and `c *HeterJobController`. Fine. But helper
  functions like `getStatusIPList` are package-level despite being
  controller-internal.

---

## 3. Validation & defaulting gaps

### P1-6 — `LauncherCreationPolicy` enum not validated by the runtime
validator
* **Where:** `pkg/apis/kubeflow/v2beta1/types.go:170-172` — the kubebuilder
  marker enforces it via OpenAPI on the apiserver, but
  `pkg/apis/kubeflow/validation/validation.go` doesn't. If a user
  patches the field via a stale client without OpenAPI checks, an invalid
  value silently flows through.
* **Fix:** Add an explicit allow-list check.

### P1-7 — `Horker` validation is identical to `Worker`
* `validation.go:104-114` — `validateMPIReplicaSpecs` uses the same
  `validateReplicaSpec` for both. Add a dedicated path that confirms
  `Horker.Replicas >= 0` and that `Horker` requires `MPIImplementation` to
  support it (today it does for OpenMPI/Intel; MPICH integration story is
  uncertain).

### P1-8 — Defaults for new fields are scattered
* `default.go` should default `LauncherCreationPolicy=AtStartup`,
  `MPIImplementation=OpenMPI`, `SSHAuthMountPath=/root/.ssh`. Check that
  defaulters run for all three and add a unit test guarding the matrix.

---

## 4. Test coverage

| Area | State | Suggestion |
|------|-------|------------|
| `MPIJobController` | 1397-line `_test.go`; covers happy paths, suspend, gang scheduling | Add table tests for the `frozen`, `recover`, `launcher-as-worker` annotation matrix |
| `HeterJobController` | 512-line `_test.go` | Add a flake-prone race test: peer Job updated mid-reconcile |
| `validation_test.go` | 388 LoC | Add cases for P0-1 / P0-2 above |
| `mpirun-recover.sh` | None | Add a Bats (or shellspec) test suite that runs the script under a fake `/etc/mpi/environ`, asserts retry behavior, kill behavior, env-var propagation |
| `mpi_job_host_list.go` | Tested | Confirm Horker hostname formatting is locked down |
| End-to-end | KIND-based, full matrix | Add a recover-path E2E using a `pi` binary that exits non-zero N-1 times |

`coverage.out` (100K) and `profile.cov` (168K) are checked in but should be
generated artifacts. Move to `.gitignore` and emit from CI.

---

## 5. Performance & scalability

### P1-9 — Default Kube API QPS/Burst is too low
* `cmd/mpi-operator/app/options/options.go:85-86` — defaults `5`/`10`.
* The internal workqueue is sized at `200/2000` but it can't keep that pace if
  the API client throttles. Bump to `100/200` (or align with the workqueue).

### P1-10 — `handleObjectUpdate` enqueues on *every* update
* `mpi_job_controller.go:392-411` — config map / secret / service / pod
  events all trigger a re-reconcile of the owning MPIJob. For a 1000-pod job
  that's a thundering herd at startup. Add a resource-version cache or
  generation-based filter.

### P1-11 — Pod creation is sequential
* The reconciler creates worker Pods one at a time inside the `syncHandler`.
  For high-fan-out jobs this serializes against API latency. Use parallel
  goroutines with a small `errgroup` (cap at e.g. 32).

### P2-3 — `cleanUpPods` deletes in a loop
* Replace per-pod `Delete` with a single `DeleteCollection` using the job
  label selector.

---

## 6. Observability

### P1-12 — No metric for reconcile duration / errors / queue depth
* Add the standard controller-runtime style metrics:
  * `mpi_operator_reconcile_duration_seconds{result}`
  * `mpi_operator_reconcile_errors_total{reason}`
  * `mpi_operator_workqueue_depth`
  * `mpi_operator_workqueue_adds_total`

### P1-13 — Mixed `klog` and `klog.V` levels
* Standardize on `klog/v2`; convert verbose key-value logs.
* Stop logging `Current queue length %d` on every dequeue
  (`mpi_job_controller.go:485` + heter equivalent at `:190`) — that's
  per-reconcile noise, not signal.

### P2-4 — `mpi_operator_job_info` gauge cleanup on delete
* Confirm `mpi_job_controller_status.go` deletes the gauge series when the
  MPIJob is GC'd; otherwise the metric grows unbounded over a long-lived
  controller.

---

## 7. Security & supply chain

### P1-14 — Static SSH keypair per job, never rotated
* Generated once when the Secret is created, then reused for the lifetime of
  the job. For long-running (days/weeks) jobs add a rotation field.

### P1-15 — Launcher RBAC includes `pods/exec`
* Required only for Elastic Horovod's `discover_hosts.sh + kubectl exec` flow.
  Make it conditional on `kubeflow.org/elastic` or equivalent spec field.

### P1-16 — Operator ClusterRole is over-broad
* `manifests/base/cluster-role.yaml` — audit and downscope. Specifically,
  drop `secrets` write to only `create` (it never updates after the initial
  generation).

### P1-17 — Image supply chain
* Sign images with cosign, publish SBOMs (`syft`), and include
  `slsa-github-generator` provenance.

### P2-5 — Use `securityContext: { runAsNonRoot: true }` in launcher /
worker templates
* Currently the wrapper script `passwd -d root` and `chpasswd` assume root.
  Document this constraint or split into a non-root path.

---

## 8. Documentation

### P1-18 — README does not document the fork-only features
* No mention of Horker, recover, frozen, heter-controller. Add a "Fork
  extensions" section that links to the new typed fields.

### P1-19 — `ROADMAP.md` is stale
* References issues `#9`, `#11`, `#12`, `#20`, `#90`, `#138`, `#159` — most
  are years old. Replace with this `design/` set or summarize and link.

### P2-6 — `examples/v2beta1/` lacks a Horker example
* Add `examples/v2beta1/heter/` with both halves of a heterogeneous pair.

### P2-7 — No architecture diagram
* Add `design/diagrams/architecture.png` (or mermaid) showing controller →
  ConfigMap/Secret/Service/Pod relationships and the heter-controller
  cross-job link.

---

## 9. CI / Build

### P1-20 — E2E does not gate PRs in this fork
* Confirm `.github/workflows/` runs `make test_e2e`. If it doesn't, gate it
  on a label (`needs-e2e`) so heavy runs are opt-in.

### P1-21 — Multi-arch is partial
* `arm.dockerfile` exists but Makefile `PLATFORMS=linux/amd64` by default.
  Add `make images PLATFORMS=linux/amd64,linux/arm64` to release flow.

### P2-8 — `coverage.out` / `profile.cov` checked in
* See §4. Add to `.gitignore`, generate in CI.

### P2-9 — `third_party_licenses/` directory
* Confirm it's regenerated by tooling and not hand-edited.

---

## 10. Dependency hygiene

* Pin `volcano.sh/apis` and `sigs.k8s.io/scheduler-plugins` to versions
  matching tested k8s minors (Makefile already extracts them but pin in CI).
* `k8s.io/utils/pointer` is deprecated upstream — migrate to `k8s.io/utils/ptr`.
* `k8s.io/klog` (v1) is in deprecated mode; migrate to `klog/v2`.

---

## Sequencing recommendation

1. **Fix the validator gaps (P0-1, P0-2)** — one-line code change, prevents
   foot-guns.
2. **Promote magic constants to flags / spec fields (P1-4)** — unblocks
   support for non-default cluster sizes.
3. **Refactor `mpi_job_controller.go` (P1-3)** — required before non-trivial
   feature work.
4. **Embed shell scripts (P1-5) + add bats tests (§4)** — locks down the
   fork's most fragile surface.
5. **Observability (P1-12)** — needed before we touch reconcile concurrency.
6. **Concurrency improvements (P1-10, P1-11)** — measurable wins for
   large-scale jobs.
7. **Security pass (P1-14 → P1-17)** — multi-tenant readiness.
8. **Docs + examples (P1-18 → P2-7)** — last, once the surface stabilizes.

Detailed phasing is in `plan.md`.
