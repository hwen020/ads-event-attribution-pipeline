# Verification and handoff

## Scope and status

The original Master Task, `AGENTS.md`, standalone handoff and acceptance checklist were not present in the repository. This handoff uses the tracked README, prior verification evidence and the user's acceptance requirements. The separate Python Lab was left untouched.

| Requirement | Implementation and evidence | Status |
|---|---|---|
| Java 21 pipeline and transaction/offset boundary | Spring Boot 4.2.0 API publishes with `acks=all`; the consumer commits the PostgreSQL transaction before manually acknowledging Kafka. | Implemented; Maven verification passed locally. |
| Duplicate, conflict and concurrent delivery safety | Integration tests cover repeated events, concurrent duplicate IDs, first-payload-wins conflicts and no duplicate attribution/revenue. | Implemented and passed locally. |
| Rollback and post-commit crash recovery | Integration tests inject an attribution write failure and check rollback; the commit-before-ack test verifies committed state from an independent JDBC connection before simulating a crash, then confirms redelivery advances the offset without duplicate business rows. | Implemented and passed locally. |
| Late-event attribution repair | Tests compare arrival orders and assert the prior and corrected campaign credit, including metrics derived from persisted state. | Implemented and passed locally. |
| Build and static analysis | Java 21 `mvn verify` passed (2 unit tests, 10 Kafka/PostgreSQL Testcontainers integration tests). SpotBugs 4.9.3.0 passed with zero findings. Docker Compose configuration and a prebuilt application image build passed locally. | Passed locally; final GitHub CI pending. |
| Dependency security scan | On the current patched dependency graph, CycloneDX generated 84 components; Trivy 0.76.2 identified 85 packages, loaded its vulnerability database and reported zero vulnerabilities across UNKNOWN/LOW/MEDIUM/HIGH/CRITICAL. Maven resolves lz4-java 1.11.4 and snappy-java 1.1.10.9. | Passed locally; final CI artifact pending. |
| Compose service outage/recovery and offset rewind | Existing GitHub run below covers worker restart, PostgreSQL and Kafka outages, replay and consumer-group rewind, but is from an earlier application commit/runtime. A fresh Boot 4.2 fault drill is pending. | Historical evidence only; rerun pending. |
| Current-runtime performance | Six prior trials and their raw JSON are preserved below and match the GitHub artifact, but belong to commit `707c6f94a541a3b3229ab6bf1badfcb9710f3bb6` before the Spring Boot 4.2.0 migration. Re-running the six workloads on the final runtime is pending. | Historical baseline; final-runtime rerun pending. |
| PR, merge and release | At this checkpoint the local feature branch is one commit ahead of `origin/feat/distributed-attribution-mvp`; no PR or Release exists. Changes remain uncommitted. | Pending. |

The local dependency-scan inputs and raw report are in ignored `target/` files. CI now generates and validates the BOM, verifies Trivy database loading and non-empty package discovery, preserves the JSON report and scanner log, records hashes for the SBOM and built JAR, and uploads these with the JAR. The existing HIGH/CRITICAL gate remains enforced; empty, malformed or failed scans fail the job. The final workflow artifact and result must be checked after push.

## Historical performance evidence

The [GitHub benchmark and fault-drill run](https://github.com/hwen020/ads-event-attribution-pipeline/actions/runs/45284275111) used commit `707c6f94a541a3b3229ab6bf1badfcb9710f3bb6`. The six tracked JSON files in [`benchmarks/`](benchmarks/) were checked against that run's artifact by SHA-256. Conditions were Ubuntu 26.04.1 LTS x86-64, 4 logical CPUs, 16,759,431,168 Docker-visible bytes (15.61 GiB), Kafka 3.9.1 (one KRaft broker, six partitions), PostgreSQL 17, one API, and one worker with one or three consumer threads. Each cell ran once, sequentially, with a single in-flight HTTP request and batches of at most 200 events. Event IDs were unique; there were no injected duplicates or late events.

| Events submitted/persisted | Consumers | Elapsed seconds | Persisted events/s | Receipt-to-database P50/P95/P99 seconds | API acknowledgment errors | Sampled peak committed lag |
|---:|---:|---:|---:|---:|---:|---:|
| 1,000 | 1 | 3.288 | 304.11 | 1.91 / 2.53 / 2.62 | 0% | 6 |
| 10,000 | 1 | 18.263 | 547.55 | 8.96 / 15.42 / 15.99 | 0% | 6,280 |
| 100,000 | 1 | 105.538 | 947.53 | 52.20 / 95.46 / 99.07 | 0% | 96,357 |
| 1,000 | 3 | 3.455 | 289.41 | 2.40 / 2.81 / 2.84 | 0% | 0 |
| 10,000 | 3 | 9.044 | 1,105.72 | 4.94 / 7.09 / 7.23 | 0% | 1,297 |
| 100,000 | 3 | 59.922 | 1,668.83 | 27.48 / 51.42 / 53.45 | 0% | 91,872 |

`observed_events_per_second` is persisted rows divided by wall time from the first request through observed persistence. The workflow does not separately measure drain-only time. P50/P95/P99 measure API receipt (`received_at`) to database processing (`processed_at`), including Kafka queue wait; they are not HTTP acknowledgment latency. In particular, 51.42 seconds is the P95 of this end-to-end processing interval for the 100,000-event, three-consumer trial, not API response time. Lag is periodically sampled and may miss short peaks. No production-scale capacity, repeatability or steady-state claim follows from these single trials. These results are not evidence for the upgraded Spring Boot runtime.

## Fault-recovery evidence

The prior Compose run above reported that:

- A stopped worker accumulated broker records and processed the backlog after restart and group rejoin.
- PostgreSQL downtime delayed processing; after recovery, a late eligible click corrected purchase credit and redelivery preserved one result and USD 1.2500.
- Kafka downtime returned HTTP 503; retrying the same event IDs after recovery preserved the stored result.
- A token-protected replay completed after an API restart; rewinding the active Kafka group returned committed lag to zero without changing event counts or conversion credit.

These are historical results from the prior application runtime. The fresh fault workflow on the final commit must pass before these are reported as current-release evidence.

## Startup and demonstration

Use the commands and API example in [README.md](README.md). In brief: copy `.env.example` to `.env` if needed, run `docker compose up --build --wait`, submit `python scripts/benchmark.py 1000 --run <fresh-run-id>`, inspect metrics and Grafana, then stop with `docker compose down`. The optional `docker compose down --volumes` removes only this Compose project's synthetic data and should be used only when a reset is intended.

## Remaining release checklist

- [ ] Push the tested feature commit and open/update the PR without force-pushing over remote work.
- [ ] Confirm every required CI check for the PR commit is completed and successful; queued, running, skipped or cancelled checks do not count.
- [ ] Run the Compose fault drill and six benchmark trials on the final Spring Boot 4.2 commit; retain raw artifacts and record their run links and commit.
- [ ] Merge only after checking for new remote commits and all required checks are green.
- [ ] Wait for required checks on the final `main` commit; create the versioned Release against that exact commit with the built JAR, SBOM and scan report.
- [ ] Record final commit, PR, Actions and Release links here, verify local/remote synchronization, and confirm the worktree is clean.
