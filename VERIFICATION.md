# Verification and handoff

## Scope and status

The original Master Task, `AGENTS.md`, standalone handoff and acceptance checklist were not present in the repository. This handoff uses the tracked README, prior verification evidence and the user's acceptance requirements. The separate Python Lab was left untouched.

| Requirement | Implementation and evidence | Status |
|---|---|---|
| Java 21 pipeline and transaction/offset boundary | Spring Boot 4.2.0 API publishes with `acks=all`; the consumer commits the PostgreSQL transaction before manually acknowledging Kafka. | Implemented; Maven verification passed locally. |
| Duplicate, conflict and concurrent delivery safety | Integration tests cover repeated events, concurrent duplicate IDs, first-payload-wins conflicts and no duplicate attribution/revenue. | Implemented and passed locally. |
| Rollback and post-commit crash recovery | Integration tests inject an attribution write failure and check rollback; the commit-before-ack test verifies committed state from an independent JDBC connection before simulating a crash, then confirms redelivery advances the offset without duplicate business rows. | Implemented and passed locally. |
| Late-event attribution repair | Tests compare arrival orders and assert the prior and corrected campaign credit, including metrics derived from persisted state. | Implemented and passed locally. |
| Build and static analysis | Java 21 `mvn verify` passed (2 unit tests, 10 Kafka/PostgreSQL Testcontainers integration tests). SpotBugs 4.9.3.0 passed with zero findings. Docker Compose configuration and a prebuilt application image build passed locally. The [main-branch Pipeline CI](https://github.com/hwen020/ads-event-attribution-pipeline/actions/runs/45562418454) for merge commit `7931da215b99f4e1674a71f012954cb4a091cefe` passed integration tests, SpotBugs, dependency audit, Compose validation and image build. |
| Dependency security scan | The main-branch CI run for application merge commit `7931da215b99f4e1674a71f012954cb4a091cefe` generated an 84-component CycloneDX SBOM; Trivy 0.76.2 identified 85 packages, loaded its vulnerability database and reported zero vulnerabilities across UNKNOWN/LOW/MEDIUM/HIGH/CRITICAL. Its [auditable artifact](https://github.com/hwen020/ads-event-attribution-pipeline/actions/runs/45562418454/artifacts/14999070520) contains the SBOM, full report, scanner log, summary, checksum manifest and built JAR. Summary SHA-256 values: SBOM `ae54a5a90648aff496aed0497adbcf8510046d37f6ec79128177172b35414970`; JAR `735ff7f3b168579a90ad2dfb59ed6607d896e7f8798fa184bed3032e85fd99ba`. | Passed on that commit. |
| Compose service outage/recovery and offset rewind | The fresh [Boot 4.2 fault and benchmark run](https://github.com/hwen020/ads-event-attribution-pipeline/actions/runs/45560523604) passed worker restart/backlog, PostgreSQL and Kafka outages, duplicate and late-event checks, replay after API restart, offset rewind/catch-up, final count/credit assertions, Grafana provisioning and live Prometheus verification. | Passed on runtime commit `24f51f5d1a10b924974488656afac17522bce9bf`. |
| Current-runtime performance | All six Boot 4.2 trials completed on the same runtime commit; raw per-trial JSON is in the [benchmark artifact](https://github.com/hwen020/ads-event-attribution-pipeline/actions/runs/45560523604/artifacts/14998091159). | Passed; details below. |
| PR, merge and release | [PR #1](https://github.com/hwen020/ads-event-attribution-pipeline/pull/1) was merged after rechecking the remote base and all PR checks. Main commit `7931da215b99f4e1674a71f012954cb4a091cefe` passed its required checks. The versioned release record is [v1.0.0](https://github.com/hwen020/ads-event-attribution-pipeline/releases/tag/v1.0.0); confirm its tag points to the final main commit and its assets come from that commit's successful CI artifact. | Release is gated on final-main CI and artifact verification. |

The application-merge-commit dependency artifact was downloaded and independently verified: its summary commit matches `7931da215b99f4e1674a71f012954cb4a091cefe`, its SBOM/JAR hashes match the files, its checksum manifest validates, and its raw report and scanner log confirm 84 components, 85 scanned packages, zero vulnerabilities and successful database loading. The CI gate scans all severity levels but fails on any HIGH/CRITICAL, empty or malformed inputs, missing DB-load evidence, scanner errors or non-empty scan failure. The [main branch history](https://github.com/hwen020/ads-event-attribution-pipeline/commits/main) and [main Pipeline CI runs](https://github.com/hwen020/ads-event-attribution-pipeline/actions/workflows/ci.yml?query=branch%3Amain) are the live record for the final documentation-only follow-up commit.

The initial CI run warned that `actions/checkout@v4`, `actions/setup-java@v4` and `actions/upload-artifact@v4` were using a deprecated Node.js runtime and that setup-java v4 would no longer receive updates. The workflows now use the upstream current major tags, verified from the official release pages: [checkout v7.0.1](https://github.com/actions/checkout/releases/tag/v7.0.1), [setup-java v6.3.0](https://github.com/actions/setup-java/releases/tag/v6.3.0) and [upload-artifact v7.0.2](https://github.com/actions/upload-artifact/releases/tag/v7.0.2). Push and PR CI both passed with these tags.

## Current-runtime performance evidence

The [six-trial run and raw JSON artifact](https://github.com/hwen020/ads-event-attribution-pipeline/actions/runs/45560523604) used application/runtime commit `24f51f5d1a10b924974488656afac17522bce9bf`, Spring Boot 4.2.0 and Java 21. It ran the Dockerfile-built `ads-attribution:local` image from the Maven-prebuilt JAR. The hosted runner reported Ubuntu 26.04.1 LTS x86-64, Linux kernel `7.0.0-1020-azure`, 4 logical CPUs and 16,759,431,168 Docker-visible bytes (15.61 GiB). The stack used Kafka 3.9.1 (one KRaft broker, six partitions), PostgreSQL 17, one API, and one worker with one or three consumer threads. Each cell ran once, sequentially, with unique event IDs, no injected duplicates or late events, one in-flight request and batches of at most 200.

| Events submitted/persisted | Consumers | Elapsed seconds | Persisted events/s | Receipt-to-database P50/P95/P99 seconds | API acknowledgment errors | Sampled peak committed lag |
|---:|---:|---:|---:|---:|---:|---:|
| 1,000 | 1 | 3.628 | 275.66 | 1.416 / 2.326 / 2.486 | 0% | 410 |
| 10,000 | 1 | 16.456 | 607.69 | 7.869 / 13.686 / 13.729 | 0% | 7,118 |
| 100,000 | 1 | 107.547 | 929.82 | 49.689 / 94.369 / 97.904 | 0% | 86,791 |
| 1,000 | 3 | 2.260 | 442.45 | 1.170 / 1.771 / 1.806 | 0% | 0 |
| 10,000 | 3 | 8.908 | 1,122.53 | 4.167 / 6.891 / 7.112 | 0% | 3,584 |
| 100,000 | 3 | 55.889 | 1,789.27 | 24.377 / 45.295 / 46.964 | 0% | 88,656 |

Across the six runs, all 222,000 requested events persisted and the API acknowledgment error rate was 0%. For 100,000 events, three consumers increased observed persisted throughput from 929.82 to 1,789.27 events/s (+92.4%) and lowered measured P95 from 94.369 to 45.295 seconds (-52.0%). This is a single comparison, not a statistically repeated result.

`observed_events_per_second` is persisted rows divided by wall time from the first request through observed persistence. The workflow does not separately measure drain-only time. P50/P95/P99 measure API receipt (`received_at`) to database processing (`processed_at`), including Kafka queue wait; they are not HTTP acknowledgment latency. API accepted-event throughput and request/acknowledgment latency were not independently measured. Lag is periodically sampled and may miss short peaks. No production-scale capacity, repeatability or steady-state claim follows from these trials.

## Historical performance baseline

The [GitHub benchmark and fault-drill run](https://github.com/hwen020/ads-event-attribution-pipeline/actions/runs/45284275111) used commit `707c6f94a541a3b3229ab6bf1badfcb9710f3bb6`. The six tracked JSON files in [`benchmarks/`](benchmarks/) were checked against that run's artifact by SHA-256. Conditions were Ubuntu 26.04.1 LTS x86-64, 4 logical CPUs, 16,759,431,168 Docker-visible bytes (15.61 GiB), Kafka 3.9.1 (one KRaft broker, six partitions), PostgreSQL 17, one API, and one worker with one or three consumer threads. Each cell ran once, sequentially, with a single in-flight HTTP request and batches of at most 200 events. Event IDs were unique; there were no injected duplicates or late events.

| Events submitted/persisted | Consumers | Elapsed seconds | Persisted events/s | Receipt-to-database P50/P95/P99 seconds | API acknowledgment errors | Sampled peak committed lag |
|---:|---:|---:|---:|---:|---:|---:|
| 1,000 | 1 | 3.288 | 304.11 | 1.91 / 2.53 / 2.62 | 0% | 6 |
| 10,000 | 1 | 18.263 | 547.55 | 8.96 / 15.42 / 15.99 | 0% | 6,280 |
| 100,000 | 1 | 105.538 | 947.53 | 52.20 / 95.46 / 99.07 | 0% | 96,357 |
| 1,000 | 3 | 3.455 | 289.41 | 2.40 / 2.81 / 2.84 | 0% | 0 |
| 10,000 | 3 | 9.044 | 1,105.72 | 4.94 / 7.09 / 7.23 | 0% | 1,297 |
| 100,000 | 3 | 59.922 | 1,668.83 | 27.48 / 51.42 / 53.45 | 0% | 91,872 |

`observed_events_per_second` is persisted rows divided by wall time from the first request through observed persistence. The workflow does not separately measure drain-only time. P50/P95/P99 measure API receipt (`received_at`) to database processing (`processed_at`), including Kafka queue wait; they are not HTTP acknowledgment latency. In particular, 51.42 seconds is the P95 of this end-to-end processing interval for the 100,000-event, three-consumer trial, not API response time. Lag is periodically sampled and may miss short peaks. No production-scale capacity, repeatability or steady-state claim follows from these single trials. These results predate the Boot 4.2.0 migration and are retained only as a historical baseline.

## Fault-recovery evidence

The current [Boot 4.2 Compose run](https://github.com/hwen020/ads-event-attribution-pipeline/actions/runs/45560523604) passed the repository fault script and validated worker backlog recovery, database outage/recovery with duplicate and late arrivals, Kafka downtime returning HTTP 503, replay completion after an API restart, and offset rewind/catch-up with preserved counts and credit. The older [baseline run](https://github.com/hwen020/ads-event-attribution-pipeline/actions/runs/45284275111) also reported that:

- A stopped worker accumulated broker records and processed the backlog after restart and group rejoin.
- PostgreSQL downtime delayed processing; after recovery, a late eligible click corrected purchase credit and redelivery preserved one result and USD 1.2500.
- Kafka downtime returned HTTP 503; retrying the same event IDs after recovery preserved the stored result.
- A token-protected replay completed after an API restart; rewinding the active Kafka group returned committed lag to zero without changing event counts or conversion credit.

The workflow's successful fault step and live dashboard checks are current-run evidence; the older detailed results remain historical.

## Startup and demonstration

Use the commands and API example in [README.md](README.md). In brief: copy `.env.example` to `.env` if needed, run `docker compose up --build --wait`, submit `python scripts/benchmark.py 1000 --run <fresh-run-id>`, inspect metrics and Grafana, then stop with `docker compose down`. The optional `docker compose down --volumes` removes only this Compose project's synthetic data and should be used only when a reset is intended.

## Publication record

[PR #1](https://github.com/hwen020/ads-event-attribution-pipeline/pull/1) merged as application commit `7931da215b99f4e1674a71f012954cb4a091cefe`; its [main-branch CI](https://github.com/hwen020/ads-event-attribution-pipeline/actions/runs/45562418454) passed. The [v1.0.0 release page](https://github.com/hwen020/ads-event-attribution-pipeline/releases/tag/v1.0.0) is the publication record. The release tag must resolve to the final main commit after this handoff update, and the JAR, SBOM, Trivy JSON/log/summary and checksum manifest must come from that exact commit's successful CI artifact. The live main-history and CI links above provide the commit/run record without changing release-tagged source after publication.
