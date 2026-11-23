# Verification evidence

## Compose and failure recovery

The [Compose run](https://github.com/hwen020/ads-event-attribution-pipeline/actions/runs/45284275111) built and started the real stack at commit `707c6f94a541a3b3229ab6bf1badfcb9710f3bb6`. Its fault drill passed these checks.

- A stopped worker accumulated new broker records and processed the backlog after restarting and rejoining its group.
- PostgreSQL went offline while the API published touches. After the database restarted, the eligible late click corrected the purchase credit and repeat deliveries preserved one result and USD 1.2500.
- Kafka downtime produced HTTP 503. Resubmitting the same conversion and click IDs after recovery preserved the stored result.
- A token-protected replay job reached completion after an API restart. A Kafka consumer-group rewind separately recovered to zero committed lag for active partitions and preserved event counts and conversion credit.

The Maven integration suite starts Kafka and PostgreSQL with Testcontainers. It exercises direct HTTP ingress, two independent producers, late and out-of-order attribution, duplicate event and conversion IDs, expired touches and purchases, a DLQ record, conversion and campaign metrics, replay with a persisted cursor, and manual acknowledgment around the database commit boundary.

The initial local container build and unit tests ran under Java 21. Windows Docker Engine stopped responding during service image setup, so Compose, fault results and performance measurements below are from GitHub Actions.

## Benchmarks

The benchmark runner used Ubuntu 26.04.1 LTS (x86-64), 4 logical CPUs, and 16,759,431,168 bytes of Docker-visible memory (15.61 GiB). It ran one Kafka 3.9.1 KRaft broker with six partitions, one PostgreSQL 17 instance, one API process, and one worker process configured with one or three consumer threads. Each configuration ran one trial per workload. See the [raw measurement JSON](benchmarks/) and the [recorded GitHub run](https://github.com/hwen020/ads-event-attribution-pipeline/actions/runs/45284275111).

| Events sent and persisted | Consumers | Elapsed s | Persisted events/s | P50 s | P95 s | P99 s | API ack errors | Sampled peak commit lag |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 1,000 | 1 | 3.288 | 304.11 | 1.91 | 2.53 | 2.62 | 0% | 6 |
| 10,000 | 1 | 18.263 | 547.55 | 8.96 | 15.42 | 15.99 | 0% | 6,280 |
| 100,000 | 1 | 105.538 | 947.53 | 52.20 | 95.46 | 99.07 | 0% | 96,357 |
| 1,000 | 3 | 3.455 | 289.41 | 2.40 | 2.81 | 2.84 | 0% | 0 |
| 10,000 | 3 | 9.044 | 1,105.72 | 4.94 | 7.09 | 7.23 | 0% | 1,297 |
| 100,000 | 3 | 59.922 | 1,668.83 | 27.48 | 51.42 | 53.45 | 0% | 91,872 |

Throughput is persisted rows divided by wall time from the first batch through observed drain. Percentiles measure API receipt to database processing and include queue wait. The lag gauge samples broker offsets and can miss short spikes. Each run submitted up to 200 events per HTTP call with one in-flight request. Datasets ran sequentially on the same stack with distinct synthetic UUIDs, so startup, polling resolution, and warming can affect the comparison.

Three consumers improved observed 100,000-event throughput by **76.1%** and reduced the measured P95 from 95.46 to 51.42 seconds. Additional consumer threads can overlap work on separate Kafka partitions, while sharing broker, PostgreSQL, CPU, and storage capacity. The 1,000-event run was slightly slower with three consumers. Scheduling and measurement overhead weigh more heavily on short runs. The 100,000-event runs both accumulated substantial lag, which explains the difference between throughput during drain and the latency of an individual event. A single trial per cell limits conclusions about steady-state scaling.
