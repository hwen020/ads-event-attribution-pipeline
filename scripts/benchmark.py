"""Run from the repository root with Compose active. Uses only synthetic IDs."""
import argparse
from datetime import datetime, timezone
import json
import pathlib
import platform
import re
import subprocess
import time
from urllib import request
from uuid import UUID, uuid5

NAMESPACE = UUID("72f08c1f-c6b8-4889-83b9-59099bb5d00f")


def shell(*args):
    return subprocess.check_output(args, text=True).strip()


def database(sql):
    return shell("docker", "compose", "exec", "-T", "postgres", "psql", "-U", "ads", "-d", "ads",
                 "-At", "-v", "ON_ERROR_STOP=1", "-c", sql)


def event_set(size, run, now):
    def identifier(label):
        return str(uuid5(NAMESPACE, f"{run}/{label}"))

    campaign = identifier("campaign")
    for index in range(size):
        group = index // 3
        kind = ("IMPRESSION", "CLICK", "CONVERSION")[index % 3]
        event = dict(eventId=identifier(index), userId=identifier(f"user-{group}"),
                     source=f"bench-{run}", type=kind, eventTime=now)
        if kind == "CONVERSION":
            event.update(conversionId=identifier(f"conversion-{group}"), conversionValue=1.25, currency="USD")
        else:
            event.update(campaignId=campaign, adId=identifier("ad"))
        yield event


def submit(url, events):
    body = json.dumps(events).encode()
    req = request.Request(url + "/api/v1/events/batch", data=body,
                          headers={"Content-Type": "application/json"})
    with request.urlopen(req, timeout=30) as response:
        result = json.load(response)
        if response.status != 202 or result["acknowledged"] != len(events):
            raise RuntimeError(f"Unexpected broker acknowledgment: {result}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("events", type=int)
    parser.add_argument("--run", required=True, help="Unique run identifier, e.g. c1-n1000")
    parser.add_argument("--url", default="http://localhost:8080")
    parser.add_argument("--timeout", type=int, default=300)
    args = parser.parse_args()
    if not re.fullmatch(r"[a-z0-9-]{1,36}", args.run) or args.events <= 0:
        parser.error("use positive events and a run ID of 1–36 lowercase letters, digits or hyphens")

    condition = f"payload::jsonb #>> '{{event,source}}' = 'bench-{args.run}'"
    if int(database(f"SELECT count(*) FROM raw_events WHERE {condition}")):
        raise SystemExit("Use a fresh --run ID for each benchmark")

    now = datetime.now(timezone.utc).isoformat()
    events = list(event_set(args.events, args.run, now))
    packets = [events[i:i + 200] for i in range(0, args.events, 200)]
    started = time.perf_counter()
    failed = 0
    observed_lag = []

    def sample_lag():
        try:
            with request.urlopen(args.url + "/actuator/prometheus", timeout=5) as response:
                for line in response.read().decode().splitlines():
                    if line.startswith("ads_consumer_lag "):
                        value = int(float(line.split()[1]))
                        if value >= 0:
                            observed_lag.append(value)
        except OSError:
            pass

    for index, packet in enumerate(packets):
        try:
            submit(args.url, packet)
        except (OSError, ValueError, RuntimeError) as error:
            failed += len(packet)
            print(f"Batch acknowledgment failed: {error}", flush=True)
        if index % 20 == 0:
            sample_lag()

    while time.perf_counter() - started < args.timeout:
        count = int(database(f"SELECT count(*) FROM raw_events WHERE {condition}"))
        if count >= args.events - failed:
            break
        sample_lag()
        time.sleep(1)

    elapsed = time.perf_counter() - started
    sample_lag()
    measured = database(f"""
        SELECT count(*),
            percentile_cont(ARRAY[0.50,0.95,0.99]) WITHIN GROUP (ORDER BY
                extract(epoch FROM processed_at-received_at)*1000)
        FROM raw_events WHERE {condition};
    """)
    persisted, latencies = measured.split("|")
    persisted = int(persisted)
    output = {
        "run": args.run, "events": args.events, "persisted": persisted,
        "docker_cpu_memory_os": shell("docker", "info", "--format",
                                     "{{.NCPU}} CPUs / {{.MemTotal}} bytes / {{.OperatingSystem}}"),
        "host": platform.platform(), "seconds": round(elapsed, 3),
        "observed_events_per_second": round(persisted / elapsed, 2),
        "db_latency_ms_p50_p95_p99": json.loads(latencies.replace("{", "[").replace("}", "]")) if latencies else None,
        "ack_error_rate": failed / args.events, "periodic_sample_peak_committed_lag": max(observed_lag, default=None),
        "worker_concurrency": shell("docker", "compose", "exec", "-T", "worker", "printenv", "WORKER_CONCURRENCY"),
        "partition_count": len(re.findall(r"Partition: [0-9]+", shell("docker", "compose", "exec", "-T", "kafka",
            "/opt/kafka/bin/kafka-topics.sh", "--bootstrap-server", "kafka:29092",
            "--describe", "--topic", "ads.events.v1")))
    }
    print(json.dumps(output, indent=2))
    path = pathlib.Path("benchmark-results")
    path.mkdir(exist_ok=True)
    (path / (args.run + ".json")).write_text(json.dumps(output, indent=2) + "\n", encoding="utf-8")
    if failed or persisted != args.events:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
