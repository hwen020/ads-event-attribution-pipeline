"""Exercise the running Compose project with real service stops and synthetic fixtures."""
import argparse
from datetime import datetime, timedelta, timezone
import json
import os
from uuid import uuid4
from urllib import error, request
from time import sleep, monotonic

from benchmark import database, event_set, shell, submit


def compose(*args):
    return shell("docker", "compose", *args)


def eventually(predicate, seconds=120):
    end = monotonic() + seconds
    while monotonic() < end:
        if predicate():
            return
        sleep(1)
    raise AssertionError("Timed out waiting for recovery")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--url", default="http://localhost:8080")
    args = parser.parse_args()
    run = "fault-" + uuid4().hex[:12]
    now = datetime.now(timezone.utc)
    view, click, conversion = event_set(3, run, now.isoformat())
    click["eventTime"] = (now - timedelta(seconds=1)).isoformat()
    condition = f"payload::jsonb #>> '{{event,source}}' = 'bench-{run}'"

    def count():
        return int(database(f"SELECT count(*) FROM raw_events WHERE {condition}"))

    def assert_credit():
        row = database(f"""
          SELECT count(*), sum(value), max(touch_id::text)
          FROM attribution_results WHERE conversion_id = '{conversion["conversionId"]}'
        """)
        assert row == f'1|1.2500|{click["eventId"]}', row

    # The stopped consumer leaves a backlog. Its replacement joins the same group.
    compose("stop", "worker")
    try:
        submit(args.url, [conversion])
        assert count() == 0
    finally:
        compose("start", "worker")
    eventually(lambda: count() == 1)

    # PostgreSQL stops while both producers and consumers remain active.
    compose("stop", "postgres")
    try:
        submit(args.url, [view, click, click])
        sleep(3)
    finally:
        compose("start", "postgres")
    eventually(lambda: count() == 3)
    assert_credit()

    # Once the broker returns, retries with the original IDs preserve the result.
    compose("stop", "kafka")
    try:
        try:
            submit(args.url, [click])
        except error.HTTPError as failure:
            if failure.code != 503:
                raise
        else:
            raise AssertionError("API unexpectedly acknowledged a stopped broker")
    finally:
        compose("start", "kafka")

    def retry():
        try:
            submit(args.url, [conversion, click])
            return True
        except OSError:
            return False

    eventually(retry)
    assert_credit()
    token = os.environ.get("REPLAY_TOKEN")
    if token:
        headers = {"Authorization": "Bearer " + token}
        from urllib.parse import urlencode
        params = urlencode({"from": (now - timedelta(minutes=1)).isoformat(),
                            "to": (now + timedelta(minutes=1)).isoformat()})
        uri = args.url + "/api/v1/replay/jobs"
        with request.urlopen(request.Request(uri + "?" + params, headers=headers, method="POST")) as response:
            job = json.load(response)["id"]
        compose("restart", "api")
        def replay_finished():
            try:
                with request.urlopen(request.Request(uri + "/" + job, headers=headers), timeout=5) as response:
                    return json.load(response)["status"] == "COMPLETE"
            except OSError:
                return False
        eventually(replay_finished)
        assert_credit()
        print("Replay completed after API restart")
    else:
        print("Replay restart check skipped because REPLAY_TOKEN is unset")
    # Rewind this isolated demo consumer group and wait for it to catch up again.
    group = ("/opt/kafka/bin/kafka-consumer-groups.sh", "--bootstrap-server", "kafka:29092",
             "--group", "ads-attribution")
    compose("stop", "worker")
    try:
        report = compose("exec", "-T", "kafka", *group, "--reset-offsets", "--topic", "ads.events.v1",
                "--to-earliest", "--execute")
        assert "Error:" not in report and report.count("ads.events.v1") == 6, report
    finally:
        compose("start", "worker")

    def caught_up():
        report = compose("exec", "-T", "kafka", *group, "--describe")
        rows = [line.split() for line in report.splitlines() if " ads.events.v1 " in line]
        return len(rows) == 6 and all(row[5] in ("0", "-") for row in rows)

    eventually(caught_up)
    assert count() == 3
    assert_credit()
    print("Passed worker restart, offset rewind, PostgreSQL and Kafka outages, duplicate and late arrival checks")


if __name__ == "__main__":
    main()
