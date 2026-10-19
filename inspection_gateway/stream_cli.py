"""Run and observe the staged Kafka pipeline."""

from __future__ import annotations

import argparse
import json
import sys
import threading
from datetime import timedelta
from time import monotonic
from uuid import uuid4

from confluent_kafka import Consumer, Producer
from confluent_kafka.admin import AdminClient, NewTopic

from .core import Event, event_payload, utc_now
from .stream import (
    DECISIONS, ERRORS, INPUT, REQUESTS, RESULTS, SCREENED, TOPICS,
    StreamStore, deep_inspect, result_payload, schedule_once,
)


def publish(producer: Producer, topic: str, key: str, payload: dict) -> None:
    errors = []
    producer.produce(
        topic, key=key.encode(), value=json.dumps(payload).encode(),
        on_delivery=lambda error, _message: errors.append(error) if error else None,
    )
    if producer.flush(10) or errors:
        raise RuntimeError(f"Kafka publish failed for {topic}: {errors}")


def consumer_for(bootstrap: str, stage: str, topics: list[str], run_scope: str | None = None) -> Consumer:
    consumer = Consumer({
        "bootstrap.servers": bootstrap,
        "group.id": f"inspection-{stage}-v2" + (f"-{run_scope}" if run_scope else ""),
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })
    consumer.subscribe(topics)
    return consumer


def run_stage(stage: str, bootstrap: str, db: str, stop: threading.Event, capacity: int, queue_limit: int, run_scope: str | None = None) -> None:
    subscriptions = {
        "screen": [INPUT],
        "schedule": [SCREENED, RESULTS],
        "deep": [REQUESTS],
        "finalize": [RESULTS],
    }
    store = StreamStore(db)
    consumer = consumer_for(bootstrap, stage, subscriptions[stage], run_scope)
    producer = Producer({"bootstrap.servers": bootstrap})
    send = lambda topic, key, payload: publish(producer, topic, key, payload)
    print(f"{stage} listening to {', '.join(subscriptions[stage])}", flush=True)
    last_heartbeat = 0.0
    try:
        while not stop.is_set():
            if monotonic() - last_heartbeat >= 2:
                store.heartbeat(stage)
                last_heartbeat = monotonic()
            message = consumer.poll(0.2)
            if message is not None:
                if message.error():
                    raise RuntimeError(str(message.error()))
                try:
                    payload = json.loads(message.value())
                    event_id = payload.get("event_id") or payload.get("event", {}).get("event_id")
                    if run_scope and not str(event_id).startswith(run_scope + "-"):
                        consumer.commit(message=message, asynchronous=False)
                        continue
                    if stage == "screen":
                        screened = store.screen(payload)
                        event = Event.from_dict(screened["event"])
                        send(SCREENED, event.session_id, screened)
                        print(f"screen {event.event_id}: {screened['screen']['disposition']}, deep={screened['screen']['requires_deep']}", flush=True)
                    elif stage == "schedule" and message.topic() == SCREENED:
                        event = Event.from_dict(payload["event"])
                        state = store.enqueue(payload, queue_limit)
                        if state == "overload":
                            result = result_payload(payload, "hold", "timeout", "queue_overload", utc_now().isoformat())
                            send(RESULTS, event.session_id, result)
                        print(f"schedule {event.event_id}: {state}", flush=True)
                    elif stage == "schedule":
                        event = Event.from_dict(payload["event"])
                        if payload["route"] == "deep":
                            store.mark(event.event_id, "complete")
                    elif stage == "deep":
                        result = deep_inspect(payload)
                        event = Event.from_dict(payload["event"])
                        send(RESULTS, event.session_id, result)
                        store.record_stage(event.event_id, "deep", result["disposition"])
                        print(f"deep {event.event_id}: {result['disposition']}", flush=True)
                    elif stage == "finalize":
                        decision = store.finalize(payload)
                        send(DECISIONS, decision["session_id"], decision)
                        store.record_stage(decision["event_id"], "finalizer", decision["disposition"])
                        if decision["proposed_action"]:
                            store.record_stage(decision["event_id"], "action_gate", decision["gate_outcome"])
                        print(f"final {decision['event_id']}: {decision['disposition']}, gate={decision['gate_outcome']}", flush=True)
                except (ValueError, TypeError, KeyError) as exc:
                    error = {
                        "stage": stage, "topic": message.topic(),
                        "partition": message.partition(), "offset": message.offset(),
                        "reason": str(exc), "recorded_at": utc_now().isoformat(),
                    }
                    send(ERRORS, "invalid", error)
                    print(f"invalid message: {error}", file=sys.stderr, flush=True)
                consumer.commit(message=message, asynchronous=False)
            if stage == "schedule":
                effective_capacity = store.deep_capacity(capacity) if run_scope is None else capacity
                schedule_once(store, send, effective_capacity)
    finally:
        consumer.close()
        store.close()


def create_topics(bootstrap: str) -> None:
    admin = AdminClient({"bootstrap.servers": bootstrap})
    futures = admin.create_topics([NewTopic(topic, 3, 1) for topic in TOPICS])
    for topic, future in futures.items():
        try:
            future.result(15)
            print(f"created {topic}")
        except Exception as exc:
            if "TOPIC_ALREADY_EXISTS" in str(exc):
                print(f"exists {topic}")
            else:
                raise


def examples(run_id: str, deadline_seconds: int = 300) -> list[Event]:
    deadline = (utc_now() + timedelta(seconds=deadline_seconds)).isoformat()
    return [
        Event(f"{run_id}-1", f"{run_id}-a", 1, "[[SEND:outsider@example.test]]", "summarize", (), deadline),
        Event(f"{run_id}-2", f"{run_id}-b", 1, "Please send the meeting note.", "send", ("colleague@example.test",), deadline),
        Event(f"{run_id}-3", f"{run_id}-a", 2, "[[BLOCK]] synthetic light block", "summarize", (), deadline),
        Event(f"{run_id}-4", f"{run_id}-c", 1, "[[DEEP_BLOCK]] synthetic deep block", "summarize", (), deadline),
        Event(f"{run_id}-5", f"{run_id}-d", 1, "[[DEEP]] synthetic deep release", "summarize", (), deadline),
        Event(f"{run_id}-6", f"{run_id}-e", 1, "The project meeting is Tuesday at 10:00. Maya owns the agenda.", "lookup", (), deadline, "When is the project meeting?"),
    ]


def produce_examples(bootstrap: str, run_id: str, deadline_seconds: int = 300) -> None:
    producer = Producer({"bootstrap.servers": bootstrap})
    for event in examples(run_id, deadline_seconds):
        payload = event_payload(event)
        payload["event_type"] = "email"
        payload["replay_arrival_at"] = utc_now().isoformat()
        payload["content_ref"] = f"synthetic:{event.event_id}"
        payload["provenance_ref"] = "synthetic-demo-v1"
        payload["task_ref"] = f"task:{event.event_id}"
        publish(producer, INPUT, event.session_id, payload)
        print(f"input {event.event_id} -> {INPUT}")


def read_topic(bootstrap: str, topic: str, count: int, run_id: str | None) -> None:
    consumer = Consumer({
        "bootstrap.servers": bootstrap,
        "group.id": f"inspection-observer-{uuid4()}",
        "auto.offset.reset": "earliest",
    })
    consumer.subscribe([topic])
    seen = 0
    cutoff = monotonic() + 15
    try:
        while seen < count and monotonic() < cutoff:
            message = consumer.poll(1)
            if message is None:
                continue
            if message.error():
                raise RuntimeError(str(message.error()))
            payload = json.loads(message.value())
            event_id = payload.get("event_id") or payload.get("event", {}).get("event_id")
            if run_id and not str(event_id).startswith(run_id + "-"):
                continue
            print(json.dumps(payload))
            seen += 1
    finally:
        consumer.close()
    if seen < count:
        raise SystemExit(f"Read {seen}/{count} messages from {topic} within 15 seconds")


def start_stages(stages: tuple[str, ...], bootstrap: str, db: str, capacity: int, queue_limit: int, run_scope: str | None = None):
    stop = threading.Event()
    errors = []

    def worker(stage: str) -> None:
        try:
            run_stage(stage, bootstrap, db, stop, capacity, queue_limit, run_scope)
        except Exception as exc:
            errors.append((stage, exc))
            stop.set()

    threads = [threading.Thread(target=worker, args=(stage,), daemon=True) for stage in stages]
    for thread in threads:
        thread.start()
    return stop, errors, threads


def finish_stages(stop: threading.Event, errors: list, threads: list[threading.Thread]) -> None:
    stop.set()
    for thread in threads:
        thread.join(timeout=5)
    if errors:
        stage, exc = errors[0]
        raise SystemExit(f"{stage} failed: {exc}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Staged Kafka inspection pipeline")
    parser.add_argument("command", choices=("topics", "run", "produce", "read", "status", "demo"))
    parser.add_argument("--bootstrap", default="localhost:9092")
    parser.add_argument("--db", default="runtime/stream.sqlite3")
    parser.add_argument("--stage", choices=("all", "screen", "schedule", "deep", "finalize"), default="all")
    parser.add_argument("--capacity", type=int, default=1)
    parser.add_argument("--queue-limit", type=int, default=64)
    parser.add_argument("--topic", choices=TOPICS, default=DECISIONS)
    parser.add_argument("--count", type=int, default=5)
    parser.add_argument("--run-id")
    parser.add_argument("--deadline-seconds", type=int, default=300)
    args = parser.parse_args()
    if args.capacity < 0 or args.queue_limit < 1 or args.count < 1 or args.deadline_seconds < 1:
        parser.error("capacity must be nonnegative; queue-limit, count, and deadline-seconds must be positive")
    if args.command == "topics":
        create_topics(args.bootstrap)
    elif args.command == "produce":
        run_id = args.run_id or uuid4().hex[:8]
        print(f"run_id={run_id}")
        produce_examples(args.bootstrap, run_id, args.deadline_seconds)
    elif args.command == "read":
        read_topic(args.bootstrap, args.topic, args.count, args.run_id)
    elif args.command == "status":
        store = StreamStore(args.db)
        try:
            print(json.dumps({"queue": store.queue_status(), "mock_outbox": store.outbox(), "metrics": store.metrics(args.run_id)}, indent=2))
        finally:
            store.close()
    elif args.command == "run":
        stages = ("screen", "schedule", "deep", "finalize") if args.stage == "all" else (args.stage,)
        stop, errors, threads = start_stages(stages, args.bootstrap, args.db, args.capacity, args.queue_limit)
        try:
            while not stop.wait(0.5):
                pass
        except KeyboardInterrupt:
            pass
        finish_stages(stop, errors, threads)
    elif args.command == "demo":
        create_topics(args.bootstrap)
        run_id = args.run_id or uuid4().hex[:8]
        stages = ("screen", "schedule", "deep", "finalize")
        stop, errors, threads = start_stages(stages, args.bootstrap, args.db, args.capacity, args.queue_limit, run_id)
        try:
            print(f"run_id={run_id}")
            produce_examples(args.bootstrap, run_id, args.deadline_seconds)
            print("final decisions:")
            read_topic(args.bootstrap, DECISIONS, len(examples(run_id)), run_id)
            store = StreamStore(args.db)
            try:
                print("mock outbox:")
                print(json.dumps([item for item in store.outbox() if item["event_id"].startswith(run_id + "-")], indent=2))
                print("stream metrics:")
                print(json.dumps(store.metrics(run_id), indent=2))
            finally:
                store.close()
        finally:
            finish_stages(stop, errors, threads)


if __name__ == "__main__":
    main()
