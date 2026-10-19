"""Run the staged Kafka pipeline and submit uploaded email files."""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
import threading
from datetime import timedelta
from pathlib import Path
from time import monotonic
from uuid import uuid4

from confluent_kafka import Consumer, Producer
from confluent_kafka.admin import AdminClient, NewTopic

from .core import Event, event_payload, utc_now
from .mail import MAX_EMAIL_BYTES, parse_eml
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


def consumer_for(bootstrap: str, stage: str, topics: list[str]) -> Consumer:
    consumer = Consumer({
        "bootstrap.servers": bootstrap,
        "group.id": f"inspection-{stage}-v3",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })
    consumer.subscribe(topics)
    return consumer


def run_stage(stage: str, bootstrap: str, db: str, stop: threading.Event, capacity: int, queue_limit: int) -> None:
    subscriptions = {
        "screen": [INPUT],
        "schedule": [SCREENED, RESULTS],
        "deep": [REQUESTS],
        "finalize": [RESULTS],
    }
    store = StreamStore(db)
    consumer = consumer_for(bootstrap, stage, subscriptions[stage])
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
                    if stage == "screen":
                        screened = store.screen(payload)
                        event = Event.from_dict(screened["event"])
                        send(SCREENED, event.session_id, screened)
                        print(f"screen {event.event_id}: {screened['screen']['category']}, deep={screened['screen']['requires_deep']}", flush=True)
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
                        store.record_stage(event.event_id, "deep", result["disposition"], result["reason_code"])
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
                schedule_once(store, send, store.deep_capacity(capacity))
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


def publish_eml(bootstrap: str, db: str, raw: bytes, task: str, recipient: str, question: str, deadline_seconds: int) -> str:
    parsed = parse_eml(raw)
    if task == "send" and not recipient.strip():
        raise ValueError("A send task needs an allowed recipient")
    if task == "lookup" and not question.strip():
        raise ValueError("A lookup task needs a question")
    create_topics(bootstrap)
    event_id = "eml-" + uuid4().hex[:12]
    event = Event(
        event_id=event_id,
        session_id=event_id,
        sequence=1,
        content=parsed.body,
        task_type=task,
        allowed_recipients=(recipient.strip(),) if task == "send" else (),
        deadline=(utc_now() + timedelta(seconds=deadline_seconds)).isoformat(),
        task_prompt=question.strip(),
        sender=parsed.sender,
        subject=parsed.subject,
        message_date=parsed.message_date,
        message_id=parsed.message_id,
        source="uploaded_eml",
    )
    payload = event_payload(event)
    payload.update({
        "event_type": "email",
        "replay_arrival_at": utc_now().isoformat(),
        "content_ref": "sha256:" + hashlib.sha256(raw).hexdigest(),
        "provenance_ref": "uploaded-eml",
        "task_ref": f"task:{event_id}",
    })
    publish(Producer({"bootstrap.servers": bootstrap}), INPUT, event.session_id, payload)
    store = StreamStore(db)
    try:
        store.record_stage(event_id, "producer", "published", INPUT)
    finally:
        store.close()
    return event_id


def read_topic(bootstrap: str, topic: str, count: int) -> None:
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
            print(json.dumps(json.loads(message.value())))
            seen += 1
    finally:
        consumer.close()
    if seen < count:
        raise SystemExit(f"Read {seen}/{count} messages from {topic} within 15 seconds")


def start_stages(stages: tuple[str, ...], bootstrap: str, db: str, capacity: int, queue_limit: int):
    stop = threading.Event()
    errors = []

    def worker(stage: str) -> None:
        try:
            run_stage(stage, bootstrap, db, stop, capacity, queue_limit)
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
    parser = argparse.ArgumentParser(description="Staged Kafka email-inspection pipeline")
    parser.add_argument("command", choices=("topics", "run", "produce", "read", "status"))
    parser.add_argument("--bootstrap", default="localhost:9092")
    parser.add_argument("--db", default="runtime/mail_gateway.sqlite3")
    parser.add_argument("--stage", choices=("all", "screen", "schedule", "deep", "finalize"), default="all")
    parser.add_argument("--capacity", type=int, default=1)
    parser.add_argument("--queue-limit", type=int, default=64)
    parser.add_argument("--topic", choices=TOPICS, default=DECISIONS)
    parser.add_argument("--count", type=int, default=5)
    parser.add_argument("--eml", type=Path)
    parser.add_argument("--task", choices=("summarize", "lookup", "send"), default="summarize")
    parser.add_argument("--recipient", default="")
    parser.add_argument("--question", default="")
    parser.add_argument("--deadline-seconds", type=int, default=120)
    args = parser.parse_args()
    if args.capacity < 0 or args.queue_limit < 1 or args.count < 1 or args.deadline_seconds < 1:
        parser.error("capacity must be nonnegative; queue-limit, count, and deadline-seconds must be positive")
    if args.command == "topics":
        create_topics(args.bootstrap)
    elif args.command == "produce":
        if args.eml is None:
            parser.error("produce requires --eml PATH")
        if args.eml.stat().st_size > MAX_EMAIL_BYTES:
            parser.error("Email file must be no larger than 2 MB")
        event_id = publish_eml(args.bootstrap, args.db, args.eml.read_bytes(), args.task, args.recipient, args.question, args.deadline_seconds)
        print(f"{event_id} -> {INPUT}")
    elif args.command == "read":
        read_topic(args.bootstrap, args.topic, args.count)
    elif args.command == "status":
        store = StreamStore(args.db)
        try:
            print(json.dumps({"queue": store.queue_status(), "draft_outbox": store.outbox(), "metrics": store.metrics()}, indent=2))
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


if __name__ == "__main__":
    main()
