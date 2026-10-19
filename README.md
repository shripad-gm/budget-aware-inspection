# Budget-aware inspection gateway

This repository implements the **streaming skeleton** in [project_plan.md](project_plan.md) using synthetic emails. The inspectors and assistant are deterministic fixtures, so this is an architecture demo rather than a prompt-injection defense evaluation.

## Current stream workflow

```text
demo producer
  -> inspection.input.v2
  -> screen worker: validates event, tracks session sequence, runs light fixture
  -> inspection.screened.v2
  -> scheduler: basic result, deep request, or bounded hold
       -> inspection.requests.v2 -> deep worker -> inspection.results.v2
       -> inspection.results.v2 (basic or timeout)
  -> finalizer: mock assistant proposal -> fixed permission gate
  -> inspection.decisions.v2 + SQLite mock outbox
```

Each Kafka event is keyed by session ID. The screen worker records the highest sequence seen for each session and flags out-of-order input. The scheduler stores pending events in SQLite, limits the hold queue to 64 events by default, permits one deep request in flight by default, and emits explicit overload or deadline outcomes. A deep-required event waits or times out when capacity is zero; it is never released through the basic route. Malformed messages are recorded on `inspection.errors.v2`.

The fixed permission gate allows a mock send only when the trusted task permits that recipient. It never takes permission from email text. Repeated results may appear on Kafka after replay; SQLite keeps the mock outbox at one action per event ID.

## One-command Kafka demo

Start Docker Desktop, then run from PowerShell in this folder:

```powershell
docker compose up -d
python -m venv .venv
.venv\Scripts\python -m pip install -e .
.venv\Scripts\python -m inspection_gateway.stream_cli demo
```

The demo creates six Kafka topics, starts four stage workers, publishes six synthetic emails, prints six final decisions, shows the mock outbox and a small latency summary, then stops the workers. It chooses a new run ID each time. Expected outcomes:

| Event | Route | Outcome |
|---|---|---|
| `-1` | Basic | Email asks for an unauthorized send; gate denies it |
| `-2` | Basic | Authorized mock send appears once in the outbox |
| `-3` | Basic | Light fixture blocks it |
| `-4` | Deep | Deep fixture blocks it |
| `-5` | Deep | Deep fixture releases it |
| `-6` | Basic | Assistant fixture answers a factual lookup question |

To see the hold path, run:

```powershell
.venv\Scripts\python -m inspection_gateway.stream_cli demo --capacity 0 --deadline-seconds 8
```

The basic events finish normally. Deep-required events remain pending until their deadlines, then become explicit timeouts. The `--queue-limit` option controls the maximum pending and in-flight queue size.

## Inspect or run stages separately

```powershell
.venv\Scripts\python -m inspection_gateway.stream_cli topics
.venv\Scripts\python -m inspection_gateway.stream_cli run
```

In a second terminal:

```powershell
.venv\Scripts\python -m inspection_gateway.stream_cli produce
.venv\Scripts\python -m inspection_gateway.stream_cli read --topic inspection.decisions.v2 --count 6
.venv\Scripts\python -m inspection_gateway.stream_cli status
```

Use `run --stage screen`, `schedule`, `deep`, or `finalize` to start individual workers. Each uses its own Kafka consumer group. The `read` command can inspect any of the six topics. Because topics have three partitions, order is preserved within a session, but output across sessions may interleave.

## Use the Streamlit dashboard

Keep Kafka running. In one terminal, run the four workers:

```powershell
.venv\Scripts\python -m inspection_gateway.stream_cli run
```

In another terminal, start the dashboard:

```powershell
.venv\Scripts\python -m streamlit run inspection_gateway/dashboard.py
```

Open `http://localhost:8501`. **All events** shows Kafka connected and `4/4` workers online. Then:

1. Open **Interactive lab**, choose **Factual lookup**, and click **Send to Kafka**. The page gives you an event ID beginning with `ui-`.
2. Follow the event as each worker records a step. Use **Start**, **Back**, **Play**, and **Next** to replay the steps; **Live** resumes following the newest step.
3. Inspect the trusted task, untrusted email, assistant output, gate result, and raw stage audit. Try **Injection attempt** next: the email is released, but its proposed mock send is denied by the action gate.
4. To see the waiting path, set **Available deep slots** to `0` and click **Apply capacity**. Send the **Deep inspection** scenario, watch it enter **held**, then restore `1` slot and watch it continue. The default 120-second deadline gives you time to do this.

The lab's queue and journey refresh every second. **All events** refreshes every two seconds and lets you compare prior events. The workers, not the dashboard, consume Kafka messages and produce decisions. The dashboard writes the deep-slot setting to SQLite; the running scheduler reads it on its next cycle. Restart workers launched before this dashboard update to enable the live capacity control. The outbox contains approved mock sends only; summaries and lookup answers appear under each event's assistant output.

## Checks and current limits

```powershell
.venv\Scripts\python -m unittest discover -s tests -v
```

The tests cover authorization, replay-safe mock sends, basic and deep routing, session sequence flags, a held deadline, and queue limits. The original single-worker `inspection_gateway.cli` remains available as a simple first-step example; the staged `stream_cli` is the current workflow.

The scheduler currently uses a fixed rule based on synthetic `[[DEEP]]`, `[[DEEP_BLOCK]]`, and `[[BLOCK]]` markers. There is no OR optimizer, model API, real dataset, or measured security result yet. The assistant produces deterministic summary previews, lookup matches, and mock send proposals; it is not a live AI model. The tiny demo's latency summary includes worker startup and is not a throughput benchmark. Kafka publication is at least once, and the local SQLite setup is intended for this single-machine POC. Input task permissions are assumed to come from a trusted producer; the gateway does not authenticate producers yet.

Next, measure arrival rate, queue age, throughput, and latency on this pipeline, then add a baseline routing policy and the capacity-constrained optimizer.
