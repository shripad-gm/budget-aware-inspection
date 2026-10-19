# Budget-aware email inspection gateway

This project accepts uploaded `.eml` email files, parses their real headers and body, and moves each message through a staged Kafka inspection pipeline. The current classifier and task output use explicit Python rules. They are inspectable starting points, not a trained detector or a measured security result.

## Workflow

```text
.eml upload or CLI producer
  -> inspection.input.v3
  -> screen worker: category, risk signals, session sequence
  -> inspection.screened.v3
  -> scheduler: basic result, deeper rule pass, or bounded hold
       -> inspection.requests.v3 -> deep worker -> inspection.results.v3
       -> inspection.results.v3 (basic or timeout)
  -> finalizer: local task output and fixed recipient authorization
  -> inspection.decisions.v3 + SQLite draft outbox
```

The screen records categories such as meeting, finance, newsletter, and support. Risk rules look for explicit instruction overrides, assistant-directed commands, outbound-send requests, credential disclosure requests, and account-verification language. A strong instruction override can be blocked by the first pass. Other flagged content goes to a second pass. The dashboard shows every rule hit and the resulting route. These rules can miss attacks and produce false positives.

The trusted task chooses summarization, factual lookup, or preparation of a send draft. Text inside an email cannot authorize a new recipient. An allowed send creates a **local draft record only**. The application is not connected to SMTP or an email account and does not transmit outgoing email.

## Run locally

Start Docker Desktop. In PowerShell from this folder:

```powershell
docker compose up -d
python -m venv .venv
.venv\Scripts\python -m pip install -e .
.venv\Scripts\python -m inspection_gateway.stream_cli run
```

Keep the worker terminal open. In a second terminal:

```powershell
.venv\Scripts\python -m streamlit run inspection_gateway/dashboard.py
```

Open `http://localhost:8501` and use **Interactive lab**:

1. Export or save an email as an `.eml` file and upload it. Files must be at most 2 MB and contain a readable plain-text or HTML body. Attachments are counted but not opened.
2. Review the parsed sender, subject, category, rule hits, and initial route before submission.
3. Choose the trusted task. Enter a question for lookup or an explicitly allowed recipient for a send draft.
4. Click **Inspect through Kafka**. The page follows the event automatically. Use **Start**, **Back**, **Play**, **Next**, and **Live** to inspect recorded stages.
5. Open **All events** to compare completed messages and inspect the local draft outbox.

To see the hold path, set **Available deep slots** to `0`, upload a message with a text rule hit, and click **Apply capacity**. The event waits in SQLite until a slot becomes available or its deadline expires. Restore `1` slot to continue it.

## CLI input

The console entry point in `inspection_gateway/cli.py` uses the staged pipeline. To submit a file without the dashboard:

```powershell
.venv\Scripts\python -m inspection_gateway.stream_cli produce --eml C:\path\to\message.eml --task summarize
.venv\Scripts\python -m inspection_gateway.stream_cli status
.venv\Scripts\python -m inspection_gateway.stream_cli read --topic inspection.decisions.v3 --count 1
```

For a lookup task, add `--task lookup --question "When is the meeting?"`. For a send draft, add `--task send --recipient colleague@example.com`. The input email body may request a different recipient, but the fixed gate will deny any draft to an address outside the trusted task.

Use `run --stage screen`, `schedule`, `deep`, or `finalize` to launch one worker. Kafka records are keyed by session ID; order is preserved within a session, while sessions can interleave. SQLite stores queue state, stage audits, decisions, and deduplicated drafts. The v3 topics and `runtime/mail_gateway.sqlite3` separate this workflow from the earlier marker-based prototype.

## Checks and limits

```powershell
.venv\Scripts\python -m unittest discover -s tests -v
```

The `.eml` parser uses the standard library and does not fetch remote HTML resources. Uploads are kept in memory while the dashboard parses them. The Kafka event and local SQLite state contain the extracted email body, so use only messages you are permitted to process on this machine. The system has no inbox connector, no model API, and no Operations Research optimizer yet. The two inspection levels currently use different hardcoded rule sets, and their accuracy has not been validated on a real corpus. There is no real outbound mail transport.
