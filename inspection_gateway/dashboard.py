"""Streamlit control room for uploaded email inspection."""

from __future__ import annotations

import socket

import streamlit as st

from inspection_gateway.mail import MAX_EMAIL_BYTES, classify_email, parse_eml
from inspection_gateway.stream import INPUT, StreamStore
from inspection_gateway.stream_cli import publish_eml


st.set_page_config(page_title="Inspection control room", page_icon="✉️", layout="wide")
st.markdown("""
<style>
  .block-container { max-width: 1400px; padding-top: 1.5rem; }
  h1, h2, h3 { letter-spacing: -0.03em; }
  .hero { background: linear-gradient(120deg, #12263a, #164e63); color: white;
          border-radius: 18px; padding: 26px 30px; margin-bottom: 20px; }
  .hero h1 { color: white; margin: 0 0 8px; }
  .hero p { color: #d9eef0; margin: 0; max-width: 800px; }
  .step { border: 1px solid #dbe5e9; border-radius: 14px; padding: 16px;
          background: #f8fbfc; min-height: 118px; }
  .step b { color: #164e63; }
  .step small { color: #526674; }
</style>
""", unsafe_allow_html=True)


def broker_online(bootstrap: str) -> bool:
    host, _, port = bootstrap.rpartition(":")
    try:
        with socket.create_connection((host or "localhost", int(port or "9092")), timeout=0.5):
            return True
    except (OSError, ValueError):
        return False


def load_state(db_path: str):
    store = StreamStore(db_path)
    try:
        return store.metrics(), store.worker_status(), store.recent_events(100), store.outbox()
    finally:
        store.close()


def load_timeline(db_path: str, event_id: str):
    store = StreamStore(db_path)
    try:
        return store.timeline(event_id)
    finally:
        store.close()


def load_controls(db_path: str):
    store = StreamStore(db_path)
    try:
        return store.deep_capacity(1), store.queue_counts()
    finally:
        store.close()


def stage_story(entry: dict, event: dict) -> tuple[str, str, str]:
    """Explain an observed stage event using its recorded state."""
    stage, status = entry["stage"], entry["status"]
    screen = event.get("screen", {})
    decision = event.get("decision") or {}
    if stage == "producer":
        return "Email entered Kafka", "The uploader wrote the parsed email event to the input topic. Workers can now read it.", INPUT
    if stage == "screen":
        route = "requires deep inspection" if screen.get("requires_deep") else f"basic disposition: {screen.get('disposition', 'unknown')}"
        signals = ", ".join(screen.get("signals", [])) or "none"
        return "Email classified and screened", f"Category: {screen.get('category', 'general')}. Rule signals: {signals}. Routing: {route}. Session sequence: {screen.get('sequence_status', 'unknown')}.", "inspection.screened.v3"
    if stage == "scheduler" and status == "pending":
        return "Accepted into the scheduler", "The event entered the bounded SQLite queue. The scheduler now decides whether to finish basic inspection, request deep inspection, or wait.", "SQLite hold queue"
    if stage == "scheduler" and status == "held":
        return "Waiting for a deep slot", "This email requires deep inspection, but no slot was free. It stays pending until capacity returns or its deadline expires.", "SQLite hold queue"
    if stage == "scheduler" and status == "deep_request":
        return "Deep inspection requested", "A slot opened. The scheduler wrote a deep-inspection request to Kafka and marked the event in flight.", "inspection.requests.v3"
    if stage == "scheduler" and status == "basic":
        return "Basic result published", "The initial email rules produced a release or block result for finalization.", "inspection.results.v3"
    if stage == "scheduler" and status == "timeout":
        return "Deadline reached", "The scheduler produced an explicit timeout. The email cannot quietly disappear from the evaluation.", "inspection.results.v3"
    if stage == "scheduler" and status == "overload":
        return "Queue full", "The bounded queue rejected this event with an explicit overload outcome.", "inspection.results.v3"
    if stage == "deep":
        return "Deep inspection completed", f"The second rule pass returned {status}: {entry.get('detail', '')}. This result is sent to the finalizer.", "inspection.results.v3"
    if stage == "finalizer":
        if status == "release":
            answer = decision.get("assistant_output") or "No text response"
            return "Task output prepared", f"The local task rules produced: {answer}", "inspection.decisions.v3"
        return "Content stopped before task processing", f"The final disposition is {status}. The task rules did not use this email.", "inspection.decisions.v3"
    if stage == "action_gate":
        proposal = decision.get("proposed_action") or {}
        target = proposal.get("recipient", "unknown recipient")
        allowed = ", ".join(event.get("allowed_recipients", [])) or "none"
        return "Fixed action gate checked the draft", f"Candidate recipient: {target}. Trusted task: {event['task_type']}; allowed recipients: {allowed}. Gate outcome: {status}. Only allowed drafts enter the local outbox.", "SQLite draft outbox"
    return f"{stage}: {status}", entry.get("detail", ""), "Audit event"


st.markdown("""
<div class="hero">
  <h1>Inspection control room</h1>
  <p>Upload an email file, see its rule-based classification, and follow its path through Kafka and the action gate.</p>
</div>
""", unsafe_allow_html=True)

with st.sidebar:
    st.header("Connections")
    bootstrap = st.text_input("Kafka broker", value="localhost:9092")
    db_path = st.text_input("Local state database", value="runtime/mail_gateway.sqlite3")
    st.caption("The dashboard reads SQLite state; the pipeline workers move records through Kafka.")
    st.code(".venv\\Scripts\\python -m inspection_gateway.stream_cli run", language="powershell")
    st.caption("Run the worker command in another terminal for interactive submissions.")

lab, overview, guide = st.tabs(["Interactive lab", "All events", "How it works"])

with overview:
    st.info("Upload an `.eml` file in **Interactive lab**, then use this tab to compare classifications, decisions, audit trails, and saved drafts. This view refreshes every 2 seconds.")

    @st.fragment(run_every="2s")
    def live_view():
        online = broker_online(bootstrap)
        try:
            metrics, workers, events, outbox = load_state(db_path)
        except Exception as exc:
            st.error(f"Could not read local pipeline state: {exc}")
            return

        worker_count = sum(item["online"] for item in workers)
        cols = st.columns(4)
        cols[0].metric("Kafka broker", "Connected" if online else "Offline")
        cols[1].metric("Workers online", f"{worker_count}/4")
        cols[2].metric("Final decisions", metrics["decisions"])
        cols[3].metric("Saved drafts", len(outbox))
        if not online:
            st.warning("Start Docker Desktop and run `docker compose up -d`.")
        elif worker_count < 4:
            st.info("Kafka is available. Start the pipeline workers to process new emails.")

        st.subheader("Workers and Kafka handoffs")
        labels = [
            ("1 · Input", "Producer writes an email to Kafka"),
            ("2 · Screen", "Email category and risk rules"),
            ("3 · Schedule", "Basic, deep, or hold"),
            ("4 · Inspect", "Second rule pass if requested"),
            ("5 · Finalize", "Task output and action gate"),
        ]
        for column, (title, body) in zip(st.columns(5), labels):
            with column:
                st.markdown(f'<div class="step"><b>{title}</b><br><small>{body}</small></div>', unsafe_allow_html=True)
        st.caption("Kafka topics connect the stages. SQLite stores waiting events, decisions, and the local draft outbox.")

        worker_cols = st.columns(4)
        for column, worker in zip(worker_cols, workers):
            column.write(("🟢" if worker["online"] else "⚪") + " " + worker["stage"].title())

        st.subheader("Recent email events")
        if not events:
            st.info("No screened emails yet. Upload an `.eml` file in Interactive lab.")
            return
        rows = []
        for item in events:
            decision = item["decision"] or {}
            rows.append({
                "Event": item["event_id"],
                "From": item["sender"],
                "Subject": item["subject"],
                "Category": item["screen"].get("category", "general"),
                "Task": item["task_type"],
                "Queue": item["queue_state"] or "screened",
                "Route": decision.get("route", "—"),
                "Disposition": decision.get("disposition", "waiting"),
                "Action gate": decision.get("gate_outcome", "—"),
            })
        st.dataframe(rows, width="stretch", hide_index=True)

        selected_id = st.selectbox("Inspect one event", [item["event_id"] for item in events])
        selected = next(item for item in events if item["event_id"] == selected_id)
        decision = selected["decision"] or {}
        left, right = st.columns([1, 1])
        with left:
            st.markdown("#### Trusted task and email")
            st.write(f"**Task:** {selected['task_type']}")
            st.write(f"**From:** {selected['sender']}")
            st.write(f"**Subject:** {selected['subject']}")
            if selected["task_prompt"]:
                st.write(f"**Question:** {selected['task_prompt']}")
            st.code(selected["content"], language="text")
            st.write(f"**Light screen:** {selected['screen']['disposition']}")
            st.write(f"**Deep required:** {selected['screen']['requires_deep']}")
            st.write(f"**Rule signals:** {', '.join(selected['screen'].get('signals', [])) or 'none'}")
            st.write(f"**Sequence:** {selected['screen']['sequence_status']}")
        with right:
            st.markdown("#### Task output and decision")
            if decision:
                st.write(f"**Route:** {decision['route']} · **Disposition:** {decision['disposition']}")
                st.write(f"**Task output:** {decision.get('assistant_output') or 'None (content was blocked or timed out)'}")
                st.write(f"**Candidate draft:** {decision.get('proposed_action') or 'None'}")
                st.write(f"**Action gate:** {decision['gate_outcome']}")
            else:
                st.write("Waiting for a final decision.")
            timeline = load_timeline(db_path, selected_id)
            st.markdown("#### Stage timeline")
            st.dataframe(timeline, width="stretch", hide_index=True)

        st.subheader("Local draft outbox")
        if outbox:
            st.dataframe(outbox, width="stretch", hide_index=True)
        else:
            st.caption("No approved drafts yet. This application never sends email to the network.")
        st.caption(
            f"Observed end-to-end latency: p50 {metrics['end_to_end_p50_ms']} ms · "
            f"p95 {metrics['end_to_end_p95_ms']} ms."
        )

    live_view()

with lab:
    st.subheader("Inspect an email file")
    st.write("Upload an `.eml` message, review the extracted fields and rule hits, then send it through Kafka. The rule engine chooses the route from the email itself.")
    try:
        current_capacity, _ = load_controls(db_path)
    except Exception as exc:
        st.error(f"Could not load scheduler controls: {exc}")
        current_capacity = 1
    control_left, control_right = st.columns([1, 2])
    with control_left:
        st.markdown("#### Deep inspection slots")
        chosen_capacity = st.slider("Available deep slots", min_value=0, max_value=8, value=min(max(current_capacity, 0), 8), help="Set to 0 to hold deep-required emails. Restore 1 to release them.")
        if st.button("Apply capacity", type="secondary"):
            store = StreamStore(db_path)
            try:
                store.set_deep_capacity(chosen_capacity)
            finally:
                store.close()
            st.success(f"Deep capacity is now {chosen_capacity}. The scheduler reads this setting on its next cycle.")
    with control_right:
        @st.fragment(run_every="1s")
        def queue_view():
            capacity, counts = load_controls(db_path)
            st.markdown("#### Current queue")
            qcols = st.columns(3)
            qcols[0].metric("Waiting", counts.get("pending", 0))
            qcols[1].metric("Deep in flight", counts.get("inflight", 0))
            qcols[2].metric("Deep slots", capacity)
            st.caption("Set slots to 0 to hold an email flagged for deep review. Restore 1 slot to let it continue before its deadline.")

        queue_view()

    st.divider()
    st.markdown("#### 1. Upload email")
    uploaded = st.file_uploader("Choose one or more .eml files", type=["eml"], accept_multiple_files=True)
    parsed_files = []
    for file in uploaded or []:
        try:
            if file.size > MAX_EMAIL_BYTES:
                raise ValueError("Email file must be no larger than 2 MB")
            parsed = parse_eml(file.getvalue())
            rules = classify_email(parsed.sender, parsed.subject, parsed.body)
            parsed_files.append((file, parsed, rules))
        except ValueError as exc:
            st.error(f"{file.name}: {exc}")
    if parsed_files:
        st.dataframe([
            {
                "File": file.name,
                "From": parsed.sender,
                "Subject": parsed.subject,
                "Category": rules["category"],
                "Rule hits": ", ".join(rules["signals"]) or "none",
                "Initial route": "deep review" if rules["requires_deep"] else rules["disposition"],
                "Attachments skipped": parsed.attachment_count,
            }
            for file, parsed, rules in parsed_files
        ], width="stretch", hide_index=True)
        with st.expander("Preview extracted message body"):
            st.code(parsed_files[0][1].body[:3000], language="text")
    with st.form("email-task"):
        task = st.selectbox("Trusted task", ["summarize", "lookup", "send"])
        prompt = st.text_input("Lookup question")
        recipient = st.text_input("Recipient allowed by the trusted send task")
        deadline_seconds = st.number_input("Deadline in seconds", min_value=10, max_value=600, value=120)
        submitted = st.form_submit_button("Inspect through Kafka", type="primary")
    if submitted:
        if not parsed_files:
            st.error("Upload at least one readable `.eml` file first.")
        elif task == "send" and not recipient.strip():
            st.error("A send task needs an allowed recipient.")
        elif task == "lookup" and not prompt.strip():
            st.error("A lookup task needs a question.")
        elif not broker_online(bootstrap):
            st.error("Kafka is offline. Start Docker Desktop and Kafka first.")
        else:
            submitted_ids = []
            for file, _, _ in parsed_files:
                try:
                    submitted_ids.append(publish_eml(bootstrap, db_path, file.getvalue(), task, recipient, prompt, int(deadline_seconds)))
                except Exception as exc:
                    st.error(f"{file.name}: {exc}")
            if submitted_ids:
                st.session_state["lab_focus_event"] = submitted_ids[-1]
                st.success(f"Submitted {len(submitted_ids)} email(s): {', '.join(submitted_ids)}. Follow the selected email below.")
    st.caption("Rules inspect extracted text and headers. Attachments are counted but not opened. This is a transparent heuristic, not a trained security detector.")

    st.divider()
    st.markdown("#### 2. Follow and replay the journey")

    @st.fragment(run_every="1s")
    def journey_view():
        store = StreamStore(db_path)
        try:
            events = store.recent_events(100)
            focus = st.session_state.get("lab_focus_event")
            ids = [item["event_id"] for item in events]
            if not ids and not focus:
                st.info("Send an email above to see its journey.")
                return
            if focus and focus not in ids:
                timeline = store.timeline(focus)
                st.info(f"{focus} is in Kafka. Waiting for the screen worker to record it...")
                if timeline:
                    st.dataframe(timeline, width="stretch", hide_index=True)
                return
            selected_id = st.selectbox(
                "Choose an email to explore",
                ids,
                index=ids.index(focus) if focus in ids else 0,
                key=f"lab_event_choice_{focus or 'recent'}",
            )
            event = next(item for item in events if item["event_id"] == selected_id)
            timeline = store.timeline(selected_id)
            if not timeline:
                st.info("The event has been screened, but its stage audit is not yet available.")
                return
            replay_key = f"replay_index_{selected_id}"
            play_key = f"replay_playing_{selected_id}"
            follow_key = f"replay_following_{selected_id}"
            if replay_key not in st.session_state:
                st.session_state[replay_key] = len(timeline) - 1
                st.session_state[follow_key] = True
            st.session_state[replay_key] = min(st.session_state[replay_key], len(timeline) - 1)
            if st.session_state.get(follow_key):
                st.session_state[replay_key] = len(timeline) - 1
            elif st.session_state.get(play_key):
                if st.session_state[replay_key] < len(timeline) - 1:
                    st.session_state[replay_key] += 1
                else:
                    st.session_state[play_key] = False
            buttons = st.columns(5)
            if buttons[0].button("⏮ Start", key=f"start_{selected_id}"):
                st.session_state[replay_key] = 0
                st.session_state[play_key] = False
                st.session_state[follow_key] = False
            if buttons[1].button("◀ Back", key=f"back_{selected_id}"):
                st.session_state[replay_key] = max(0, st.session_state[replay_key] - 1)
                st.session_state[play_key] = False
                st.session_state[follow_key] = False
            if buttons[2].button("▶ Play", key=f"play_{selected_id}"):
                st.session_state[replay_key] = 0
                st.session_state[play_key] = True
                st.session_state[follow_key] = False
            if buttons[3].button("Next ▶", key=f"next_{selected_id}"):
                st.session_state[replay_key] = min(len(timeline) - 1, st.session_state[replay_key] + 1)
                st.session_state[play_key] = False
                st.session_state[follow_key] = False
            if buttons[4].button("● Live", key=f"live_{selected_id}"):
                st.session_state[replay_key] = len(timeline) - 1
                st.session_state[play_key] = False
                st.session_state[follow_key] = True
            index = st.session_state[replay_key]
            st.progress((index + 1) / len(timeline), text=f"Step {index + 1} of {len(timeline)} · {selected_id}")
            columns = st.columns(len(timeline))
            for position, (column, step) in enumerate(zip(columns, timeline)):
                symbol = "●" if position == index else ("✓" if position < index else "○")
                column.caption(f"{symbol} {step['stage']}\n{step['status']}")
            entry = timeline[index]
            title, explanation, location = stage_story(entry, event)
            with st.container(border=True):
                st.markdown(f"### {title}")
                st.write(explanation)
                st.caption(f"Where this state lives: {location}")
            task_col, email_col = st.columns(2)
            with task_col:
                st.markdown("**Trusted user task**")
                st.write(event["task_type"])
                if event["task_prompt"]:
                    st.write(event["task_prompt"])
                if event["allowed_recipients"]:
                    st.write("Allowed recipient: " + ", ".join(event["allowed_recipients"]))
            with email_col:
                st.markdown(f"**Email from:** {event['sender']}")
                st.write(f"**Subject:** {event['subject']}")
                st.write(f"**Category:** {event['screen'].get('category', 'general')}")
                st.write(f"**Rule hits:** {', '.join(event['screen'].get('signals', [])) or 'none'}")
                st.markdown("**Untrusted email body**")
                st.code(event["content"], language="text")
            decision = event["decision"] or {}
            if index == len(timeline) - 1 and decision:
                st.markdown("#### Final result")
                result_cols = st.columns(3)
                result_cols[0].metric("Disposition", decision["disposition"])
                result_cols[1].metric("Action gate", decision["gate_outcome"])
                result_cols[2].metric("Queue age", f"{decision.get('queue_age_ms', 0)} ms")
                st.write("**Task output:** " + (decision.get("assistant_output") or "None; the content did not reach task processing."))
                st.caption(f"Inspection route: {decision['route']} · Reason: {decision['reason_code']}")
                outbox_entry = next((item for item in store.outbox() if item["event_id"] == selected_id), None)
                if decision["disposition"] == "block":
                    st.error("Inspection blocked the email before task processing could use its content.")
                elif decision["disposition"] == "timeout":
                    st.warning("The deadline passed before inspection finished. Task processing did not use this email.")
                elif outbox_entry:
                    st.success(f"The trusted send task allowed a draft to {outbox_entry['recipient']}. One local outbox entry was saved; no email was sent.")
                    st.code(outbox_entry["body"], language="text")
                elif decision.get("proposed_action"):
                    proposed = decision["proposed_action"]["recipient"]
                    st.warning(f"The email text requested a send to {proposed}, but the trusted task did not authorize it. The fixed gate denied the draft, so the outbox has no entry.")
                else:
                    st.info("The task rules produced a response. No send was requested, so the draft outbox did not change.")
            with st.expander("Show raw stage audit"):
                st.dataframe(timeline, width="stretch", hide_index=True)
        finally:
            store.close()

    journey_view()

with guide:
    st.subheader("What each component does")
    st.markdown("""
1. **Uploader:** parses the `.eml` sender, subject, date, and readable body, then writes an event to Kafka.
2. **Screen worker:** applies explicit category and risk rules, and records the matching signals.
3. **Scheduler:** sends ordinary email to the basic result, flagged email to a second rule pass, or holds it in SQLite until a slot is free.
4. **Deep worker:** checks combinations such as assistant-directed credential disclosure and account-verification links.
5. **Finalizer:** prepares local summary, lookup, or send-draft output. A fixed gate checks any draft recipient against the trusted task.

Kafka stores records between stages. Reading a record does not remove it. Consumer groups track offsets; SQLite prevents duplicate draft entries. No SMTP or email account is connected.
""")
    st.info("These rules are explainable starting points and can miss attacks or flag ordinary messages. They are not a trained model or a validated security guarantee.")
