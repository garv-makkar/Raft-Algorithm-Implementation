# Local Raft key/value demo

Run a cluster of **1-9 nodes in one terminal**. Python 3.12+ is required.
The launcher starts the nodes as background processes and opens a live web
dashboard in your browser. No extra terminal windows, Docker, or database are needed.

## Git Bash setup

```bash
python -m venv .venv
source .venv/Scripts/activate
python -m pip install -r requirements.txt
python start.py
```

If your virtual environment is already active, just run `python start.py`.
The launcher asks `How many Raft nodes? (1-9):`. Enter, for example, `3`.
The count means Raft nodes; the interactive client is included automatically.
Zero, negative numbers, non-integers, and counts of 10 or more are rejected.

You can also skip the prompt:

```bash
python start.py --nodes 3
```

PowerShell without activating the environment:

```powershell
.\.venv\Scripts\python.exe start.py
```

## Use the web dashboard

After entering the node count, your browser opens http://127.0.0.1:8080.
The terminal also prints the dashboard URL. Keep that terminal running.

The dashboard displays live roles, terms, leader claims, majority availability,
replication logs and committed key/value state. Select a node to inspect it.
Send SET/GET commands through the form. Stop and restart individual nodes to watch
elections and log catch-up. Observed events are status changes sampled by your
browser; topology lines are replication paths, not a packet trace. STATUS reads
local node state without adding log entries or requiring a quorum.

Use **Stop cluster** in the dashboard or Ctrl+C in the launcher terminal to stop
all child nodes. Closing the browser tab alone leaves the cluster running.

Options:

```bash
python start.py --nodes 3 --dashboard-port 8081
python start.py --nodes 3 --dashboard-port 0  # choose an available HTTP port
python start.py --nodes 3 --no-browser       # print URL without opening a browser
python start.py --nodes 3 --cli              # original terminal client
```

The dashboard uses Python's standard HTTP server and plain HTML/CSS/JavaScript,
with no extra packages, Node.js or frontend build step. It binds to 127.0.0.1.

## Optional terminal client

Election can take about 10 seconds. Enter single-word keys and values:

```text
SET name Garv
GET name
GET missing
SET name Raft
GET name
EXIT
```

Expected responses: SET request processed, Value: Garv, Key not found,
SET request processed, Value: Raft.

`EXIT`, `QUIT`, EOF, or Ctrl+C in --cli mode stops the client and launched nodes.
Every protocol state change is persisted before acknowledgement, so shutdown does
not require nodes to receive keyboard input. A timed-out SET can still commit.

## Logs and saved state

For three nodes on the default ports, output is under `.raft/cluster-3-50051/`:

- `node-0.console.log`, etc.: node output, appended across runs.
- `logs_node_0/state.json`, etc.: durable term, vote, log, and commit position.

Restart with the same count and base port to reuse saved data. Each cluster size
has separate storage; changing the count starts or resumes that size's cluster.
This is fixed membership per run, not live membership changes.

Nodes listen on consecutive localhost ports starting at 50051. The launcher checks
for occupied ports and reports startup failures. For another independent cluster:

```bash
python start.py --nodes 4 --base-port 50100
```

The required majority is `floor(node_count / 2) + 1`. One node elects itself;
two nodes need both online; three nodes need two; nine nodes need five.

## Optional individual processes

For manual failure experiments you can still launch nodes individually. All nodes
and the client must use the same count and base port, with distinct node IDs:

```bash
python Raft.py --nodes 3 --id 0
python Raft.py --nodes 3 --id 1
python Raft.py --nodes 3 --id 2
python Client.py --nodes 3
```

These individual commands use the current directory for node state by default.
Use a separate `--data-dir` on the node commands when changing membership.

## Implementation

`start.py` owns subprocess startup, readiness checks, output files and cleanup.
`dashboard.py` serves the local API; `dashboard.html` shows live node state.
`cluster_config.py` validates the shared count/ports. `Raft.py` handles elections,
heartbeats, replication and the committed key/value state machine. `Client.py`
discovers the leader and uses bounded retries. Generated bindings for `raft.proto`
are included; grpcio-tools is needed only when regenerating them.

Terms and votes are persisted before replies. Entries commit after a majority
acknowledges them, and only committed entries update the key/value state. GET
commits a NO-OP barrier to check leadership with a majority. Atomic JSON snapshots
restore state on restart; legacy logs.txt/metadata.txt remain readable.

This is an educational implementation without authentication, log compaction,
dynamic membership or request deduplication. GET adds a log entry; retries may
add duplicate SET entries, although SET of the same value is idempotent.

## Checks

```bash
python -m unittest discover -s tests -v
```

Manual checks: start with 1, 2, 3 and 9 nodes, try SET/GET, stop the cluster, restart with the
same count and GET the saved value. Invalid counts must reprompt (or fail with
--nodes). Reusing occupied ports must fail without disturbing the running cluster.
Live cluster testing is left to you as requested.
