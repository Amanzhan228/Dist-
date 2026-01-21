import threading
import time
import random
import requests
import sys
from flask import Flask, request, jsonify

app = Flask(__name__)

# Configuration

NODE_ID = None
PORT = None
PEERS = []

ELECTION_RANGE = (0.15, 0.3)
HEARTBEAT_DELAY = 0.1

# Raft State

role = "Follower"
term = 0
voted_candidate = None

log_store = []
commit_pos = 0
applied_pos = 0

last_contact = time.time()
timeout_limit = random.uniform(*ELECTION_RANGE)

# Leader-only state
replica_match = {}
replica_next = {}

# Utility Functions

def refresh_timeout():
    global last_contact, timeout_limit
    last_contact = time.time()
    timeout_limit = random.uniform(*ELECTION_RANGE)


def step_down(new_term):
    global role, term, voted_candidate
    role = "Follower"
    term = new_term
    voted_candidate = None
    refresh_timeout()

# Election Logic

def election_watchdog():
    while True:
        if role != "Leader" and time.time() - last_contact > timeout_limit:
            initiate_election()
        time.sleep(0.05)


def initiate_election():
    global role, term, voted_candidate

    role = "Candidate"
    term += 1
    voted_candidate = NODE_ID
    votes = 1

    print(f"[{NODE_ID}] Election timeout → Candidate (term {term})")

    for peer in PEERS:
        try:
            response = requests.post(
                f"{peer}/vote",
                json={"term": term, "candidate": NODE_ID},
                timeout=1
            ).json()

            if response.get("granted"):
                votes += 1

        except:
            pass

    majority = (len(PEERS) + 1) // 2 + 1
    if votes >= majority:
        assume_leadership()


def assume_leadership():
    global role
    role = "Leader"
    print(f"[{NODE_ID}] Became Leader (term {term})")

    for peer in PEERS:
        replica_next[peer] = len(log_store) + 1
        replica_match[peer] = 0

    threading.Thread(target=heartbeat_loop, daemon=True).start()
    
# Heartbeats & Replication

def heartbeat_loop():
    while role == "Leader":
        for peer in PEERS:
            transmit_entries(peer, [])
        time.sleep(HEARTBEAT_DELAY)


def transmit_entries(peer, entries):
    try:
        response = requests.post(
            f"{peer}/append",
            json={
                "term": term,
                "leader": NODE_ID,
                "entries": entries
            },
            timeout=1
        ).json()

        if response.get("success"):
            if entries:
                replica_match[peer] = len(log_store)
                verify_commit()
        else:
            if response.get("term", 0) > term:
                step_down(response["term"])

    except:
        pass


def verify_commit():
    global commit_pos, applied_pos

    if role != "Leader":
        return

    cluster_size = len(PEERS) + 1
    quorum = cluster_size // 2 + 1

    for idx in range(commit_pos + 1, len(log_store) + 1):
        confirmations = 1

        for peer in PEERS:
            if replica_match.get(peer, 0) >= idx:
                confirmations += 1

        if confirmations >= quorum and log_store[idx - 1]["term"] == term:
            commit_pos = idx
            print(f"[{NODE_ID}] Entry committed (index={commit_pos}, term={term})")
        else:
            break

    while applied_pos < commit_pos:
        print(f"[{NODE_ID}] Applied: {log_store[applied_pos]['cmd']}")
        applied_pos += 1
# API Endpoints
@app.route("/vote", methods=["POST"])
def vote():
    global voted_candidate, term

    data = request.json
    incoming_term = data["term"]
    candidate = data["candidate"]

    if incoming_term < term:
        return jsonify({"term": term, "granted": False})

    if voted_candidate in (None, candidate):
        voted_candidate = candidate
        refresh_timeout()
        return jsonify({"term": term, "granted": True})

    return jsonify({"term": term, "granted": False})


@app.route("/append", methods=["POST"])
def append():
    global term, role

    data = request.json
    incoming_term = data["term"]
    entries = data.get("entries", [])

    if incoming_term < term:
        return jsonify({"term": term, "success": False})

    if incoming_term > term:
        step_down(incoming_term)

    role = "Follower"
    refresh_timeout()

    for entry in entries:
        log_store.append({"term": incoming_term, "cmd": entry})
        print(f"[{NODE_ID}] Log appended: {entry}")

    return jsonify({"term": term, "success": True})


@app.route("/command", methods=["POST"])
def command():
    if role != "Leader":
        return jsonify({"success": False, "error": "Not leader"})

    cmd = request.json["command"]
    log_store.append({"term": term, "cmd": cmd})
    print(f"[{NODE_ID}] New command: {cmd}")

    for peer in PEERS:
        transmit_entries(peer, [cmd])

    verify_commit()
    return jsonify({"success": True})

if __name__ == "__main__":
    if len(sys.argv) < 7:
        print("Usage: python3 node.py --id <ID> --port <PORT> --peers <A,B>")
        sys.exit(1)

    NODE_ID = sys.argv[2]
    PORT = int(sys.argv[4])

    peer_ids = sys.argv[6].split(",")
    peer_table = {
        "A": "http://10.0.1.136:8000",
        "B": "http://10.0.1.113:8001",
        "C": "http://10.0.1.100:8002"
    }

    PEERS = [peer_table[p] for p in peer_ids]

    threading.Thread(target=election_watchdog, daemon=True).start()
    app.run(host="0.0.0.0", port=PORT)
