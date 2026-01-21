import threading
import time
import random
import requests
from flask import Flask, request, jsonify
import argparse

# =========================
# Configuration
# =========================
ELECTION_TIMEOUT = (3, 6)
HEARTBEAT_INTERVAL = 1

# =========================
# Node State
# =========================
class RaftNode:
    def __init__(self, node_id, port, peers):
        self.id = node_id
        self.port = port
        self.peers = peers

        self.state = "Follower"
        self.currentTerm = 0
        self.votedFor = None
        self.log = []
        self.commitIndex = -1

        self.votesReceived = 0
        self.lastHeartbeat = time.time()

        self.app = Flask(__name__)
        self.setup_routes()

    # =========================
    # Flask Routes
    # =========================
    def setup_routes(self):

        @self.app.route("/request_vote", methods=["POST"])
        def request_vote():
            data = request.json
            term = data["term"]
            candidate = data["candidateId"]

            if term > self.currentTerm:
                self.currentTerm = term
                self.votedFor = None
                self.state = "Follower"

            voteGranted = False
            if term == self.currentTerm and self.votedFor is None:
                self.votedFor = candidate
                voteGranted = True

            return jsonify({
                "term": self.currentTerm,
                "voteGranted": voteGranted
            })

        @self.app.route("/append_entries", methods=["POST"])
        def append_entries():
            data = request.json
            term = data["term"]
            entries = data["entries"]

            if term >= self.currentTerm:
                self.currentTerm = term
                self.state = "Follower"
                self.lastHeartbeat = time.time()

                for entry in entries:
                    self.log.append(entry)

                return jsonify({"success": True})

            return jsonify({"success": False})

        @self.app.route("/client_command", methods=["POST"])
        def client_command():
            if self.state != "Leader":
                return jsonify({"error": "Not leader"}), 400

            command = request.json["command"]
            entry = {
                "term": self.currentTerm,
                "command": command
            }

            self.log.append(entry)
            index = len(self.log) - 1
            acks = 1

            for peer in self.peers:
                try:
                    r = requests.post(
                        f"http://{peer}/append_entries",
                        json={
                            "term": self.currentTerm,
                            "leaderId": self.id,
                            "entries": [entry]
                        },
                        timeout=1
                    )
                    if r.json().get("success"):
                        acks += 1
                except:
                    pass

            if acks > len(self.peers) // 2:
                self.commitIndex = index
                print(f"[Leader {self.id}] Entry committed: {command}")

            return jsonify({"status": "ok"})

    # =========================
    # Raft Logic
    # =========================
    def election_timer(self):
        while True:
            time.sleep(0.1)
            if self.state == "Leader":
                continue

            if time.time() - self.lastHeartbeat > random.uniform(*ELECTION_TIMEOUT):
                self.start_election()

    def start_election(self):
        self.state = "Candidate"
        self.currentTerm += 1
        self.votedFor = self.id
        self.votesReceived = 1

        print(f"[Node {self.id}] Candidate (term {self.currentTerm})")

        for peer in self.peers:
            try:
                r = requests.post(
                    f"http://{peer}/request_vote",
                    json={
                        "term": self.currentTerm,
                        "candidateId": self.id
                    },
                    timeout=1
                )
                if r.json().get("voteGranted"):
                    self.votesReceived += 1
            except:
                pass

        if self.votesReceived > len(self.peers) // 2:
            self.become_leader()

    def become_leader(self):
        self.state = "Leader"
        print(f"[Node {self.id}] Became Leader (term {self.currentTerm})")
        threading.Thread(target=self.send_heartbeats, daemon=True).start()

    def send_heartbeats(self):
        while self.state == "Leader":
            for peer in self.peers:
                try:
                    requests.post(
                        f"http://{peer}/append_entries",
                        json={
                            "term": self.currentTerm,
                            "leaderId": self.id,
                            "entries": []
                        },
                        timeout=1
                    )
                except:
                    pass
            time.sleep(HEARTBEAT_INTERVAL)

    # =========================
    # Start Node
    # =========================
    def start(self):
        threading.Thread(target=self.election_timer, daemon=True).start()
        self.app.run(host="0.0.0.0", port=self.port)

# =========================
# Main
# =========================
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--id", required=True)
    parser.add_argument("--port", type=int, required=True)
    parser.add_argument("--peers", required=True)
    args = parser.parse_args()

    peers = args.peers.split(",")
    node = RaftNode(args.id, args.port, peers)
    node.start()
