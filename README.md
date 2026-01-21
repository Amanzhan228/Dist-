# Raft Lite — Distributed Consensus

## Run nodes

Terminal 1:
python3 node.py --id A --port 8000 --peers 10.0.0.2:8001,10.0.0.3:8002

Terminal 2:
python3 node.py --id B --port 8001 --peers 10.0.0.1:8000,10.0.0.3:8002

Terminal 3:
python3 node.py --id C --port 8002 --peers 10.0.0.1:8000,10.0.0.2:8001

## Send command
python3 client.py 10.0.0.1:8000 "SET x = 5"

## Failure test
Kill leader process and observe new election.
