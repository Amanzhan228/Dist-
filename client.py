import requests
import sys

leader = sys.argv[1]
command = sys.argv[2]

r = requests.post(
    f"http://{leader}/client_command",
    json={"command": command}
)

print(r.json())
