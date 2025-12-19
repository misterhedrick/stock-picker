import json

data = {
    "users": 42,
    "status": "ok"
}

with open("output.json", "w") as f:
    json.dump(data, f, indent=2)

print("Wrote output.json")
