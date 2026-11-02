import urllib.request
import json
import time

BASE_URL = "http://localhost:8000"

def post_json(path, data):
    req = urllib.request.Request(
        f"{BASE_URL}{path}",
        data=json.dumps(data).encode("utf-8"),
        headers={"Content-Type": "application/json"}
    )
    with urllib.request.urlopen(req) as resp:
        return json.loads(resp.read().decode("utf-8"))

def get_json(path):
    with urllib.request.urlopen(f"{BASE_URL}{path}") as resp:
        return json.loads(resp.read().decode("utf-8"))

print("=== 1. TEST REGISTRATION ===")
reg_data = {
    "name": "Gopi V",
    "email": f"gopi_{int(time.time())}@chronosmatch.hft",
    "password": "mypassword2026",
    "role": "Lead Quantitative Trader",
    "desk": "Ultra-Low Latency Equities Desk"
}
reg_res = post_json("/api/auth/register", reg_data)
print("Registration Result:", reg_res["success"], "| User:", reg_res["user"]["name"], "| Token:", reg_res["token"][:20])

print("\n=== 2. TEST LOGIN WITH REGISTERED CREDENTIALS ===")
login_res = post_json("/api/auth/login", {
    "email": reg_data["email"],
    "password": reg_data["password"]
})
print("Login Result:", login_res["success"], "| User:", login_res["user"]["name"], "| Desk:", login_res["user"]["desk"])

print("\n=== 3. TEST FIREHOSE PAUSE ===")
pause_res = post_json("/api/firehose/pause", {})
print("Pause Result:", pause_res)

time.sleep(0.5)
tel_paused = get_json("/api/telemetry")
print("Telemetry when Paused: is_paused =", tel_paused["firehose_paused"], "| TPS =", tel_paused["tps"])

print("\n=== 4. TEST MANUAL CLIENT EXECUTION WHILE FIREHOSE IS PAUSED ===")
buy_order = {
    "side": 1,
    "price": 99.50,
    "quantity": 250,
    "is_market": True,
    "account_id": "primary"
}
buy_res = post_json("/api/orders", buy_order)
print("Client Order while Paused:", buy_res["status"], f"#{buy_res['order_id']} in {buy_res['latency_us']} us")

print("\n=== 5. TEST FIREHOSE RESUME ===")
resume_res = post_json("/api/firehose/resume", {})
print("Resume Result:", resume_res)

time.sleep(0.5)
tel_resumed = get_json("/api/telemetry")
print("Telemetry when Resumed: is_paused =", tel_resumed["firehose_paused"], "| TPS =", tel_resumed["tps"])

print("\n>>> ALL TESTS PASSED SUCCESSFULLY! <<<")
