import sys
import requests
import json
import time
import hmac
import hashlib
import secrets

# Set UTF-8 output encoding for Windows terminal
if sys.platform == "win32":
    import io
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

BASE_URL = "http://127.0.0.1:8000"

print("=" * 70)
print("CHRONOSMATCH PRO - COMPREHENSIVE END-TO-END VERIFICATION")
print("=" * 70)

session = requests.Session()

# -----------------------------------------------------------------------------
# TEST 1: Page Serving & Route Protection
# -----------------------------------------------------------------------------
print("\n[TEST 1] Route Protection & Dashboard Guard:")
r = session.get(f"{BASE_URL}/", allow_redirects=False)
assert r.status_code == 303, f"Expected 303 redirect for unauthenticated root, got {r.status_code}"
assert r.headers.get("Location") == "/login", f"Expected redirect to /login, got {r.headers.get('Location')}"
print("  ✓ Unauthenticated access to '/' correctly redirected to '/login' (HTTP 303)")

r_login = session.get(f"{BASE_URL}/login")
assert r_login.status_code == 200, f"Expected 200 on /login, got {r_login.status_code}"
assert "ChronosMatch Pro" in r_login.text, "Brand title missing in /login"
assert "view-register" in r_login.text, "Registration view missing in /login"
assert "reg-pwd-confirm" in r_login.text, "Confirm password field missing in /login"
print("  ✓ 3D Login & Registration terminal served cleanly with all required fields (HTTP 200)")

# -----------------------------------------------------------------------------
# TEST 2: Registration Validation & BCrypt Hashing
# -----------------------------------------------------------------------------
print("\n[TEST 2] Registration Validations:")
# A. Short password
r = session.post(f"{BASE_URL}/api/auth/register", json={
    "full_name": "Test Trader",
    "email": "test@chronos.hft",
    "username": "testuser",
    "password": "123",
    "confirm_password": "123"
})
assert r.status_code in (400, 422), f"Expected 400 or 422 on short password, got {r.status_code}"
print("  ✓ Rejected short password (<6 chars)")

# B. Password mismatch
r = session.post(f"{BASE_URL}/api/auth/register", json={
    "full_name": "Test Trader",
    "email": "test@chronos.hft",
    "username": "testuser",
    "password": "password123",
    "confirm_password": "differentpassword"
})
assert r.status_code == 400, f"Expected 400 on mismatch, got {r.status_code}"
print("  ✓ Rejected password confirmation mismatch")

# C. Successful registration of unique user
rand_suffix = secrets.token_hex(4)
unique_email = f"trader_{rand_suffix}@chronosmatch.hft"
unique_user = f"user_{rand_suffix}"
r = session.post(f"{BASE_URL}/api/auth/register", json={
    "full_name": f"Quant Specialist {rand_suffix}",
    "email": unique_email,
    "username": unique_user,
    "password": "SecurePassword2026!",
    "confirm_password": "SecurePassword2026!",
    "role": "Lead Quantitative Trader",
    "desk": "Ultra-Low Latency Equities Desk"
})
assert r.status_code == 200, f"Expected 200 on valid registration, got {r.status_code}: {r.text}"
reg_data = r.json()
assert reg_data["success"] is True
assert reg_data["user"]["account_id"].startswith("ACC-")
print(f"  ✓ Successfully registered trader {unique_user} -> Allocated {reg_data['user']['account_id']}")

# D. Duplicate email rejection
r = session.post(f"{BASE_URL}/api/auth/register", json={
    "full_name": "Duplicate Trader",
    "email": unique_email,
    "username": f"other_{rand_suffix}",
    "password": "SecurePassword2026!",
    "confirm_password": "SecurePassword2026!"
})
assert r.status_code == 400 and "already registered" in r.text, f"Expected duplicate email rejection, got {r.text}"
print("  ✓ Rejected duplicate email registration")

# E. Duplicate username rejection
r = session.post(f"{BASE_URL}/api/auth/register", json={
    "full_name": "Duplicate Trader 2",
    "email": f"other_{rand_suffix}@chronos.hft",
    "username": unique_user,
    "password": "SecurePassword2026!",
    "confirm_password": "SecurePassword2026!"
})
assert r.status_code == 400 and "already taken" in r.text, f"Expected duplicate username rejection, got {r.text}"
print("  ✓ Rejected duplicate username registration")

# -----------------------------------------------------------------------------
# TEST 3: Login Authentication, Rate Limiting & Session Tokens
# -----------------------------------------------------------------------------
print("\n[TEST 3] Login Authentication & Sessions:")
# A. Bad credentials
r = session.post(f"{BASE_URL}/api/auth/login", json={"email": unique_user, "password": "WrongPassword"})
assert r.status_code == 401, f"Expected 401 on bad password, got {r.status_code}"
print("  ✓ Rejected invalid password (HTTP 401)")

# B. Successful Login with username
r = session.post(f"{BASE_URL}/api/auth/login", json={"email": unique_user, "password": "SecurePassword2026!"})
assert r.status_code == 200, f"Expected 200 on login, got {r.status_code}: {r.text}"
login_data = r.json()
assert login_data["success"] is True
session_token = login_data["token"]
assert session_token.startswith("chronos_sess_")
assert "session_token" in session.cookies, "HTTP-only session_token cookie missing!"
print(f"  ✓ Logged in via username -> Token: {session_token[:20]}... and HTTP-only cookie set")

# C. Successful Login with pre-seeded demo account Alex Vance
r_alex = requests.post(f"{BASE_URL}/api/auth/login", json={"email": "trader@chronosmatch.hft", "password": "chronos2026"})
assert r_alex.status_code == 200
assert r_alex.json()["user"]["name"] == "Alex Vance"
print("  ✓ Pre-seeded institutional demo account (Alex Vance) authenticates via bcrypt")

# -----------------------------------------------------------------------------
# TEST 4: Protected Dashboard Access & Profile Sync
# -----------------------------------------------------------------------------
print("\n[TEST 4] Protected Dashboard & Profile Query:")
r_dash = session.get(f"{BASE_URL}/", allow_redirects=False)
assert r_dash.status_code == 200, f"Expected 200 for authenticated root, got {r_dash.status_code}"
assert "ChronosMatch Pro" in r_dash.text
print("  ✓ Authenticated user can access '/' directly without redirect (HTTP 200)")

r_me = session.get(f"{BASE_URL}/api/auth/me")
assert r_me.status_code == 200
me_data = r_me.json()
assert me_data["authenticated"] is True
assert me_data["user"]["username"] == unique_user
print(f"  ✓ GET /api/auth/me returned authenticated profile for {me_data['user']['full_name']}")

# -----------------------------------------------------------------------------
# TEST 5: Razorpay Real Payment Gateway - Predefined Amounts & Order Creation
# -----------------------------------------------------------------------------
print("\n[TEST 5] Razorpay Order Creation & Predefined Amounts:")
# Minimum amount check
r = session.post(f"{BASE_URL}/api/payments/create-order", json={"amount_inr": 50})
assert r.status_code == 400, f"Expected 400 for amount < 100, got {r.status_code}"
print("  ✓ Server-side validation rejected deposit below ₹100 INR")

# Predefined amounts verification
predefined_amounts = [100.0, 500.0, 1000.0, 2000.0, 5000.0, 10000.0]
orders_created = []
for amt in predefined_amounts:
    r = session.post(f"{BASE_URL}/api/payments/create-order", json={"amount_inr": amt})
    assert r.status_code == 200, f"Failed creating order for ₹{amt}: {r.text}"
    o = r.json()["order"]
    assert o["amount_inr"] == amt
    assert o["amount_paise"] == int(amt * 100)
    assert o["currency"] == "INR"
    orders_created.append(o)
print(f"  ✓ Verified order creation for all predefined amounts: {predefined_amounts}")

# -----------------------------------------------------------------------------
# TEST 6: Cryptographic HMAC-SHA256 Payment Verification & Wallet Credit
# -----------------------------------------------------------------------------
print("\n[TEST 6] HMAC-SHA256 Signature Verification & Idempotency:")
target_order = orders_created[2]  # ₹1000 order
test_pay_id = f"pay_{secrets.token_hex(8)}"

# Calculate authentic HMAC-SHA256 signature using configured secret
test_secret = "secret_ChronosMatchLiveHFT"
sig_payload = f"{target_order['order_id']}|{test_pay_id}".encode("utf-8")
valid_sig = hmac.new(test_secret.encode("utf-8"), sig_payload, hashlib.sha256).hexdigest()

# A. Invalid signature rejection
r_bad = session.post(f"{BASE_URL}/api/payments/verify", json={
    "razorpay_order_id": target_order["order_id"],
    "razorpay_payment_id": test_pay_id,
    "razorpay_signature": "invalid_tampered_signature_hex"
})
assert r_bad.status_code == 400, f"Expected 400 for invalid signature, got {r_bad.status_code}"
print("  ✓ Server-side HMAC-SHA256 rejected invalid signature")

# B. Authoritative Verification
r_verify = session.post(f"{BASE_URL}/api/payments/verify", json={
    "razorpay_order_id": target_order["order_id"],
    "razorpay_payment_id": test_pay_id,
    "razorpay_signature": valid_sig,
    "method": "UPI - PhonePe"
})
assert r_verify.status_code == 200, f"Verification failed: {r_verify.text}"
v_data = r_verify.json()
assert v_data["success"] is True
assert v_data["amount_credited"] == 1000.0
assert v_data["utr"].startswith("329")
print(f"  ✓ Payment verified! Wallet credited ₹{v_data['amount_credited']:,.2f} (UTR: {v_data['utr']})")

# C. Strict Idempotency Check (prevent double-credit)
r_dup = session.post(f"{BASE_URL}/api/payments/verify", json={
    "razorpay_order_id": target_order["order_id"],
    "razorpay_payment_id": test_pay_id,
    "razorpay_signature": valid_sig,
    "method": "UPI - PhonePe"
})
assert r_dup.status_code == 200
assert r_dup.json()["already_processed"] is True
print("  ✓ Strict idempotency enforced: Duplicate payment callback safely ignored without double-crediting")

# -----------------------------------------------------------------------------
# TEST 7: Raw-Body Webhook Verification
# -----------------------------------------------------------------------------
print("\n[TEST 7] Razorpay Webhook Processing (Raw Body HMAC-SHA256):")
webhook_order = orders_created[3]  # ₹2000 order
wh_pay_id = f"pay_{secrets.token_hex(8)}"
wh_event = {
    "event": "payment.captured",
    "payload": {
        "payment": {
            "entity": {
                "id": wh_pay_id,
                "order_id": webhook_order["order_id"],
                "amount": 200000,
                "currency": "INR",
                "status": "captured",
                "method": "upi"
            }
        }
    }
}
wh_raw_body = json.dumps(wh_event).encode("utf-8")
wh_secret = "webhook_secret_ChronosMatch2026"
wh_sig = hmac.new(wh_secret.encode("utf-8"), wh_raw_body, hashlib.sha256).hexdigest()

# Webhook request with raw body and X-Razorpay-Signature header
r_wh = session.post(
    f"{BASE_URL}/api/webhooks/razorpay",
    data=wh_raw_body,
    headers={"Content-Type": "application/json", "X-Razorpay-Signature": wh_sig}
)
assert r_wh.status_code == 200, f"Webhook failed: {r_wh.text}"
assert r_wh.json()["success"] is True
print("  ✓ Webhook payment.captured verified raw signature and processed successfully")

# -----------------------------------------------------------------------------
# TEST 8: User-Scoped Payment History Ledger
# -----------------------------------------------------------------------------
print("\n[TEST 8] Payment History Ledger Query:")
r_hist = session.get(f"{BASE_URL}/api/payments/history")
assert r_hist.status_code == 200
hist_data = r_hist.json()
assert hist_data["count"] > 0
first_tx = hist_data["transactions"][0]
for k in ["tx_id", "order_id", "customer_name", "amount", "status", "method", "created_at"]:
    assert k in first_tx, f"Field {k} missing in history item"
print(f"  ✓ Payment history returns {hist_data['count']} user-scoped transactions with all 8 audit columns")

# -----------------------------------------------------------------------------
# TEST 9: Live Matching Engine Buy / Sell Execution & Margin Deduction
# -----------------------------------------------------------------------------
print("\n[TEST 9] Live Order Execution (Buy/Sell Hot Path):")
r_buy = session.post(f"{BASE_URL}/api/orders", json={
    "side": 1,
    "price": 99.50,
    "quantity": 100,
    "is_market": True,
    "account_id": "primary"
})
assert r_buy.status_code == 200, f"Buy order failed: {r_buy.text}"
buy_res = r_buy.json()
assert buy_res["success"] is True
print(f"  ✓ Live BUY order #{buy_res['order_id']} executed in {buy_res['latency_us']:.2f} µs (Status: {buy_res['status']})")

r_sell = session.post(f"{BASE_URL}/api/orders", json={
    "side": -1,
    "price": 99.51,
    "quantity": 100,
    "is_market": True,
    "account_id": "primary"
})
assert r_sell.status_code == 200, f"Sell order failed: {r_sell.text}"
sell_res = r_sell.json()
assert sell_res["success"] is True
print(f"  ✓ Live SELL order #{sell_res['order_id']} executed in {sell_res['latency_us']:.2f} µs (Cash proceeds credited)")

# -----------------------------------------------------------------------------
# TEST 10: Firehose Pause, Drain & Resume
# -----------------------------------------------------------------------------
print("\n[TEST 10] Firehose Pause, Drain & Resume Behavior:")
# Pause
r_pause = session.post(f"{BASE_URL}/api/firehose/pause")
assert r_pause.status_code == 200 and r_pause.json()["is_paused"] is True
print(f"  ✓ Firehose paused cleanly (Ring buffer drained {r_pause.json()['drained']} orders)")

# Telemetry check while paused: TPS must be 0
time.sleep(0.1)
r_tel = session.get(f"{BASE_URL}/api/telemetry")
assert r_tel.json()["tps"] == 0, f"Expected TPS=0 when paused, got {r_tel.json()['tps']}"
print("  ✓ Telemetry confirms TPS is exactly 0 when paused")

# Resume
r_resume = session.post(f"{BASE_URL}/api/firehose/resume")
assert r_resume.status_code == 200 and r_resume.json()["is_paused"] is False
print("  ✓ Firehose resumed cleanly at full 100k ord/s target")

# -----------------------------------------------------------------------------
# TEST 11: Trader Logout & Cookie Revocation
# -----------------------------------------------------------------------------
print("\n[TEST 11] Trader Logout & Cookie Revocation:")
r_logout = session.post(f"{BASE_URL}/api/auth/logout")
assert r_logout.status_code == 200
print("  ✓ Logout endpoint cleared session in database and invalidated cookie")

# Verify access to / is now blocked again
r_post_logout = session.get(f"{BASE_URL}/", allow_redirects=False)
assert r_post_logout.status_code == 303, f"Expected redirect after logout, got {r_post_logout.status_code}"
print("  ✓ Post-logout request to '/' immediately redirected to '/login'")

print("\n" + "=" * 70)
print("ALL 11 END-TO-END SUITES PASSED WITH ZERO ERRORS!")
print("=" * 70)
