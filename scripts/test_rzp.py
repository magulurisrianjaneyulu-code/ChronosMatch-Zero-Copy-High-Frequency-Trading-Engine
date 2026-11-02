import sys, hmac, hashlib
sys.path.insert(0, r"C:\Users\GOPI V\Desktop\hft_engine")
from core.payment_razorpay import RazorpayManager

rzp = RazorpayManager()
print("RazorpayManager initialized successfully")

# 1. Order Creation
order = rzp.create_order(500.0, user_id=1, customer_name="Gopi V")
print("Order created:", order["order_id"], "Amount:", order["amount_inr"], "Key:", order["key_id"][:12])

# 2. Signature verification
pay_id = "pay_test123456"
payload = f"{order['order_id']}|{pay_id}".encode("utf-8")
sig = hmac.new(rzp.key_secret.encode("utf-8"), payload, hashlib.sha256).hexdigest()
v_res = rzp.verify_payment(order["order_id"], pay_id, sig, user_id=1)
print("Payment verification:", v_res["success"], "Credited:", v_res["amount_credited"])

# 3. Duplicate callback (idempotency)
dup_res = rzp.verify_payment(order["order_id"], pay_id, sig, user_id=1)
print("Duplicate prevented:", dup_res["success"], "Already processed:", dup_res.get("already_processed"))

# 4. Webhook testing
import json
wh_order = rzp.create_order(1000.0, user_id=1, customer_name="Webhook User")
wh_payload = {
    "event": "payment.captured",
    "payload": {
        "payment": {
            "entity": {
                "id": "pay_wh987654",
                "order_id": wh_order["order_id"],
                "method": "UPI - GPay",
                "amount": 100000
            }
        }
    }
}
raw_wh_bytes = json.dumps(wh_payload).encode("utf-8")
wh_sig = hmac.new(rzp.webhook_secret.encode("utf-8"), raw_wh_bytes, hashlib.sha256).hexdigest()
wh_res = rzp.verify_and_process_webhook(raw_wh_bytes, wh_sig)
print("Webhook processed:", wh_res["success"], "Event:", wh_res["event"])

# 5. History check
history = rzp.get_payment_history(user_id=1)
print("Payment history count:", len(history), "Statuses:", [h["status"] for h in history[:3]])
print("TEST COMPLETED WITH ZERO ERRORS!")
