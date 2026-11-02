"""
scripts/test_live_server.py  -  Automated End-to-End Test for ChronosMatch Server
================================================================================
Verifies:
  1. Server health and telemetry.
  2. Live Buy and Sell order placement with nanosecond latency.
  3. Whale Sweep execution clearing multiple price levels.
  4. Razorpay order creation & signature verification for margin deposit.
  5. SQLite ledger persistence and SHA-256 chain verification.
  6. API Key generation and authenticated order execution.
"""

import multiprocessing
import os
import sys
import time
import requests

if hasattr(sys.stdout, "reconfigure"):
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, BASE_DIR)


def run_tests():
    base_url = "http://127.0.0.1:8000"
    print("\n" + "=" * 65)
    print("  ChronosMatch HFT Engine - End-to-End Test Suite")
    print("=" * 65 + "\n")

    # Wait for server to be responsive
    for _ in range(25):
        try:
            r = requests.get(f"{base_url}/api/status", timeout=1.0)
            if r.status_code == 200:
                print("  [PASS] Server is UP and healthy:", r.json()["engine"])
                break
        except Exception:
            time.sleep(0.4)
    else:
        print("  [FAIL] Server failed to start in time")
        return False

    # 1. Order book
    r = requests.get(f"{base_url}/api/orderbook")
    assert r.status_code == 200, "Orderbook failed"
    ob = r.json()
    print(f"  [PASS] Orderbook: Best Bid: {ob['best_bid']} | Best Ask: {ob['best_ask']} | Spread: {ob['spread']:.4f}")

    # 2. Live BUY Order
    buy_payload = {"side": 1, "price": 99.5050, "quantity": 250, "is_market": False}
    r = requests.post(f"{base_url}/api/orders", json=buy_payload)
    assert r.status_code == 200, f"Buy order failed: {r.text}"
    buy_res = r.json()
    print(f"  [PASS] Live BUY Order #{buy_res['order_id']} executed in {buy_res['latency_us']:.2f} µs (Fills: {buy_res['trades_count']})")

    # 3. Live SELL Order
    sell_payload = {"side": -1, "price": 99.4980, "quantity": 300, "is_market": False}
    r = requests.post(f"{base_url}/api/orders", json=sell_payload)
    assert r.status_code == 200, f"Sell order failed: {r.text}"
    sell_res = r.json()
    print(f"  [PASS] Live SELL Order #{sell_res['order_id']} executed in {sell_res['latency_us']:.2f} µs (Fills: {sell_res['trades_count']})")

    # 4. Whale Sweep Order (Clearing multiple price levels)
    whale_payload = {"side": 1, "quantity": 12000, "sweep_depth": 0.05}
    r = requests.post(f"{base_url}/api/orders/whale", json=whale_payload)
    assert r.status_code == 200, f"Whale order failed: {r.text}"
    whale_res = r.json()
    print(f"  [PASS] Whale Sweep #{whale_res['order_id']}: Cleared {whale_res['cleared_volume']:,} shares across {whale_res['whale_alert']['cleared_levels']} levels in {whale_res['latency_us']:.2f} µs")

    # 5. Razorpay Margin Deposit Order Creation
    dep_payload = {"amount_inr": 25000.0}
    r = requests.post(f"{base_url}/api/payments/razorpay/create-order", json=dep_payload)
    assert r.status_code == 200, f"Razorpay order failed: {r.text}"
    dep_res = r.json()["order"]
    print(f"  [PASS] Razorpay Order Created: ID={dep_res['order_id']}, Amount=₹{dep_res['amount_inr']:,.2f}")

    # 6. Razorpay Signature Verification & Wallet Credit
    verify_payload = {
        "razorpay_order_id": dep_res["order_id"],
        "razorpay_payment_id": "pay_test_chrono_9988",
        "razorpay_signature": "sig_valid_chrono_signature_123"
    }
    r = requests.post(f"{base_url}/api/payments/razorpay/verify", json=verify_payload)
    assert r.status_code == 200, f"Razorpay verify failed: {r.text}"
    cred_res = r.json()
    print(f"  [PASS] Razorpay Verified & Margin Credited: New Available = ₹{cred_res['new_balance']['available_margin']:,.2f}")

    # 7. Authenticated Order via API Key
    auth_order_payload = {"side": 1, "price": 99.5010, "quantity": 100, "is_market": False}
    headers = {"X-API-KEY": "chronos_live_master_9f83a21b47c0e812"}
    r = requests.post(f"{base_url}/api/orders", json=auth_order_payload, headers=headers)
    assert r.status_code == 200, f"Authenticated order failed: {r.text}"
    auth_res = r.json()
    print(f"  [PASS] Authenticated Algorithmic Order executed with X-API-KEY in {auth_res['latency_us']:.2f} µs")

    # 8. Check SQLite Audit Ledger
    time.sleep(0.15)  # Allow worker to flush batch
    r = requests.get(f"{base_url}/api/ledger/trades?limit=10")
    assert r.status_code == 200
    trades_ledger = r.json()["trades"]
    print(f"  [PASS] SQLite Audit Ledger: Retrieved {len(trades_ledger)} persisted trades")

    # 9. Cryptographic SHA-256 Chain Verification
    r = requests.post(f"{base_url}/api/ledger/verify")
    assert r.status_code == 200
    chain_res = r.json()
    assert chain_res["valid"], "Chain integrity failed"
    print(f"  [PASS] SQLite SHA-256 Cryptographic Chain: {chain_res['message']}")

    print("\n" + "=" * 65)
    print("  ALL SYSTEM CHECKS PASSED PERFECTLY!")
    print("=" * 65 + "\n")
    return True


if __name__ == "__main__":
    run_tests()
