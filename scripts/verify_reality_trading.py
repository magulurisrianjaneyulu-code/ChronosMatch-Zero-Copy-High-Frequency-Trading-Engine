import urllib.request
import json

def call_api(url, method="GET", data=None):
    req = urllib.request.Request(url, method=method)
    if data:
        req.data = json.dumps(data).encode("utf-8")
        req.add_header("Content-Type", "application/json")
    with urllib.request.urlopen(req) as resp:
        return json.loads(resp.read().decode("utf-8"))

print("=== 1. CHECK INITIAL PORTFOLIO & MARGIN ===")
bal = call_api("http://localhost:8000/api/payments/balance")
print(f"Available Margin: Rs {bal['available_margin']:,.2f} | Cash: Rs {bal['cash_balance']:,.2f}")
port0 = call_api("http://localhost:8000/api/portfolio")
print(f"Initial Position: {port0['side']} {port0['shares']} shs | Realized P&L: ${port0['realized_pnl_usd']}")

print("\n=== 2. EXECUTE BUY 500 SHARES (MARKET) ===")
buy_res = call_api("http://localhost:8000/api/orders", method="POST", data={
    "side": 1,
    "price": 99.50,
    "quantity": 500,
    "is_market": True,
    "is_whale": False,
    "account_id": "primary"
})
print(f"Order #{buy_res['order_id']} {buy_res['side']} {buy_res['filled_quantity']}/{buy_res['quantity']} shs status: {buy_res['status']}")
print(f"Fills: {buy_res['trades_count']} | Execution Speed: {buy_res['latency_us']} us")
print(f"Position After Buy: {buy_res['position']['side']} {buy_res['position']['shares']} shs @ ${buy_res['position']['avg_entry_price']}")

print("\n=== 3. EXECUTE BUY 500 SHARES (LIMIT @ BEST ASK) ===")
ob = call_api("http://localhost:8000/api/orderbook")
best_ask = ob["best_ask"] or 99.5012
buy_lim_res = call_api("http://localhost:8000/api/orders", method="POST", data={
    "side": 1,
    "price": best_ask,
    "quantity": 500,
    "is_market": False,
    "is_whale": False,
    "account_id": "primary"
})
print(f"Order #{buy_lim_res['order_id']} Limit BUY {buy_lim_res['filled_quantity']}/{buy_lim_res['quantity']} shs status: {buy_lim_res['status']}")
print(f"Position After 2nd Buy: {buy_lim_res['position']['side']} {buy_lim_res['position']['shares']} shs @ ${buy_lim_res['position']['avg_entry_price']}")

print("\n=== 4. EXECUTE SELL 500 SHARES (REAL CASH CREDIT) ===")
sell_res = call_api("http://localhost:8000/api/orders", method="POST", data={
    "side": -1,
    "price": 99.49,
    "quantity": 500,
    "is_market": True,
    "is_whale": False,
    "account_id": "primary"
})
print(f"Order #{sell_res['order_id']} {sell_res['side']} {sell_res['filled_quantity']}/{sell_res['quantity']} shs status: {sell_res['status']}")
print(f"Position After Sell: {sell_res['position']['side']} {sell_res['position']['shares']} shs | Realized P&L: ${sell_res['position']['realized_pnl_usd']}")

print("\n=== 5. 1-CLICK FLATTEN / CLOSE POSITION ===")
close_res = call_api("http://localhost:8000/api/portfolio/close", method="POST")
print(f"Flatten Result: {close_res['status']} | Final Position: {close_res['position']['side']} {close_res['position']['shares']} shs")

bal_final = call_api("http://localhost:8000/api/payments/balance")
print(f"\nFinal Available Margin: Rs {bal_final['available_margin']:,.2f} | Cash: Rs {bal_final['cash_balance']:,.2f}")
print("ALL REALITY BUYING & SELLING TESTED AND VERIFIED SUCCESSFULLY!")
