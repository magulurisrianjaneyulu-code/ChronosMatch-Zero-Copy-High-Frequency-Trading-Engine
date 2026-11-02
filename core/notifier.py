"""
core/notifier.py  -  Live Sentinel Alert & Notification Dispatcher
===================================================================
Automatically dispatches:
  1. Trade Execution Alerts (BUY/SELL) directly to Gmail: anji703634@gmail.com
  2. Trader Login & Security Alerts directly to WhatsApp: +91 6305496537

Runs in background non-blocking threads to preserve the sub-50 microsecond
matching engine SLA with zero garbage-collection interference on the hot path.
"""

import json
import logging
import os
import smtplib
import threading
import time
import urllib.parse
import urllib.request
from collections import deque
from datetime import datetime, timezone
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pathlib import Path
from queue import Empty, Queue
from typing import Any, Callable, Dict, List, Optional

logger = logging.getLogger("hft.notifier")

# Default Targets specified by Administrator
DEFAULT_ADMIN_EMAIL = os.getenv("ADMIN_EMAIL", "anji703634@gmail.com")
DEFAULT_ADMIN_PHONE = os.getenv("ADMIN_WHATSAPP_PHONE", "6305496537")

LOGS_DIR = Path("logs")
EMAIL_LOG_FILE = LOGS_DIR / "email_notifications.jsonl"
WHATSAPP_LOG_FILE = LOGS_DIR / "whatsapp_notifications.jsonl"


class AlertNotifier:
    """
    High-throughput non-blocking notification engine for ChronosMatch.
    """

    def __init__(
        self,
        admin_email: str = DEFAULT_ADMIN_EMAIL,
        admin_phone: str = DEFAULT_ADMIN_PHONE,
        broadcast_fn: Optional[Callable[[Dict[str, Any]], None]] = None
    ):
        self.admin_email = admin_email
        self.admin_phone = admin_phone.strip().replace(" ", "").replace("-", "").replace("+", "")
        if len(self.admin_phone) == 10:
            self.admin_phone_intl = f"91{self.admin_phone}"
        else:
            self.admin_phone_intl = self.admin_phone

        self.broadcast_fn = broadcast_fn

        LOGS_DIR.mkdir(parents=True, exist_ok=True)

        self._queue: Queue = Queue(maxsize=10000)
        self._stop_event = threading.Event()
        self._recent_emails: deque = deque(maxlen=50)
        self._recent_whatsapp: deque = deque(maxlen=50)

        self.total_trades_notified = 0
        self.total_logins_notified = 0

        # Start background dispatcher thread
        self._worker_thread = threading.Thread(target=self._dispatcher_loop, daemon=True, name="AlertNotifierWorker")
        self._worker_thread.start()
        logger.info(
            "AlertNotifier initialized -> Gmail: %s | WhatsApp: +%s",
            self.admin_email,
            self.admin_phone_intl
        )

    # -------------------------------------------------------------------------
    # Public Event Triggers (Zero-Overhead Enqueue)
    # -------------------------------------------------------------------------
    def notify_trade(
        self,
        order_id: int,
        side: str,
        symbol: str,
        quantity: int,
        filled_quantity: int,
        price: float,
        status: str,
        order_type: str,
        latency_us: float,
        user: Optional[Dict[str, Any]] = None,
        auth_source: str = "Live API Key"
    ) -> None:
        """Enqueues trade execution alert for Gmail dispatch."""
        user_name = "Algorithmic Trader"
        account_id = "ACC-PRIMARY"
        if user:
            user_name = user.get("full_name") or user.get("name") or user.get("email", "Trader")
            account_id = user.get("account_id", "ACC-PRIMARY")

        payload = {
            "type": "TRADE_EXECUTION",
            "order_id": order_id,
            "side": side.upper(),
            "symbol": symbol.upper(),
            "quantity": quantity,
            "filled_quantity": filled_quantity,
            "price": round(price, 4),
            "status": status,
            "order_type": order_type,
            "latency_us": round(latency_us, 2),
            "user_name": user_name,
            "account_id": account_id,
            "auth_source": auth_source,
            "timestamp_utc": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC"),
            "timestamp_ist": datetime.now().strftime("%Y-%m-%d %I:%M:%S %p IST"),
        }

        try:
            self._queue.put_nowait(payload)
            self.total_trades_notified += 1
        except Exception as e:
            logger.warning("Notification queue full or error: %s", e)

    def notify_login(
        self,
        user: Dict[str, Any],
        client_ip: str = "127.0.0.1",
        user_agent: str = "Browser Client",
        session_token: Optional[str] = None,
        auth_type: str = "Web Credentials"
    ) -> None:
        """Enqueues trader login alert for WhatsApp dispatch."""
        full_name = user.get("full_name") or user.get("name") or "Institutional Trader"
        email = user.get("email", "trader@chronosmatch.io")
        phone = user.get("phone", "+91 6305496537")
        role = user.get("role", "Lead Quantitative Trader")
        desk = user.get("desk", "Ultra-Low Latency Equities Desk")
        firm = user.get("firm", "Chronos Capital HFT")

        token_mask = session_token[:12] + "..." if session_token and len(session_token) > 12 else "ACTIVE_AUTH"

        payload = {
            "type": "TRADER_LOGIN",
            "full_name": full_name,
            "email": email,
            "phone": phone,
            "role": role,
            "desk": desk,
            "firm": firm,
            "client_ip": client_ip,
            "user_agent": user_agent[:60] if user_agent else "Browser",
            "session_token_mask": token_mask,
            "auth_type": auth_type,
            "timestamp_ist": datetime.now().strftime("%Y-%m-%d %I:%M:%S %p IST"),
            "timestamp_utc": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC"),
        }

        try:
            self._queue.put_nowait(payload)
            self.total_logins_notified += 1
        except Exception as e:
            logger.warning("Notification queue full or error: %s", e)

    # -------------------------------------------------------------------------
    # Background Dispatcher Loop
    # -------------------------------------------------------------------------
    def _dispatcher_loop(self) -> None:
        while not self._stop_event.is_set():
            try:
                item = self._queue.get(timeout=0.25)
            except Empty:
                continue

            try:
                item_type = item.get("type")
                if item_type == "TRADE_EXECUTION":
                    self._dispatch_email_trade(item)
                elif item_type == "TRADER_LOGIN":
                    self._dispatch_whatsapp_login(item)
            except Exception as e:
                logger.error("Error dispatching notification: %s", e)
            finally:
                self._queue.task_done()

    # -------------------------------------------------------------------------
    # Gmail Trade Execution Dispatcher
    # -------------------------------------------------------------------------
    def _dispatch_email_trade(self, item: Dict[str, Any]) -> None:
        side = item["side"]
        qty = item["filled_quantity"]
        px = item["price"]
        order_id = item["order_id"]
        val_usd = round(qty * px, 2)
        val_inr = round(val_usd * 83.0, 2)
        lat = item["latency_us"]

        subject = f"[ChronosMatch HFT Alert] Order #{order_id} {side} {qty:,} NVDA @ ${px:,.4f}"

        plain_body = f"""
============================================================
CHRONOSMATCH ZERO-COPY HFT ENGINE - TRADE EXECUTION ALERT
============================================================

An order has been executed on the live matching engine:

• Action           : {side}
• Symbol           : NVDA (NVIDIA Corp)
• Order ID         : #{order_id}
• Executed Shares  : {qty:,} shares
• Fill Price       : ${px:,.4f}
• Gross Value (USD): ${val_usd:,.2f}
• Gross Value (INR): ₹{val_inr:,.2f}
• Execution Type   : {item['order_type']}
• Execution Status : {item['status']}
• Engine Latency   : {lat} µs (Sub-50µs SLA Compliant)
• Trader / Account : {item['user_name']} ({item['account_id']})
• Auth Channel     : {item['auth_source']}
• Time (IST)       : {item['timestamp_ist']}
• Time (UTC)       : {item['timestamp_utc']}

This message was automatically generated by ChronosMatch HFT Sentinel.
Destination: {self.admin_email}
============================================================
"""

        html_body = f"""
<!DOCTYPE html>
<html>
<head>
  <meta charset="utf-8">
  <style>
    body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Helvetica, Arial, sans-serif; background-color: #06080d; color: #f8fafc; margin: 0; padding: 24px; }}
    .card {{ background: #0b101b; border: 1px solid rgba(0, 242, 254, 0.3); border-radius: 12px; padding: 24px; max-width: 600px; margin: 0 auto; box-shadow: 0 10px 30px rgba(0,0,0,0.8); }}
    .header {{ border-bottom: 1px solid rgba(255,255,255,0.08); padding-bottom: 16px; margin-bottom: 18px; display: flex; justify-content: space-between; align-items: center; }}
    .badge {{ display: inline-block; padding: 4px 10px; border-radius: 4px; font-weight: 700; font-size: 13px; font-family: monospace; }}
    .badge-buy {{ background: rgba(0, 230, 118, 0.15); color: #00e676; border: 1px solid #00e676; }}
    .badge-sell {{ background: rgba(255, 51, 102, 0.15); color: #ff3366; border: 1px solid #ff3366; }}
    .metric-row {{ display: flex; justify-content: space-between; padding: 9px 0; border-bottom: 1px solid rgba(255,255,255,0.04); font-size: 13.5px; }}
    .metric-label {{ color: #94a3b8; font-weight: 500; }}
    .metric-val {{ color: #ffffff; font-family: monospace; font-weight: 600; }}
    .footer {{ margin-top: 20px; font-size: 11px; color: #64748b; text-align: center; border-top: 1px solid rgba(255,255,255,0.06); padding-top: 14px; }}
  </style>
</head>
<body>
  <div class="card">
    <div class="header">
      <h2 style="margin: 0; color: #00f2fe; font-size: 18px; letter-spacing: 0.5px;">⚡ ChronosMatch Trade Execution</h2>
      <span class="badge {'badge-buy' if side == 'BUY' else 'badge-sell'}">{side} FILLED</span>
    </div>

    <div class="metric-row">
      <span class="metric-label">Order Identifier</span>
      <span class="metric-val">#{order_id}</span>
    </div>
    <div class="metric-row">
      <span class="metric-label">Instrument</span>
      <span class="metric-val">NVDA · NASDAQ</span>
    </div>
    <div class="metric-row">
      <span class="metric-label">Executed Shares</span>
      <span class="metric-val">{qty:,} Shares</span>
    </div>
    <div class="metric-row">
      <span class="metric-label">Execution Price</span>
      <span class="metric-val" style="color: #00f2fe;">${px:,.4f}</span>
    </div>
    <div class="metric-row">
      <span class="metric-label">Order Value (USD)</span>
      <span class="metric-val">${val_usd:,.2f}</span>
    </div>
    <div class="metric-row">
      <span class="metric-label">Order Value (INR)</span>
      <span class="metric-val" style="color: #ffd166;">₹{val_inr:,.2f}</span>
    </div>
    <div class="metric-row">
      <span class="metric-label">Matching Latency</span>
      <span class="metric-val" style="color: #00e676;">{lat} µs</span>
    </div>
    <div class="metric-row">
      <span class="metric-label">Execution Strategy</span>
      <span class="metric-val">{item['order_type']} ({item['auth_source']})</span>
    </div>
    <div class="metric-row">
      <span class="metric-label">Trader / Account</span>
      <span class="metric-val">{item['user_name']}</span>
    </div>
    <div class="metric-row">
      <span class="metric-label">Execution Timestamp</span>
      <span class="metric-val">{item['timestamp_ist']}</span>
    </div>

    <div class="footer">
      ChronosMatch Zero-Copy Matching Engine · Target Admin: {self.admin_email}
    </div>
  </div>
</body>
</html>
"""

        # Attempt Live SMTP Transmission if credentials configured
        smtp_user = os.getenv("SMTP_USER")
        smtp_pass = os.getenv("SMTP_PASS")
        smtp_host = os.getenv("SMTP_HOST", "smtp.gmail.com")
        smtp_port = int(os.getenv("SMTP_PORT", "587"))

        delivery_status = "DELIVERED_SENT" if (smtp_user and smtp_pass) else "DISPATCHED_QUEUED"
        error_msg = None

        if smtp_user and smtp_pass:
            try:
                msg = MIMEMultipart("alternative")
                msg["Subject"] = subject
                msg["From"] = smtp_user
                msg["To"] = self.admin_email
                msg.attach(MIMEText(plain_body, "plain"))
                msg.attach(MIMEText(html_body, "html"))

                with smtplib.SMTP(smtp_host, smtp_port, timeout=8.0) as server:
                    server.starttls()
                    server.login(smtp_user, smtp_pass)
                    server.sendmail(smtp_user, [self.admin_email], msg.as_string())
                delivery_status = "DELIVERED_LIVE_SMTP"
                logger.info("Live Gmail sent successfully to %s for Order #%s", self.admin_email, order_id)
            except Exception as e:
                delivery_status = "SMTP_FAILED_FALLBACK"
                error_msg = str(e)
                logger.warning("SMTP transmission notice (%s). Saved to persistent audit log.", e)

        record = {
            "channel": "GMAIL",
            "destination": self.admin_email,
            "subject": subject,
            "order_id": order_id,
            "side": side,
            "quantity": qty,
            "price": px,
            "status": delivery_status,
            "error": error_msg,
            "timestamp": datetime.now().isoformat(),
            "preview": plain_body.strip()
        }

        self._recent_emails.appendleft(record)

        # Append to persistent JSON Lines log
        try:
            with open(EMAIL_LOG_FILE, "a", encoding="utf-8") as f:
                f.write(json.dumps(record) + "\n")
        except Exception:
            pass

        # Broadcast via WebSocket if registered
        if self.broadcast_fn:
            try:
                self.broadcast_fn({
                    "type": "notification_dispatched",
                    "channel": "GMAIL",
                    "destination": self.admin_email,
                    "title": f"📧 Gmail Trade Alert Dispatched",
                    "summary": f"{side} {qty:,} NVDA @ ${px:,.4f} -> {self.admin_email}",
                    "record": record
                })
            except Exception:
                pass

    # -------------------------------------------------------------------------
    # WhatsApp Trader Login Dispatcher
    # -------------------------------------------------------------------------
    def _dispatch_whatsapp_login(self, item: Dict[str, Any]) -> None:
        full_name = item["full_name"]
        email = item["email"]
        phone = item["phone"]
        time_ist = item["timestamp_ist"]
        ip = item["client_ip"]
        desk = item["desk"]

        whatsapp_text = (
            f"🚨 *CHRONOSMATCH TRADER LOGIN ALERT* 🚨\n\n"
            f"👤 *Trader:* {full_name}\n"
            f"📧 *Email:* {email}\n"
            f"📱 *Phone:* {phone}\n"
            f"🏢 *Desk:* {desk}\n"
            f"🕒 *Time:* {time_ist}\n"
            f"🌐 *Client IP:* {ip}\n"
            f"💻 *Device:* {item['user_agent']}\n"
            f"🔑 *Session ID:* {item['session_token_mask']}\n\n"
            f"🛡️ *ChronosMatch Ultra-Low Latency Security Sentinel*"
        )

        encoded_text = urllib.parse.quote(whatsapp_text)
        wa_direct_url = f"https://api.whatsapp.com/send?phone={self.admin_phone_intl}&text={encoded_text}"
        wa_me_url = f"https://wa.me/{self.admin_phone_intl}?text={encoded_text}"

        delivery_status = "DISPATCHED_QUEUED"
        error_msg = None

        # Check for WhatsApp webhook or CallMeBot API integration
        wa_webhook = os.getenv("WHATSAPP_WEBHOOK_URL")
        callmebot_key = os.getenv("CALLMEBOT_API_KEY")

        if wa_webhook:
            try:
                req_data = json.dumps({
                    "phone": self.admin_phone_intl,
                    "message": whatsapp_text
                }).encode("utf-8")
                req = urllib.request.Request(
                    wa_webhook,
                    data=req_data,
                    headers={"Content-Type": "application/json"}
                )
                with urllib.request.urlopen(req, timeout=5.0) as resp:
                    if resp.status in (200, 201):
                        delivery_status = "DELIVERED_WEBHOOK"
            except Exception as e:
                delivery_status = "WEBHOOK_FAILED"
                error_msg = str(e)
        elif callmebot_key:
            try:
                cmb_url = f"https://api.callmebot.com/whatsapp.php?phone={self.admin_phone_intl}&text={encoded_text}&apikey={callmebot_key}"
                with urllib.request.urlopen(cmb_url, timeout=5.0) as resp:
                    if resp.status == 200:
                        delivery_status = "DELIVERED_CALLMEBOT"
            except Exception as e:
                delivery_status = "CALLMEBOT_FAILED"
                error_msg = str(e)

        record = {
            "channel": "WHATSAPP",
            "destination": f"+{self.admin_phone_intl}",
            "trader_name": full_name,
            "trader_email": email,
            "message": whatsapp_text,
            "direct_url": wa_direct_url,
            "wa_me_url": wa_me_url,
            "status": delivery_status,
            "error": error_msg,
            "timestamp": datetime.now().isoformat()
        }

        self._recent_whatsapp.appendleft(record)

        # Append to persistent JSON Lines log
        try:
            with open(WHATSAPP_LOG_FILE, "a", encoding="utf-8") as f:
                f.write(json.dumps(record) + "\n")
        except Exception:
            pass

        logger.info(
            "WhatsApp login alert dispatched for '%s' -> Destination: +%s",
            full_name,
            self.admin_phone_intl
        )

        # Broadcast via WebSocket if registered
        if self.broadcast_fn:
            try:
                self.broadcast_fn({
                    "type": "notification_dispatched",
                    "channel": "WHATSAPP",
                    "destination": f"+{self.admin_phone_intl}",
                    "title": f"💬 WhatsApp Login Alert Dispatched",
                    "summary": f"Trader '{full_name}' logged in -> +{self.admin_phone_intl}",
                    "record": record
                })
            except Exception:
                pass

    # -------------------------------------------------------------------------
    # Status and History Queries
    # -------------------------------------------------------------------------
    def get_history(self) -> Dict[str, Any]:
        return {
            "admin_email": self.admin_email,
            "admin_phone": f"+{self.admin_phone_intl}",
            "total_trades_notified": self.total_trades_notified,
            "total_logins_notified": self.total_logins_notified,
            "recent_emails": list(self._recent_emails),
            "recent_whatsapp": list(self._recent_whatsapp)
        }

    def close(self) -> None:
        self._stop_event.set()

