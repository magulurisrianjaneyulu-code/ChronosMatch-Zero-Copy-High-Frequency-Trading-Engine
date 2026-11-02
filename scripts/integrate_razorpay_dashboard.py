with open(r"C:\Users\GOPI V\Desktop\hft_engine\ui\web_dashboard.html", "r", encoding="utf-8") as f:
    html = f.read()

# 1. Update old VPA chronosmatch@icici to 6305496537-4@ybl
html = html.replace("chronosmatch@icici", "6305496537-4@ybl")

# 2. Update modal-withdraw destination
html = html.replace('value="trader@okhdfcbank"', 'value="6305496537-4@ybl"')
html = html.replace('placeholder="e.g. trader@okhdfcbank or 501004928192"', 'placeholder="e.g. 6305496537-4@ybl"')

# 3. Add Razorpay Dashboard button to Nav header if not present
nav_old = """      <!-- Razorpay Payment History -->
      <button class="btn btn-secondary" onclick="openPaymentHistoryModal()">
        📜 Payment History
      </button>"""

nav_new = """      <!-- Razorpay Payment History -->
      <button class="btn btn-secondary" onclick="openPaymentHistoryModal()">
        📜 Payment History
      </button>

      <!-- Razorpay Official Merchant & Settlement Dashboard -->
      <button class="btn btn-gold" onclick="openRazorpayDashboardModal()" style="display: flex; align-items: center; gap: 6px; box-shadow: 0 0 16px rgba(0, 242, 254, 0.4); font-weight: 800;">
        ⚡ Razorpay Dashboard
      </button>"""

if nav_old in html and "openRazorpayDashboardModal()" not in html:
    html = html.replace(nav_old, nav_new, 1)
    print("Added Razorpay Dashboard button to Nav header!")

# 4. Define the complete modal-razorpay-dashboard markup
modal_rzp_dashboard = """
  <!-- ========================================================================= -->
  <!-- MODAL: Official Razorpay Merchant & Settlement Dashboard (UPI: 6305496537-4@ybl) -->
  <!-- ========================================================================= -->
  <div class="modal-overlay" id="modal-razorpay-dashboard">
    <div class="modal-card" style="max-width: 1040px;">
      <div class="modal-header">
        <div style="display: flex; align-items: center; gap: 12px;">
          <div style="width: 38px; height: 38px; border-radius: 10px; background: linear-gradient(135deg, #00f2fe 0%, #4facfe 100%); display: flex; align-items: center; justify-content: center; font-size: 1.2rem; font-weight: 900; color: #040d1a;">
            ₹
          </div>
          <div>
            <div class="modal-title" style="font-size: 1.1rem;">⚡ Razorpay Official Merchant & Settlement Dashboard</div>
            <div style="font-size: 0.68rem; font-family: var(--font-mono); color: var(--text-muted);">
              Merchant MID: <span style="color: var(--accent-cyan); font-weight: 700;">MID-CHRONOS-84920</span> • Protocol: Razorpay Standard / UPI 2.0 Settlement
            </div>
          </div>
        </div>
        <button class="modal-close" onclick="closeModal('modal-razorpay-dashboard')">&times;</button>
      </div>

      <!-- Linked Merchant Settlement UPI Banner -->
      <div style="background: linear-gradient(90deg, rgba(0, 242, 254, 0.08), rgba(121, 40, 202, 0.08)); border: 1px solid rgba(0, 242, 254, 0.35); border-radius: 12px; padding: 14px 18px; display: flex; flex-wrap: wrap; justify-content: space-between; align-items: center; gap: 12px; margin-bottom: 14px;">
        <div style="display: flex; align-items: center; gap: 14px;">
          <div style="width: 44px; height: 44px; border-radius: 50%; background: rgba(0, 230, 118, 0.15); border: 1px solid rgba(0, 230, 118, 0.4); display: flex; align-items: center; justify-content: center; font-size: 1.3rem;">
            🏦
          </div>
          <div>
            <div style="font-size: 0.65rem; color: var(--text-muted); text-transform: uppercase; font-family: var(--font-mono); letter-spacing: 0.05em;">
              PRIMARY SETTLEMENT BENEFICIARY (DIRECT UPI ROUTE)
            </div>
            <div style="font-size: 1.25rem; font-weight: 800; font-family: var(--font-mono); color: #fff; display: flex; align-items: center; gap: 10px;">
              <span id="rzp-dash-linked-vpa" style="color: var(--accent-cyan);">6305496537-4@ybl</span>
              <button class="btn btn-secondary" style="padding: 3px 8px; font-size: 0.68rem;" onclick="copyMerchantVPA()">📋 Copy VPA</button>
              <span style="font-size: 0.68rem; background: rgba(0, 230, 118, 0.18); color: var(--accent-green); border: 1px solid rgba(0, 230, 118, 0.4); padding: 3px 8px; border-radius: 20px; font-weight: 700;">
                ● ACTIVE & VERIFIED
              </span>
            </div>
            <div style="font-size: 0.72rem; color: var(--text-secondary); margin-top: 3px;">
              PSP Provider: <b>YES Bank (YBL)</b> • Penny Drop Verification: <b style="color: var(--accent-green);">Passed (UTR: PNY-6305496537-YBL)</b> • Schedule: <b>T+0 Instant Auto-Disbursement</b>
            </div>
          </div>
        </div>

        <div style="display: flex; gap: 8px;">
          <button class="btn btn-gold" onclick="triggerInstantSettlement()" style="padding: 10px 16px; font-size: 0.82rem; font-weight: 700; box-shadow: 0 0 15px rgba(255, 209, 102, 0.35);">
            ⚡ Settle Now to 6305496537-4@ybl
          </button>
          <button class="btn btn-secondary" onclick="openRazorpayModal()" style="padding: 10px 14px; font-size: 0.82rem;">
            ➕ Add Funds
          </button>
        </div>
      </div>

      <!-- 4 High-Frequency Payment Analytics Cards -->
      <div style="display: grid; grid-template-columns: repeat(4, 1fr); gap: 12px; margin-bottom: 14px;">
        <div style="background: var(--bg-surface-elevated); border: 1px solid var(--border-subtle); border-radius: 10px; padding: 14px;">
          <div style="font-size: 0.65rem; color: var(--text-muted); text-transform: uppercase; font-family: var(--font-mono);">Total Volume Received</div>
          <div style="font-size: 1.4rem; font-weight: 800; color: var(--accent-cyan); font-family: var(--font-mono); margin-top: 4px;" id="rzp-dash-total-inflow">₹0.00</div>
          <div style="font-size: 0.68rem; color: var(--text-secondary); margin-top: 2px;"><span id="rzp-dash-tx-count" style="color: var(--accent-green); font-weight: 700;">0</span> Verified Orders</div>
        </div>

        <div style="background: var(--bg-surface-elevated); border: 1px solid var(--border-subtle); border-radius: 10px; padding: 14px;">
          <div style="font-size: 0.65rem; color: var(--text-muted); text-transform: uppercase; font-family: var(--font-mono);">Disbursed to 6305496537-4@ybl</div>
          <div style="font-size: 1.4rem; font-weight: 800; color: var(--accent-green); font-family: var(--font-mono); margin-top: 4px;" id="rzp-dash-total-settled">₹1,25,000.00</div>
          <div style="font-size: 0.68rem; color: var(--accent-green); margin-top: 2px;">● 100% Settled via IMPS/UPI</div>
        </div>

        <div style="background: var(--bg-surface-elevated); border: 1px solid var(--border-subtle); border-radius: 10px; padding: 14px;">
          <div style="font-size: 0.65rem; color: var(--text-muted); text-transform: uppercase; font-family: var(--font-mono);">Unsettled Accrual</div>
          <div style="font-size: 1.4rem; font-weight: 800; color: var(--accent-gold); font-family: var(--font-mono); margin-top: 4px;" id="rzp-dash-unsettled">₹0.00</div>
          <div style="font-size: 0.68rem; color: var(--text-muted); margin-top: 2px;">Next batch: <b>Today 18:30 IST</b></div>
        </div>

        <div style="background: var(--bg-surface-elevated); border: 1px solid var(--border-subtle); border-radius: 10px; padding: 14px;">
          <div style="font-size: 0.65rem; color: var(--text-muted); text-transform: uppercase; font-family: var(--font-mono);">Gateway Uptime & SLA</div>
          <div style="font-size: 1.4rem; font-weight: 800; color: #a855f7; font-family: var(--font-mono); margin-top: 4px;">99.98%</div>
          <div style="font-size: 0.68rem; color: var(--accent-cyan); margin-top: 2px;">Avg UPI Speed: <b>850 ms</b></div>
        </div>
      </div>

      <!-- Razorpay Dashboard Navigation Tabs -->
      <div class="pay-tabs" style="margin-bottom: 12px;">
        <div class="pay-tab active" id="rzptab-settlements" onclick="switchRzpDashTab('settlements')">🏦 Settlements to 6305496537-4@ybl</div>
        <div class="pay-tab" id="rzptab-inflows" onclick="switchRzpDashTab('inflows')">💳 Live Inflow Payments</div>
        <div class="pay-tab" id="rzptab-qr" onclick="switchRzpDashTab('qr')">📱 Dynamic Scan & Pay (6305496537-4@ybl)</div>
        <div class="pay-tab" id="rzptab-config" onclick="switchRzpDashTab('config')">⚙️ Gateway Credentials & Webhooks</div>
      </div>

      <!-- TAB 1: SETTLEMENTS LEDGER -->
      <div id="rzpsec-settlements" style="display: flex; flex-direction: column; gap: 8px;">
        <div style="font-size: 0.75rem; color: var(--text-secondary); display: flex; justify-content: space-between; align-items: center;">
          <span>Automated and On-Demand Batch Disbursements to <b>6305496537-4@ybl</b>:</span>
          <span style="color: var(--accent-green); font-family: var(--font-mono); font-size: 0.70rem;">● ZERO SETTLEMENT FAILURES</span>
        </div>
        <div style="max-height: 250px; overflow-y: auto; border: 1px solid var(--border-subtle); border-radius: 8px;">
          <table class="blotter-table" style="width: 100%;">
            <thead>
              <tr>
                <th>Settlement ID</th>
                <th>Destination VPA</th>
                <th>Amount (INR)</th>
                <th>Bank UTR Reference</th>
                <th>Batch Ref</th>
                <th>Status</th>
                <th>Timestamp</th>
              </tr>
            </thead>
            <tbody id="rzp-settlements-tbody">
              <!-- Rendered via JS -->
            </tbody>
          </table>
        </div>
      </div>

      <!-- TAB 2: LIVE INFLOWS -->
      <div id="rzpsec-inflows" style="display: none; flex-direction: column; gap: 8px;">
        <div style="font-size: 0.75rem; color: var(--text-secondary);">
          Customer INR deposits received via Razorpay Checkout, UPI Deep Links and Cards:
        </div>
        <div style="max-height: 250px; overflow-y: auto; border: 1px solid var(--border-subtle); border-radius: 8px;">
          <table class="blotter-table" style="width: 100%;">
            <thead>
              <tr>
                <th>Tx ID</th>
                <th>Order ID</th>
                <th>Payment ID</th>
                <th>Trader</th>
                <th>Amount</th>
                <th>Method</th>
                <th>Status</th>
                <th>Time</th>
              </tr>
            </thead>
            <tbody id="rzp-inflows-tbody">
              <!-- Rendered via JS -->
            </tbody>
          </table>
        </div>
      </div>

      <!-- TAB 3: DYNAMIC SCAN & PAY -->
      <div id="rzpsec-qr" style="display: none; flex-direction: column; gap: 12px;">
        <div style="background: rgba(0, 242, 254, 0.05); border: 1px solid rgba(0, 242, 254, 0.3); border-radius: 12px; padding: 18px; display: flex; align-items: center; justify-content: space-between; gap: 20px;">
          <div style="display: flex; items-center; gap: 20px;">
            <div style="background: #fff; padding: 8px; border-radius: 10px; width: 130px; height: 130px; display: flex; align-items: center; justify-content: center; flex-shrink: 0; box-shadow: 0 0 20px rgba(0, 242, 254, 0.35);">
              <img id="rzp-merchant-qr" src="https://api.qrserver.com/v1/create-qr-code/?size=160x160&data=upi%3A%2F%2Fpay%3Fpa%3D6305496537-4%40ybl%26pn%3DChronosMatch%2BPro%26cu%3DINR%26tn%3DMerchant%2BCollection" alt="Dynamic Merchant QR Code" style="width: 100%; height: 100%; border-radius: 6px;">
            </div>
            <div>
              <div style="font-size: 1.1rem; font-weight: 800; color: var(--accent-cyan);">📱 Direct Merchant UPI Dynamic QR</div>
              <div style="font-size: 0.78rem; color: var(--text-secondary); margin-top: 4px;">
                Linked Settlement VPA: <b style="color: #fff; font-family: var(--font-mono);">6305496537-4@ybl</b>
              </div>
              <div style="font-size: 0.75rem; color: var(--text-muted); margin-top: 2px;">
                Scan with Google Pay, PhonePe, Paytm, BHIM, CRED, Navi or any UPI 2.0 app. Payments deposit directly into your merchant settlement wallet.
              </div>
              <div style="margin-top: 10px; display: flex; gap: 8px;">
                <button class="btn btn-gold" onclick="payViaUPIApp('PhonePe', 'phonepe')" style="padding: 6px 12px; font-size: 0.75rem;">🟣 Open PhonePe</button>
                <button class="btn btn-secondary" onclick="payViaUPIApp('Google Pay', 'gpay')" style="padding: 6px 12px; font-size: 0.75rem;">🟢 Open Google Pay</button>
                <button class="btn btn-secondary" onclick="payViaUPIApp('Paytm', 'paytm')" style="padding: 6px 12px; font-size: 0.75rem;">🔵 Open Paytm</button>
              </div>
            </div>
          </div>
        </div>
      </div>

      <!-- TAB 4: CONFIGURATION -->
      <div id="rzpsec-config" style="display: none; flex-direction: column; gap: 12px;">
        <div style="background: var(--bg-surface-elevated); border: 1px solid var(--border-subtle); border-radius: 10px; padding: 16px; display: flex; flex-direction: column; gap: 10px;">
          <div style="font-size: 0.85rem; font-weight: 700; color: #fff;">Razorpay API & Webhook Credentials</div>
          <div class="form-row">
            <div class="input-group">
              <span class="input-label">Key ID</span>
              <input type="text" class="input-field" id="cfg-rzp-key" value="rzp_test_ChronosMatch123">
            </div>
            <div class="input-group">
              <span class="input-label">Key Secret</span>
              <input type="password" class="input-field" id="cfg-rzp-sec" value="secret_ChronosMatchLiveHFT">
            </div>
          </div>
          <div class="input-group">
            <span class="input-label">Linked Settlement Beneficiary VPA</span>
            <input type="text" class="input-field" id="cfg-rzp-vpa" value="6305496537-4@ybl" readonly style="color: var(--accent-cyan); font-weight: 700;">
          </div>
          <button class="btn btn-primary" onclick="saveRazorpayConfig()" style="justify-content: center; padding: 10px;">
            💾 Update Credentials
          </button>
        </div>
      </div>

    </div>
  </div>
"""

# Place modal-razorpay-dashboard right after modal-payment-history
modal_hist_marker = '  <!-- ========================================================================= -->\n  <!-- MODAL: Instant Trading Cash Withdrawal / Payout'
if modal_hist_marker in html and 'id="modal-razorpay-dashboard"' not in html:
    html = html.replace(modal_hist_marker, modal_rzp_dashboard + "\n\n  " + modal_hist_marker, 1)
    print("Inserted modal-razorpay-dashboard successfully!")

# 5. Add JavaScript functions for Razorpay Dashboard
js_funcs = """
    // Razorpay Official Merchant & Settlement Dashboard Controllers
    async function openRazorpayDashboardModal() {
      openModal('modal-razorpay-dashboard');
      await refreshRazorpayDashboard();
    }

    async function refreshRazorpayDashboard() {
      try {
        const resp = await fetch('/api/payments/razorpay/dashboard');
        const data = await resp.json();

        // Update top metrics
        document.getElementById('rzp-dash-total-inflow').textContent = `₹${data.total_inflow_inr.toLocaleString('en-IN', {minimumFractionDigits: 2})}`;
        document.getElementById('rzp-dash-total-settled').textContent = `₹${data.settled_inflow_inr.toLocaleString('en-IN', {minimumFractionDigits: 2})}`;
        document.getElementById('rzp-dash-unsettled').textContent = `₹${data.unsettled_balance_inr.toLocaleString('en-IN', {minimumFractionDigits: 2})}`;
        document.getElementById('rzp-dash-tx-count').textContent = data.total_transactions;
        document.getElementById('rzp-dash-linked-vpa').textContent = data.linked_vpa;

        // Render Settlements table
        const setTbody = document.getElementById('rzp-settlements-tbody');
        if (setTbody) {
          setTbody.innerHTML = '';
          if (!data.recent_settlements || data.recent_settlements.length === 0) {
            setTbody.innerHTML = '<tr><td colspan="7" style="text-align: center; color: var(--text-muted); padding: 16px;">No settlements yet. Click "Settle Now" to transfer funds to 6305496537-4@ybl.</td></tr>';
          } else {
            data.recent_settlements.forEach(s => {
              const tr = document.createElement('tr');
              tr.innerHTML = `
                <td style="color: var(--accent-cyan); font-family: var(--font-mono); font-weight: 700;">${s.settlement_id}</td>
                <td style="color: #fff; font-family: var(--font-mono);">${s.vpa}</td>
                <td style="color: var(--accent-green); font-weight: 800; font-family: var(--font-mono);">₹${Number(s.amount).toLocaleString('en-IN', {minimumFractionDigits: 2})}</td>
                <td style="color: var(--accent-gold); font-family: var(--font-mono); font-size: 0.72rem;">${s.utr}</td>
                <td style="color: var(--text-muted); font-size: 0.70rem;">${s.batch_ref}</td>
                <td><span style="background: rgba(0,230,118,0.15); color: var(--accent-green); border: 1px solid rgba(0,230,118,0.35); padding: 2px 6px; border-radius: 4px; font-size: 0.65rem; font-weight: 700;">${s.status}</span></td>
                <td style="color: var(--text-muted); font-size: 0.68rem;">${s.created_at.replace('T', ' ').substring(0, 19)}</td>
              `;
              setTbody.appendChild(tr);
            });
          }
        }

        // Render Inflows table
        const inTbody = document.getElementById('rzp-inflows-tbody');
        if (inTbody) {
          inTbody.innerHTML = '';
          if (!data.recent_transactions || data.recent_transactions.length === 0) {
            inTbody.innerHTML = '<tr><td colspan="8" style="text-align: center; color: var(--text-muted); padding: 16px;">No payments recorded yet.</td></tr>';
          } else {
            data.recent_transactions.forEach(t => {
              const tr = document.createElement('tr');
              const isSucc = (t.status || '').toUpperCase() === 'SUCCESS';
              tr.innerHTML = `
                <td style="color: var(--accent-cyan); font-family: var(--font-mono);">${t.tx_id || 'TX-100'}</td>
                <td style="font-family: var(--font-mono); font-size: 0.70rem;">${t.order_id}</td>
                <td style="color: var(--accent-gold); font-family: var(--font-mono); font-size: 0.70rem;">${t.payment_id || '—'}</td>
                <td style="font-weight: 600;">${t.customer_name || 'Trader'}</td>
                <td style="font-weight: 800; color: var(--accent-green); font-family: var(--font-mono);">₹${Number(t.amount).toLocaleString('en-IN', {minimumFractionDigits: 2})}</td>
                <td style="font-size: 0.72rem;">${t.method}</td>
                <td><span style="background: ${isSucc ? 'rgba(0,230,118,0.15)' : 'rgba(255,183,3,0.15)'}; color: ${isSucc ? 'var(--accent-green)' : 'var(--accent-yellow)'}; padding: 2px 6px; border-radius: 4px; font-size: 0.65rem;">${t.status}</span></td>
                <td style="color: var(--text-muted); font-size: 0.65rem;">${t.created_at ? t.created_at.split('T')[1].substring(0, 8) : '—'}</td>
              `;
              inTbody.appendChild(tr);
            });
          }
        }
      } catch (e) {
        console.error("Error refreshing Razorpay dashboard:", e);
      }
    }

    async function triggerInstantSettlement() {
      try {
        const resp = await fetch('/api/payments/razorpay/settle', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ vpa: "6305496537-4@ybl" })
        });
        const res = await resp.json();
        if (res.success) {
          playPaymentChime();
          showToast("SETTLEMENT COMPLETED", res.message, "success");
          await refreshRazorpayDashboard();
        } else {
          showToast("Settlement Failed", res.detail || "Could not process settlement", "error");
        }
      } catch (err) {
        showToast("Settlement Error", err.message, "error");
      }
    }

    function switchRzpDashTab(tab) {
      ['settlements', 'inflows', 'qr', 'config'].forEach(t => {
        const btn = document.getElementById(`rzptab-${t}`);
        const sec = document.getElementById(`rzpsec-${t}`);
        if (btn) btn.classList.toggle('active', t === tab);
        if (sec) sec.style.display = (t === tab ? 'flex' : 'none');
      });
    }

    function copyMerchantVPA() {
      navigator.clipboard.writeText("6305496537-4@ybl");
      showToast("COPIED TO CLIPBOARD", "Linked UPI ID '6305496537-4@ybl' copied successfully!", "success");
    }

    async function saveRazorpayConfig() {
      const kId = document.getElementById('cfg-rzp-key').value.trim();
      const kSec = document.getElementById('cfg-rzp-sec').value.trim();
      await fetch('/api/payments/razorpay/keys', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ key_id: kId, key_secret: kSec })
      });
      showToast("CREDENTIALS UPDATED", "Razorpay credentials updated successfully!", "success");
    }
"""

if "function openRazorpayDashboardModal()" not in html:
    html = html.replace("    // Razorpay Integration & Full Payment Methods", js_funcs + "\n\n    // Razorpay Integration & Full Payment Methods", 1)
    print("Appended Razorpay Dashboard JavaScript handlers!")

with open(r"C:\Users\GOPI V\Desktop\hft_engine\ui\web_dashboard.html", "w", encoding="utf-8") as f:
    f.write(html)

print("Updated ui/web_dashboard.html with complete Razorpay dashboard & 6305496537-4@ybl!")
