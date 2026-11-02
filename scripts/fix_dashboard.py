with open(r"C:\Users\GOPI V\Desktop\hft_engine\ui\web_dashboard.html", "r", encoding="utf-8") as f:
    content = f.read()

# Check how many times modal-payment-history occurs
modal_tag = 'id="modal-payment-history"'
count = content.count(modal_tag)
print(f"Occurrences of modal-payment-history: {count}")

# The modal block starts with:
block_start = '  <!-- ========================================================================= -->\n  <!-- MODAL: Official Razorpay Payment & Ledger History                         -->'
block_end = '    </div>\n  </div>'

# Find the full block
idx = content.find(block_start)
if idx != -1:
    end_idx = content.find(block_end, idx) + len(block_end)
    block_text = content[idx:end_idx]
    
    # Remove all occurrences of block_text
    clean_content = content.replace(block_text, "")
    
    # Now insert block_text exactly once, right after modal-razorpay closes
    rzp_close = '<!-- ========================================================================= -->\n  <!-- MODAL: Instant Trading Cash Withdrawal'
    if rzp_close in clean_content:
        clean_content = clean_content.replace(rzp_close, block_text + "\n\n  " + rzp_close)
        with open(r"C:\Users\GOPI V\Desktop\hft_engine\ui\web_dashboard.html", "w", encoding="utf-8") as f:
            f.write(clean_content)
        print("Successfully cleaned up and placed modal-payment-history once!")
    else:
        print("Could not find rzp_close marker")
else:
    print("Could not find block_start")
