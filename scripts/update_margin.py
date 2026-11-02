import sqlite3
import datetime
from pathlib import Path

db_path = Path("logs") / "margin_account.db"
conn = sqlite3.connect(str(db_path))
now = datetime.datetime.now(datetime.timezone.utc).isoformat()
conn.execute("UPDATE account_balance SET cash_balance = 2500000.0, used_margin = 0.0, updated_at = ? WHERE account_id = 'primary'", (now,))
conn.commit()
cur = conn.cursor()
cur.execute("SELECT * FROM account_balance")
print("ACCOUNT_BALANCE:", cur.fetchall())
conn.close()
