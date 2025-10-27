import sqlite3

conn = sqlite3.connect('vp_masters.sqlite')
cur = conn.cursor()

print("📋 Колонки в таблице requests:")
columns = cur.execute("PRAGMA table_info(requests)").fetchall()
for col in columns:
    print(f"  - {col[1]} ({col[2]})")

conn.close()

print("\n✅ Должна быть колонка 'client_user_id'")