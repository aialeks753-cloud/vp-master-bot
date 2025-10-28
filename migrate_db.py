import sqlite3, os

DB_PATH = os.path.join(os.path.dirname(__file__), "vp_masters.sqlite")
conn = sqlite3.connect(DB_PATH)
cur = conn.cursor()

def ensure_table(name, ddl):
    cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (name,))
    if not cur.fetchone():
        print(f"[CREATE] {name}")
        cur.execute(ddl)

def ensure_column(table, name, ddl):
    # если таблицы нет — создадим пустую "каркасом" и только потом добавим колонку
    cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (table,))
    if not cur.fetchone():
        return  # колонку добавим после ensure_table() ниже
    cur.execute(f"PRAGMA table_info({table})")
    cols = {row[1] for row in cur.fetchall()}
    if name not in cols:
        print(f"[ADD] {table}.{name}")
        cur.execute(f"ALTER TABLE {table} ADD COLUMN {name} {ddl}")

# --- Базовые таблицы (каркасы) ---
ensure_table("masters", """
CREATE TABLE masters(
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  fio TEXT,
  contact TEXT,                 -- uid TG мастера
  phone TEXT,
  inn TEXT,
  exp_bucket TEXT,
  exp_text TEXT,
  portfolio TEXT,
  level TEXT DEFAULT 'Кандидат',
  verified INTEGER DEFAULT 0,
  has_npd_ip INTEGER DEFAULT 0,
  passport_info TEXT,
  passport_scan_file_id TEXT,
  face_photo_file_id TEXT,
  npd_ip_doc_file_id TEXT,
  categories_auto TEXT,
  orders_completed INTEGER DEFAULT 0,
  skill_tier TEXT DEFAULT 'Новичок',
  rating REAL DEFAULT 5.0,
  is_active INTEGER DEFAULT 1,
  sub_until DATETIME,
  free_orders_left INTEGER DEFAULT 3,
  priority_until DATETIME,
  pin_until DATETIME,
  created_at DATETIME DEFAULT CURRENT_TIMESTAMP
)""")

ensure_table("requests", """
CREATE TABLE requests(
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  name TEXT, contact TEXT, category TEXT, district TEXT,
  description TEXT, when_text TEXT,
  status TEXT DEFAULT 'new',
  master_id INTEGER,
  auto_category TEXT,
  score REAL DEFAULT 0,
  created_at DATETIME DEFAULT CURRENT_TIMESTAMP
)""")

ensure_table("offers", """
CREATE TABLE offers(
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  request_id INTEGER,
  master_id INTEGER,
  status TEXT,
  created_at DATETIME DEFAULT CURRENT_TIMESTAMP
)""")

ensure_table("complaints", """
CREATE TABLE complaints(
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  who TEXT,
  order_id TEXT,
  master_id TEXT,
  text TEXT,
  created_at DATETIME DEFAULT CURRENT_TIMESTAMP
)""")

# --- Мягкие добавления колонок (на случай старых БД) ---
for col, ddl in [
    ("phone", "TEXT"),
    ("inn", "TEXT"),
    ("exp_bucket", "TEXT"),
    ("exp_text", "TEXT"),
    ("portfolio", "TEXT"),
    ("level", "TEXT DEFAULT 'Кандидат'"),
    ("verified", "INTEGER DEFAULT 0"),
    ("has_npd_ip", "INTEGER DEFAULT 0"),
    ("passport_info", "TEXT"),
    ("passport_scan_file_id", "TEXT"),
    ("face_photo_file_id", "TEXT"),
    ("npd_ip_doc_file_id", "TEXT"),
    ("categories_auto", "TEXT"),
    ("orders_completed", "INTEGER DEFAULT 0"),
    ("skill_tier", "TEXT DEFAULT 'Новичок'"),
    ("rating", "REAL DEFAULT 5.0"),
    ("is_active", "INTEGER DEFAULT 1"),
    ("sub_until", "DATETIME"),
    ("free_orders_left", "INTEGER DEFAULT 3"),
    ("priority_until", "DATETIME"),
    ("pin_until", "DATETIME"),
    ("contact", "TEXT"),   # на случай, если раньше не было
    ("fio", "TEXT"),       # и это тоже
    ("references", "TEXT") # контакты клиентов для рекомендаций
]:
    ensure_column("masters", col, ddl)

for col, ddl in [
    ("auto_category", "TEXT"),
    ("score", "REAL DEFAULT 0"),
    ("status", "TEXT DEFAULT 'new'"),
    ("master_id", "INTEGER")
]:
    ensure_column("requests", col, ddl)

conn.commit()
conn.close()
print("✅ Migration finished.")