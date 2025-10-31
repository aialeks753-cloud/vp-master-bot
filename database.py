import sqlite3
import logging
import re
from datetime import datetime

class DatabaseManager:
    def __init__(self, db_path):
        self.db_path = db_path
        self.conn = None
        self.connect()
    
    def connect(self):
        """Установка соединения с БД"""
        try:
            self.conn = sqlite3.connect(self.db_path, check_same_thread=False)
            self.conn.row_factory = sqlite3.Row
            logging.info("[DB] Connection established")
        except Exception as e:
            logging.error(f"[DB] Connection failed: {e}")
    
    def execute(self, query: str, params: tuple = ()):
        """Безопасное выполнение запроса"""
        try:
            cur = self.conn.cursor()
            cur.execute(query, params)
            self.conn.commit()
            return cur
        except Exception as e:
            logging.error(f"[DB] Execute error: {e} - Query: {query}")
            return None
    
    def fetch_one(self, query: str, params: tuple = ()):
        """Получить одну запись"""
        cur = self.execute(query, params)
        return cur.fetchone() if cur else None
    
    def fetch_all(self, query: str, params: tuple = ()):
        """Получить все записи"""
        cur = self.execute(query, params)
        return cur.fetchall() if cur else []
    
    def commit(self):
        """Коммит текущей транзакции"""
        if self.conn:
            self.conn.commit()

    def close(self):
        """Закрытие соединения"""
        if self.conn:
            self.conn.close()
            logging.info("[DB] Connection closed")

def ensure_column(db, table, name, ddl):
    """Безопасное добавление колонки в таблицу"""
    # Валидация имён таблицы и колонки
    if not re.match(r'^[a-zA-Z_][a-zA-Z0-9_]*$', table) or not re.match(r'^[a-zA-Z_][a-zA-Z0-9_]*$', name):
        logging.error(f"[DB] Invalid table or column name: {table}.{name}")
        return
    
    # Белый список допустимых типов данных
    ALLOWED_TYPES = [
        'TEXT', 'INTEGER', 'REAL', 'DATETIME', 'BLOB',
        'INTEGER DEFAULT 0', 'INTEGER DEFAULT 1', 
        'REAL DEFAULT 0', 'REAL DEFAULT 5.0',
        'TEXT DEFAULT "Кандидат"', "TEXT DEFAULT 'Кандидат'",
        'TEXT DEFAULT "Новичок"', "TEXT DEFAULT 'Новичок'",
        'DATETIME DEFAULT CURRENT_TIMESTAMP'
    ]
    
    # Проверяем что тип разрешён
    type_valid = False
    for allowed in ALLOWED_TYPES:
        if ddl.startswith(allowed) or ddl == allowed:
            type_valid = True
            break
    
    if not type_valid:
        logging.error(f"[DB] Invalid column type: {ddl}")
        return
    
    # Проверяем существование колонки
    try:
        existing_columns = db.fetch_all(f"PRAGMA table_info({table})")
    except Exception as e:
        logging.error(f"[DB] PRAGMA error for {table}: {e}")
        return
        
    if not existing_columns:
        return
    
    cols = {row['name'] for row in existing_columns}
    if name not in cols:
        try:
            safe_query = f"ALTER TABLE {table} ADD COLUMN {name} {ddl}"
            db.execute(safe_query)
            logging.info(f"[DB] ADD {table}.{name}")
        except Exception as e:
            logging.error(f"[DB] ALTER fail {table}.{name}: {e}")

def init_database(db):
    """Инициализация всех таблиц и колонок"""
    # Базовые таблицы
    db.execute("""
    CREATE TABLE IF NOT EXISTS requests(
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      name TEXT, contact TEXT, category TEXT, district TEXT,
      description TEXT, when_text TEXT,
      status TEXT DEFAULT 'new',
      master_id INTEGER,
      auto_category TEXT,
      score REAL DEFAULT 0,
      created_at DATETIME DEFAULT CURRENT_TIMESTAMP
    )""")
    
    db.execute("""
    CREATE TABLE IF NOT EXISTS masters(
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      fio TEXT, contact TEXT, inn TEXT,
      experience TEXT, portfolio TEXT,
      level TEXT DEFAULT 'Кандидат',
      rating REAL DEFAULT 5.0,
      is_active INTEGER DEFAULT 1,
      sub_until DATETIME,
      free_orders_left INTEGER DEFAULT 3,
      priority_until DATETIME,
      pin_until DATETIME,
      has_npd_ip INTEGER DEFAULT 0,
      verified INTEGER DEFAULT 0,
      passport_info TEXT,
      photo_file_id TEXT,
      categories_auto TEXT,
      created_at DATETIME DEFAULT CURRENT_TIMESTAMP
    )""")
    
    db.execute("""
    CREATE TABLE IF NOT EXISTS complaints(
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      who TEXT, order_id TEXT, master_id TEXT, text TEXT,
      created_at DATETIME DEFAULT CURRENT_TIMESTAMP
    )""")
    
    db.execute("""
    CREATE TABLE IF NOT EXISTS offers(
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      request_id INTEGER,
      master_id INTEGER,
      status TEXT,
      created_at DATETIME DEFAULT CURRENT_TIMESTAMP
    )""")
    
    db.execute("""
    CREATE TABLE IF NOT EXISTS reviews(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        request_id INTEGER NOT NULL,
        master_id INTEGER NOT NULL,
        client_id TEXT NOT NULL,
        rating INTEGER CHECK(rating >= 1 AND rating <= 5),
        comment TEXT,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (request_id) REFERENCES requests(id),
        FOREIGN KEY (master_id) REFERENCES masters(id)
    )""")

    # Индексы
    db.execute("CREATE INDEX IF NOT EXISTS idx_reviews_master_id ON reviews(master_id)")
    db.execute("CREATE INDEX IF NOT EXISTS idx_reviews_request_id ON reviews(request_id)")
    db.execute("CREATE INDEX IF NOT EXISTS idx_masters_contact ON masters(contact)")
    db.execute("CREATE INDEX IF NOT EXISTS idx_requests_status ON requests(status)")
    db.execute("CREATE INDEX IF NOT EXISTS idx_offers_request_id ON offers(request_id)")
    db.execute("CREATE INDEX IF NOT EXISTS idx_offers_master_id ON offers(master_id)")
    db.execute("CREATE INDEX IF NOT EXISTS idx_masters_active ON masters(is_active, level)")
    
    # Мягкие добавления недостающих колонок
    ensure_column(db, "requests", "completed_at", "DATETIME")
    ensure_column(db, "requests", "client_rating", "INTEGER")
    ensure_column(db, "requests", "client_comment", "TEXT")
    ensure_column(db, "requests", "review_requested", "INTEGER DEFAULT 0")
    ensure_column(db, "requests", "auto_category", "TEXT")
    ensure_column(db, "requests", "score", "REAL DEFAULT 0")
    ensure_column(db, "requests", "client_user_id", "TEXT")
    
    ensure_column(db, "masters", "avg_rating", "REAL DEFAULT 5.0")
    ensure_column(db, "masters", "reviews_count", "INTEGER DEFAULT 0")
    ensure_column(db, "masters", "free_orders_left", "INTEGER DEFAULT 3")
    ensure_column(db, "masters", "priority_until", "DATETIME")
    ensure_column(db, "masters", "pin_until", "DATETIME")
    ensure_column(db, "masters", "has_npd_ip", "INTEGER DEFAULT 0")
    ensure_column(db, "masters", "verified", "INTEGER DEFAULT 0")
    ensure_column(db, "masters", "photo_file_id", "TEXT")
    ensure_column(db, "masters", "categories_auto", "TEXT")
    ensure_column(db, "masters", "phone", "TEXT")
    ensure_column(db, "masters", "exp_bucket", "TEXT")
    ensure_column(db, "masters", "exp_text", "TEXT")
    ensure_column(db, "masters", "passport_scan_file_id", "TEXT")
    ensure_column(db, "masters", "face_photo_file_id", "TEXT")
    ensure_column(db, "masters", "npd_ip_doc_file_id", "TEXT")
    ensure_column(db, "masters", "orders_completed", "INTEGER DEFAULT 0")
    ensure_column(db, "masters", "skill_tier", "TEXT DEFAULT 'Новичок'")
    
    logging.info("[DB] Database initialized")