import sqlite3
from datetime import datetime

conn = sqlite3.connect('vp_masters.sqlite')
conn.row_factory = sqlite3.Row
cur = conn.cursor()

print(f"📅 Отчёт за {datetime.now().strftime('%d.%m.%Y')}\n")

# Статистика за сегодня
today_requests = cur.execute("""
    SELECT COUNT(*) FROM requests 
    WHERE DATE(created_at) = DATE('now')
""").fetchone()[0]

today_completed = cur.execute("""
    SELECT COUNT(*) FROM requests 
    WHERE DATE(completed_at) = DATE('now')
""").fetchone()[0]

today_reviews = cur.execute("""
    SELECT COUNT(*) FROM reviews 
    WHERE DATE(created_at) = DATE('now')
""").fetchone()[0]

print(f"📝 Новых заявок сегодня: {today_requests}")
print(f"✅ Завершено сегодня: {today_completed}")
print(f"⭐ Отзывов сегодня: {today_reviews}")

# Топ мастеров
print(f"\n🏆 ТОП-3 МАСТЕРА ПО РЕЙТИНГУ:")
top_masters = cur.execute("""
    SELECT fio, avg_rating, reviews_count, orders_completed
    FROM masters
    WHERE reviews_count > 0
    ORDER BY avg_rating DESC, reviews_count DESC
    LIMIT 3
""").fetchall()

for i, m in enumerate(top_masters, 1):
    print(f"{i}. {m['fio']}: {m['avg_rating']}⭐ ({m['reviews_count']} отзывов, {m['orders_completed']} заказов)")

conn.close()