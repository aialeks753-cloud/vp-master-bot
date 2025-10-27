import sqlite3

conn = sqlite3.connect('vp_masters.sqlite')
conn.row_factory = sqlite3.Row
cur = conn.cursor()

print("⭐ ОТЗЫВЫ:")
reviews = cur.execute("""
    SELECT r.id, r.request_id, r.master_id, r.rating, r.comment, r.created_at
    FROM reviews r
    ORDER BY r.created_at DESC
""").fetchall()

if reviews:
    for rev in reviews:
        print(f"\n  ID: {rev['id']}")
        print(f"  Заявка: #{rev['request_id']}")
        print(f"  Мастер: #{rev['master_id']}")
        print(f"  Оценка: {rev['rating']} ⭐")
        print(f"  Комментарий: {rev['comment'] or 'нет'}")
        print(f"  Дата: {rev['created_at']}")
        print("-" * 40)
else:
    print("  (Пока нет отзывов)")

# Проверяем статистику мастера
print("\n📊 СТАТИСТИКА МАСТЕРОВ:")
masters = cur.execute("""
    SELECT id, fio, avg_rating, reviews_count, orders_completed
    FROM masters
""").fetchall()

for m in masters:
    print(f"\n  #{m['id']} {m['fio']}")
    print(f"  Средний рейтинг: {m['avg_rating']} ⭐")
    print(f"  Отзывов: {m['reviews_count']}")
    print(f"  Выполнено заказов: {m['orders_completed']}")
    print("-" * 40)

conn.close()