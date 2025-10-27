import sqlite3

conn = sqlite3.connect('vp_masters.sqlite')
cur = conn.cursor()

print("🗑 Удаление всех данных...")

# Удаляем все записи из всех таблиц
cur.execute("DELETE FROM requests")
cur.execute("DELETE FROM offers")
cur.execute("DELETE FROM reviews")
cur.execute("DELETE FROM complaints")
cur.execute("DELETE FROM masters")

conn.commit()

# Проверяем что удалилось
print("\n✅ Удалено:")
print(f"  Заявок: {cur.execute('SELECT COUNT(*) FROM requests').fetchone()[0]}")
print(f"  Мастеров: {cur.execute('SELECT COUNT(*) FROM masters').fetchone()[0]}")
print(f"  Офферов: {cur.execute('SELECT COUNT(*) FROM offers').fetchone()[0]}")
print(f"  Отзывов: {cur.execute('SELECT COUNT(*) FROM reviews').fetchone()[0]}")
print(f"  Жалоб: {cur.execute('SELECT COUNT(*) FROM complaints').fetchone()[0]}")

conn.close()

print("\n🎉 База данных полностью очищена!")
print("📝 Можно начинать тестирование с чистого листа")