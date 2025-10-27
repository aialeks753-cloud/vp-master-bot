import sqlite3

conn = sqlite3.connect('vp_masters.sqlite')
cur = conn.cursor()

# Удаляем тестовые данные
cur.execute("DELETE FROM requests")
cur.execute("DELETE FROM offers")
cur.execute("DELETE FROM reviews")

# Сбрасываем счётчик бесплатных заказов мастеру
cur.execute("UPDATE masters SET free_orders_left = 3, orders_completed = 0")

conn.commit()
conn.close()
print("✅ Тестовые данные удалены")