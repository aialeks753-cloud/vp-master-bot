import sqlite3

conn = sqlite3.connect('vp_masters.sqlite')
cur = conn.cursor()

# Удаляем заявку #8 и все связанные офферы
cur.execute("DELETE FROM requests WHERE id = 8")
cur.execute("DELETE FROM offers WHERE request_id = 8")

conn.commit()
conn.close()

print("✅ Старая заявка #8 удалена")
print("📝 Теперь создавай новую заявку!")