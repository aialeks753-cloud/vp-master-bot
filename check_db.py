import sqlite3

conn = sqlite3.connect('vp_masters.sqlite')
conn.row_factory = sqlite3.Row
cur = conn.cursor()

print("=" * 60)
print("ПРОВЕРКА БАЗЫ ДАННЫХ")
print("=" * 60)

# Проверяем заявки
print("\n📝 ЗАЯВКИ:")
requests = cur.execute("SELECT id, name, contact, client_user_id, status FROM requests").fetchall()
for r in requests:
    print(f"  ID: {r['id']}")
    print(f"  Имя: {r['name']}")
    print(f"  Контакт: {r['contact']}")
    print(f"  Client User ID: {r['client_user_id']} ← ВАЖНО!")
    print(f"  Статус: {r['status']}")
    print("-" * 40)

# Проверяем мастеров
print("\n👨‍🔧 МАСТЕРА:")
masters = cur.execute("SELECT id, fio, contact, phone FROM masters").fetchall()
for m in masters:
    print(f"  ID: {m['id']}")
    print(f"  ФИО: {m['fio']}")
    print(f"  Контакт (user_id): {m['contact']}")
    print(f"  Телефон: {m['phone']}")
    print("-" * 40)

# Проверяем офферы
print("\n🤝 ОФФЕРЫ:")
offers = cur.execute("SELECT id, request_id, master_id, status FROM offers").fetchall()
for o in offers:
    print(f"  ID: {o['id']}")
    print(f"  Request ID: {o['request_id']}")
    print(f"  Master ID: {o['master_id']}")
    print(f"  Статус: {o['status']}")
    print("-" * 40)

conn.close()

print("\n✅ Проверка завершена")
print("\nВАЖНО: В requests должно быть заполнено client_user_id!")