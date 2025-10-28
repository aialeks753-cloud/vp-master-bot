import asyncio, os, sqlite3, json, re
from datetime import datetime, timedelta
from collections import defaultdict

from aiogram import Bot, Dispatcher, F
from aiogram.client.default import DefaultBotProperties
from aiogram.filters import CommandStart, Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import (
    CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton,
    Message, LabeledPrice, PreCheckoutQuery,
    ReplyKeyboardMarkup, KeyboardButton, ReplyKeyboardRemove
)
from dotenv import load_dotenv
import logging

from database import DatabaseManager, init_database
from rate_limiter import RateLimiter

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)

# ----------------- ENV -----------------
load_dotenv()
BOT_TOKEN = os.getenv("BOT_TOKEN")
ADMIN_CHAT_ID = int(os.getenv("ADMIN_CHAT_ID", "0"))
PAY_PROVIDER_TOKEN = os.getenv("PAY_PROVIDER_TOKEN", "")  # когда подключишь провайдера

bot = Bot(BOT_TOKEN, default=DefaultBotProperties(parse_mode="HTML"))
dp = Dispatcher(storage=MemoryStorage())

# ----------------- DB ------------------
# Инициализируем менеджер БД
DB_PATH = "vp_masters.sqlite"
db = DatabaseManager(DB_PATH)

# Создаем глобальный экземпляр лимитера
rate_limiter = RateLimiter()

# Инициализируем структуру БД
init_database(db)

# ----------------- PRICING -------------
SUB_PRICE_RUB = 99000         # 990 ₽ (в копейках)
PRIORITY_PRICE_RUB = 49000    # 490 ₽/мес
PIN_PRICE_RUB = 19000         # 190 ₽/нед

FREE_ORDERS_START = 3
SUB_DURATION_DAYS = 30
PRIORITY_DURATION_DAYS = 30
PIN_DURATION_DAYS = 7

# ----------------- CATEGORIES ----------
CATS = [
    ("🛠 Ремонт", "remont"),
    ("🧹 Уборка", "uborka"),
    ("🚚 Переезд", "pereezd"),
    ("💅 Красота", "krasota"),
    ("👶 Персонал", "person"),
]

def categories_kb():
    rows = [[InlineKeyboardButton(text=t, callback_data=f"cat:{c}")] for t, c in CATS]
    rows.append([InlineKeyboardButton(text="🏠 Меню", callback_data="go:menu")])
    return InlineKeyboardMarkup(inline_keyboard=rows)

def share_phone_kb():
    return ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text="📱 Отправить номер", request_contact=True)]],
        resize_keyboard=True, one_time_keyboard=True
    )

def cancel_text_kb():
    return ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text="❌ Отмена")]],
        resize_keyboard=True,
        one_time_keyboard=False
    )

# ----------------- STATES --------------
class Req(StatesGroup):
    name = State()
    contact = State()
    category = State()
    district = State()
    desc = State()
    when = State()
    preview = State()

class MasterForm(StatesGroup):
    fio = State()
    phone = State()
    categories = State()
    exp_bucket = State()
    exp_text = State()
    portfolio = State()
    references = State()
    verify_offer = State()
    consent = State()
    passport_info = State()
    passport_scan = State()
    face_photo = State()
    npd_offer = State()
    inn_cert = State()
    npd_doc = State()
    finalize = State()

class Complaint(StatesGroup):
    who = State()
    order_id = State()
    master_id = State()
    text = State()

# ----------------- HELPERS -------------
def is_active(until_str: str | None) -> bool:
    if not until_str: return False
    try:
        return datetime.utcnow() < datetime.strptime(until_str, "%Y-%m-%d %H:%M:%S")
    except:
        return False

def is_admin(user_id: int) -> bool:
    """Проверка, является ли пользователь администратором"""
    if not ADMIN_CHAT_ID or ADMIN_CHAT_ID == 0:
        return False
    return str(user_id) == str(ADMIN_CHAT_ID)

async def notify_admin(text: str):
    if not ADMIN_CHAT_ID: return
    try:
        await bot.send_message(ADMIN_CHAT_ID, text)
    except Exception as e:
        logging.error(f"[ADMIN_NOTIFY_ERROR] {e}")

async def safe_cleanup_documents():
    """
    Безопасное удаление документов с уведомлением админа и детальным логированием
    """
    try:
        logging.info("[CLEANUP] Starting document cleanup...")
        
        # Находим мастеров, у которых нужно удалить документы (старше 72 часов)
        masters_to_clean = db.fetch_all("""
            SELECT id, fio, level, contact, phone,
                   passport_scan_file_id, face_photo_file_id, npd_ip_doc_file_id,
                   datetime(created_at) as created_time
            FROM masters 
            WHERE created_at < datetime('now', '-72 hours')
              AND (passport_scan_file_id IS NOT NULL 
                   OR face_photo_file_id IS NOT NULL 
                   OR npd_ip_doc_file_id IS NOT NULL)
        """)
        
        if not masters_to_clean:
            logging.info("[CLEANUP] No documents to clean")
            return
        
        cleaned_count = 0
        cleanup_details = []
        
        for master in masters_to_clean:
            try:
                master_id = master['id']
                master_fio = master['fio'] or 'Неизвестно'
                master_level = master['level'] or 'Кандидат'
                
                # Собираем информацию о том, какие файлы будут удалены
                files_to_remove = []
                if master['passport_scan_file_id']:
                    files_to_remove.append("паспорт")
                if master['face_photo_file_id']:
                    files_to_remove.append("фото лица")
                if master['npd_ip_doc_file_id']:
                    files_to_remove.append("документ НПД/ИП")
                
                # Удаляем файлы из БД
                db.execute("""
                    UPDATE masters 
                    SET passport_scan_file_id = NULL,
                        face_photo_file_id = NULL,
                        npd_ip_doc_file_id = NULL
                    WHERE id = ?
                """, (master_id,))
                
                cleaned_count += 1
                
                # Добавляем в детали очистки
                cleanup_details.append(
                    f"#{master_id} {master_fio} ({master_level}): {', '.join(files_to_remove)}"
                )
                
                logging.info(f"[DOC_CLEANED] Master #{master_id} - removed: {', '.join(files_to_remove)}")
                
            except Exception as e:
                logging.error(f"[DOC_CLEAN_ERROR] Master #{master.get('id', 'unknown')}: {e}")
                cleanup_details.append(f"#{master.get('id', 'unknown')} - ОШИБКА: {e}")
        
        # Формируем отчет для админа
        if cleaned_count > 0 and ADMIN_CHAT_ID:
            report_lines = [
                "🧹 <b>Автоочистка документов завершена</b>",
                f"📊 Обработано мастеров: {cleaned_count}",
                "",
                "<b>Детали очистки:</b>"
            ]
            report_lines.extend(cleanup_details[:10])  # Первые 10 записей
            
            if len(cleanup_details) > 10:
                report_lines.append(f"... и еще {len(cleanup_details) - 10} мастеров")
            
            report_lines.extend([
                "",
                f"⏰ Время выполнения: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
                "✅ Все документы старше 72 часов удалены"
            ])
            
            # Разбиваем сообщение если слишком длинное
            report_text = "\n".join(report_lines)
            if len(report_text) > 4000:
                report_text = "\n".join(report_lines[:8] + ["...", "💡 Сообщение сокращено из-за длины"])
            
            try:
                await bot.send_message(ADMIN_CHAT_ID, report_text)
            except Exception as e:
                logging.error(f"[CLEANUP_REPORT_ERROR] {e}")
        
        logging.info(f"[CLEANUP] Documents cleaned for {cleaned_count} masters")
        
    except Exception as e:
        logging.error(f"[CLEANUP_ERROR] Global error: {e}")
        
        # Уведомляем админа об ошибке
        if ADMIN_CHAT_ID:
            try:
                await bot.send_message(
                    ADMIN_CHAT_ID,
                    f"❌ <b>Ошибка автоочистки документов</b>\n\n"
                    f"Ошибка: {str(e)[:500]}\n"
                    f"Время: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
                )
            except Exception as notify_error:
                logging.error(f"[CLEANUP_NOTIFY_ERROR] {notify_error}")

def calc_skill_tier(master_id: int) -> str:
    """Рассчитывает уровень мастерства на основе выполненных заказов"""
    try:
        master = db.fetch_one("SELECT orders_completed FROM masters WHERE id = ?", (master_id,))
        if not master:
            return "Новичок"
            
        orders_completed = master['orders_completed'] or 0
        
        if orders_completed < 20:
            return "Новичок"
        elif orders_completed < 50:
            return "Мастер"
        else:
            return "Профессионал"
    except Exception as e:
        logging.error(f"[CALC_SKILL_TIER_ERROR] {e}")
        return "Новичок"

def exp_bucket_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="до 1 года", callback_data="exp:<=1")],
        [InlineKeyboardButton(text="1–3 года", callback_data="exp:1-3")],
        [InlineKeyboardButton(text="3–5 лет", callback_data="exp:3-5")],
        [InlineKeyboardButton(text="5–10 лет", callback_data="exp:5-10")],
        [InlineKeyboardButton(text="более 10 лет", callback_data="exp:>10")],
        [InlineKeyboardButton(text="❌ Отмена", callback_data="master:cancel")]
    ])

def admin_master_card(mid: int) -> str:
    row = db.fetch_one("""
        SELECT fio, contact, phone, level, verified, has_npd_ip, categories_auto,
               exp_bucket, exp_text, portfolio, inn
        FROM masters WHERE id=?
    """, (mid,))
    if not row:
        return f"Мастер #{mid} — запись не найдена"
    
    # Используем доступ по ключам (row_factory = sqlite3.Row)
    fio = row['fio'] or '—'
    contact = row['contact'] or '—'
    phone = row['phone'] or '—'
    level = row['level'] or '—'
    verified = row['verified'] or '—'
    has_npd_ip = row['has_npd_ip'] or '—'
    cats_auto = row['categories_auto'] or '—'
    exp_bucket = row['exp_bucket'] or '—'
    exp_text = row['exp_text'] or '—'
    portfolio = row['portfolio'] or '—'
    inn = row['inn'] or '—'
    
    verified_txt = "Да" if verified else "Нет"
    npd_txt = "Да" if has_npd_ip else "Нет"
    return (
        f"🧾 Анкета мастера #{mid}\n"
        f"👤 {fio or '—'}\n"
        f"🆔 uid: {contact or '—'}\n"
        f"📞 {phone or '—'}\n"
        f"🏷 Статус: {level or '—'}\n"
        f"✅ Проверенный: {verified_txt} | НПД/ИП: {npd_txt}\n"
        f"📂 Категория(ии): {cats_auto or '—'}\n"
        f"🛠 Опыт: {exp_bucket or '—'}\n"
        f"📝 Навыки: {exp_text or '—'}\n"
        f"📚 Портфолио: {portfolio or '—'}\n"
        f"🧾 ИНН: {inn or '—'}"
    )

MASTER_CATS = ["Ремонт", "Уборка", "Переезд", "Красота", "Персонал", "Другое"]

def build_cats_kb(selected: list[str]) -> InlineKeyboardMarkup:
    """
    Рисуем клавиатуру с чекбоксами (✓) и кнопкой Готово (активна при 1–2 выбранных).
    """
    rows = []
    for title in MASTER_CATS:
        mark = "✓ " if title in selected else ""
        rows.append([InlineKeyboardButton(text=f"{mark}{title}", callback_data=f"mcat:toggle:{title}")])
    # Кнопка Готово
    done_enabled = 1 <= len(selected) <= 2
    rows.append([InlineKeyboardButton(
        text=f"Готово ({len(selected)}/2)" + ("" if done_enabled else " — выберите 1–2"),
        callback_data="mcat:done"
    )])
    # кнопка отмены
    rows.append([InlineKeyboardButton(text="❌ Отмена", callback_data="master:cancel")])
    return InlineKeyboardMarkup(inline_keyboard=rows)
    
async def request_review(request_id: int, master_id: int, client_id: str):
    """Запрос отзыва у клиента после выполнения заказа"""
    try:
        # Проверяем флаг review_requested
        request_data = db.fetch_one(
            "SELECT review_requested FROM requests WHERE id = ?", 
            (request_id,)
        )
        if request_data and request_data['review_requested']:
            logging.info(f"[REVIEW] Review already requested for request #{request_id}")
            return
        
        # Проверяем, не существует ли уже отзыв
        existing_review = db.fetch_one(
            "SELECT id FROM reviews WHERE request_id = ?", 
            (request_id,)
        )
        
        if existing_review:
            logging.info(f"[REVIEW] Review already exists for request #{request_id}")
            return
        
        # Помечаем, что отзыв запрошен
        db.execute(
            "UPDATE requests SET review_requested = 1 WHERE id = ?", 
            (request_id,)
        )
        
        # Создаем клавиатуру для оценки
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [
                InlineKeyboardButton(text="⭐ 1", callback_data=f"review:{request_id}:1"),
                InlineKeyboardButton(text="⭐ 2", callback_data=f"review:{request_id}:2"), 
                InlineKeyboardButton(text="⭐ 3", callback_data=f"review:{request_id}:3"),
                InlineKeyboardButton(text="⭐ 4", callback_data=f"review:{request_id}:4"),
                InlineKeyboardButton(text="⭐ 5", callback_data=f"review:{request_id}:5")
            ],
            [
                InlineKeyboardButton(text="📝 Написать отзыв", callback_data=f"review_text:{request_id}"),
                InlineKeyboardButton(text="🚫 Пропустить", callback_data=f"review_skip:{request_id}")
            ]
        ])
        
        # Получаем информацию о мастере для персонализации
        master_info = db.fetch_one(
            "SELECT fio FROM masters WHERE id = ?", 
            (master_id,)
        )
        master_name = master_info['fio'] if master_info else "мастер"
        
        await bot.send_message(
            client_id,
            f"📝 <b>Оцените работу {master_name}</b>\n\n"
            f"Заявка #{request_id} завершена. Пожалуйста, оцените качество услуги:",
            reply_markup=kb
        )
        
        logging.info(f"[REVIEW] Review requested for request #{request_id}")
        
    except Exception as e:
        logging.error(f"[REVIEW_REQUEST_ERROR] {e}")

async def update_master_stats(master_id: int):
    """Обновление статистики мастера на основе отзывов"""
    try:
        # Получаем средний рейтинг и количество отзывов
        stats = db.fetch_one("""
            SELECT 
                AVG(rating) as avg_rating,
                COUNT(*) as reviews_count,
                COUNT(CASE WHEN rating = 5 THEN 1 END) as five_stars,
                COUNT(CASE WHEN rating = 4 THEN 1 END) as four_stars, 
                COUNT(CASE WHEN rating = 3 THEN 1 END) as three_stars,
                COUNT(CASE WHEN rating = 2 THEN 1 END) as two_stars,
                COUNT(CASE WHEN rating = 1 THEN 1 END) as one_stars
            FROM reviews 
            WHERE master_id = ?
        """, (master_id,))
        
        if stats and stats['reviews_count'] > 0:
            avg_rating = round(stats['avg_rating'], 1)
            reviews_count = stats['reviews_count']
            
            # Обновляем рейтинг мастера
            db.execute("""
                UPDATE masters 
                SET avg_rating = ?, reviews_count = ?
                WHERE id = ?
            """, (avg_rating, reviews_count, master_id))
            
            logging.info(f"[MASTER_STATS] Updated master #{master_id}: rating={avg_rating}, reviews={reviews_count}")
        
    except Exception as e:
        logging.error(f"[MASTER_STATS_ERROR] {e}")

def get_rating_stars(rating: float) -> str:
    """Генерирует строку со звездами для рейтинга"""
    full_stars = int(rating)
    half_star = rating - full_stars >= 0.5
    empty_stars = 5 - full_stars - (1 if half_star else 0)
    
    stars = "⭐" * full_stars
    if half_star:
        stars += "✨"
    stars += "☆" * empty_stars
    
    return stars

async def mark_request_completed(request_id: int):
    """Пометить заявку как выполненную и запросить отзыв"""
    try:
        # Получаем информацию о заявке
        request = db.fetch_one("""
            SELECT id, master_id, contact, client_user_id, status 
            FROM requests 
            WHERE id = ?
        """, (request_id,))
        
        if not request:
            logging.error(f"[COMPLETE_REQUEST] Request #{request_id} not found")
            return False
        
        # Помечаем как выполненную
        db.execute("""
            UPDATE requests 
            SET status = 'completed', completed_at = CURRENT_TIMESTAMP
            WHERE id = ?
        """, (request_id,))
        
        # Определяем client_id для отправки отзыва
        client_id = request['client_user_id'] if request['client_user_id'] else request['contact']
        
        # Запрашиваем отзыв у клиента
        await request_review(
            request_id=request_id,
            master_id=request['master_id'],
            client_id=client_id
        )
        
        # Обновляем счетчик выполненных заказов у мастера
        db.execute("""
            UPDATE masters 
            SET orders_completed = orders_completed + 1,
                skill_tier = ?
            WHERE id = ?
        """, (calc_skill_tier(request['master_id']), request['master_id']))
        
        logging.info(f"[COMPLETE_REQUEST] Request #{request_id} marked as completed")
        return True
        
    except Exception as e:
        logging.error(f"[COMPLETE_REQUEST_ERROR] {e}")
        return False

# ----------------- UI ------------------
def main_menu_kb(user_id: str = None):
    """Главное меню (адаптивное для мастеров)"""
    docs_url = "https://disk.yandex.ru/d/1mlvS2VtcJTiXg"
    
    # Проверяем является ли пользователь мастером
    is_master = False
    if user_id:
        master = db.fetch_one("SELECT id FROM masters WHERE contact = ?", (user_id,))
        is_master = master is not None
    
    buttons = []
    
    # Первая строка: Заявка + (Стать мастером ИЛИ Личный кабинет)
    if is_master:
        # Для мастеров: Заявка + Личный кабинет
        buttons.append([
            InlineKeyboardButton(text="📝 Оставить заявку", callback_data="go:req"),
            InlineKeyboardButton(text="👤 Личный кабинет", callback_data="master:cabinet")
        ])
    else:
        # Для обычных пользователей: Заявка + Стать мастером
        buttons.append([
            InlineKeyboardButton(text="📝 Оставить заявку", callback_data="go:req"),
            InlineKeyboardButton(text="👨‍🔧 Стать мастером", callback_data="go:master")
        ])
    
    # Вторая строка: Документы + Пожаловаться
    buttons.append([
        InlineKeyboardButton(text="📔 Документы", url=docs_url),
        InlineKeyboardButton(text="🚨 Пожаловаться", callback_data="go:complaint")
    ])
    
    # Подписка ТОЛЬКО для мастеров
    if is_master:
        buttons.append([
            InlineKeyboardButton(text="💳 Подписка и услуги", callback_data="go:billing")
        ])
    
    return InlineKeyboardMarkup(inline_keyboard=buttons)

# ----------------- MASTER COMMON FUNCTIONS --------
async def get_master_cabinet_data(user_id: str):
    """Общая функция для получения данных личного кабинета мастера"""
    # Проверяем, является ли пользователь мастером
    master = db.fetch_one("""
        SELECT id, fio, phone, level, categories_auto,
               avg_rating, reviews_count, orders_completed, skill_tier,
               free_orders_left, sub_until, priority_until, pin_until
        FROM masters
        WHERE contact = ?
    """, (user_id,))

    if not master:
        return None

    # Эмодзи статусов
    status_emoji = {
        "Кандидат": "🟡",
        "Проверенный": "🟢",
        "Верифицированный": "💎"
    }

    # Проверяем подписки
    sub_active = is_active(master['sub_until'])
    priority_active = is_active(master['priority_until'])
    pin_active = is_active(master['pin_until'])

    # Форматируем даты
    def format_date(date_str):
        if not date_str:
            return "—"
        try:
            dt = datetime.strptime(date_str, "%Y-%m-%d %H:%M:%S")
            return dt.strftime("%d.%m.%Y")
        except:
            return "—"

    sub_status = f"✅ до {format_date(master['sub_until'])}" if sub_active else "❌ не активна"
    priority_status = f"⚡ до {format_date(master['priority_until'])}" if priority_active else "❌ не активен"
    pin_status = f"📌 до {format_date(master['pin_until'])}" if pin_active else "❌ не активен"

    # Формируем сообщение
    text = (
        f"{status_emoji.get(master['level'], '⚪')} <b>Личный кабинет мастера</b>\n\n"
        f"👤 <b>{master['fio']}</b>\n"
        f"📞 {master['phone'] or 'не указан'}\n"
        f"📂 {master['categories_auto'] or 'не указаны'}\n"
        f"🏷 Статус: {master['level']}\n"
        f"🎯 Уровень мастерства: {master['skill_tier']}\n\n"
        f"📊 <b>СТАТИСТИКА:</b>\n"
        f"✅ Выполнено заказов: {master['orders_completed']}\n"
        f"⭐ Средний рейтинг: {master['avg_rating']:.1f}/5.0\n"
        f"💬 Получено отзывов: {master['reviews_count']}\n\n"
        f"💰 <b>ПОДПИСКИ:</b>\n"
        f"🆓 Бесплатных заказов: {master['free_orders_left']}/3\n"
        f"💳 Подписка: {sub_status}\n"
        f"⚡ Приоритет: {priority_status}\n"
        f"📌 Закреп: {pin_status}"
    )

    # Создаём клавиатуру
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="📋 Мои заказы", callback_data="master:orders"),
            InlineKeyboardButton(text="⭐ Отзывы", callback_data="master:reviews")
        ],
        [
            InlineKeyboardButton(text="📊 Статистика", callback_data="master:stats"),
            InlineKeyboardButton(text="💳 Подписка", callback_data="go:billing")
        ],
        [InlineKeyboardButton(text="🏠 Главное меню", callback_data="go:menu")]
    ])

    return {"text": text, "keyboard": kb}

async def get_master_reviews_data(user_id: str):
    """Общая функция для получения отзывов мастера"""
    try:
        # Получаем данные мастера
        master = db.fetch_one("SELECT id, fio FROM masters WHERE contact = ?", (user_id,))

        if not master:
            return None

        master_id = master['id']
        master_fio = master['fio']

        # Получаем статистику
        stats = db.fetch_one("""
            SELECT
                COUNT(*) as reviews_count,
                AVG(rating) as avg_rating,
                COUNT(CASE WHEN rating = 5 THEN 1 END) as five_stars,
                COUNT(CASE WHEN rating = 4 THEN 1 END) as four_stars,
                COUNT(CASE WHEN rating = 3 THEN 1 END) as three_stars,
                COUNT(CASE WHEN rating = 2 THEN 1 END) as two_stars,
                COUNT(CASE WHEN rating = 1 THEN 1 END) as one_stars
            FROM reviews
            WHERE master_id = ?
        """, (master_id,))

        if not stats or stats['reviews_count'] == 0:
            return {"text": "📝 У вас пока нет отзывов", "keyboard": None}

        # Получаем последние отзывы
        reviews = db.fetch_all("""
            SELECT r.rating, r.comment, r.created_at, r.request_id
            FROM reviews r
            WHERE r.master_id = ?
            ORDER BY r.created_at DESC
            LIMIT 10
        """, (master_id,))

        # Формируем сообщение
        text_lines = [f"⭐ <b>Отзывы на {master_fio}</b>\n"]

        # Общая статистика
        text_lines.append(f"📊 <b>Общая оценка: {stats['avg_rating']:.1f}/5.0</b> ({stats['reviews_count']} отзывов)\n")

        # Разбивка по звёздам
        text_lines.append("<b>Распределение оценок:</b>")
        total = stats['reviews_count']
        text_lines.append(f"⭐⭐⭐⭐⭐ {stats['five_stars']} ({stats['five_stars']/total*100:.0f}%)")
        text_lines.append(f"⭐⭐⭐⭐ {stats['four_stars']} ({stats['four_stars']/total*100:.0f}%)")
        text_lines.append(f"⭐⭐⭐ {stats['three_stars']} ({stats['three_stars']/total*100:.0f}%)")
        if stats['two_stars'] > 0:
            text_lines.append(f"⭐⭐ {stats['two_stars']} ({stats['two_stars']/total*100:.0f}%)")
        if stats['one_stars'] > 0:
            text_lines.append(f"⭐ {stats['one_stars']} ({stats['one_stars']/total*100:.0f}%)")

        # Последние отзывы
        text_lines.append(f"\n<b>Последние отзывы:</b>")

        for i, review in enumerate(reviews[:5], 1):
            stars = get_rating_stars(review['rating'])
            date = datetime.fromisoformat(review['created_at']).strftime('%d.%m.%Y')

            text_lines.append(f"\n{i}. {stars} <i>({date})</i>")
            if review['comment']:
                comment = review['comment']
                if len(comment) > 150:
                    comment = comment[:150] + "..."
                text_lines.append(f"   💬 «{comment}»")

        if len(reviews) > 5:
            text_lines.append(f"\n... и ещё {len(reviews) - 5} отзывов")

        return {"text": "\n".join(text_lines), "keyboard": None}

    except Exception as e:
        logging.error(f"[GET_MASTER_REVIEWS_ERROR] {e}")
        return {"error": "❌ Ошибка при получении отзывов"}

async def get_master_stats_data(user_id: str):
    """Общая функция для получения статистики мастера"""
    try:
        # Получаем данные мастера
        master = db.fetch_one("""
            SELECT id, fio, created_at, orders_completed, avg_rating, reviews_count, skill_tier
            FROM masters
            WHERE contact = ?
        """, (user_id,))

        if not master:
            return None

        master_id = master['id']

        # Считаем активные заказы
        active_orders = db.fetch_one("""
            SELECT COUNT(*) as count
            FROM requests
            WHERE master_id = ? AND status = 'assigned'
        """, (master_id,))['count']

        # Считаем пропущенные заказы
        skipped_orders = db.fetch_one("""
            SELECT COUNT(*) as count
            FROM offers
            WHERE master_id = ? AND status = 'skipped'
        """, (master_id,))['count']

        # Считаем принятые предложения
        accepted_offers = db.fetch_one("""
            SELECT COUNT(*) as count
            FROM offers
            WHERE master_id = ? AND status = 'accepted'
        """, (master_id,))['count']

        # Дата регистрации
        reg_date = datetime.fromisoformat(master['created_at']).strftime('%d.%m.%Y')

        # Формируем сообщение
        text_lines = [
            f"📊 <b>Статистика мастера {master['fio']}</b>\n",
            f"📅 <b>Дата регистрации:</b> {reg_date}",
            f"🎯 <b>Уровень мастерства:</b> {master['skill_tier']}\n",

            "<b>📈 ВЫПОЛНЕНИЕ ЗАКАЗОВ:</b>",
            f"✅ Завершено: {master['orders_completed']}",
            f"🔄 В работе: {active_orders}",
            f"⏭ Пропущено: {skipped_orders}",
            f"📥 Принято предложений: {accepted_offers}\n",

            "<b>⭐ РЕЙТИНГ И ОТЗЫВЫ:</b>",
            f"⭐ Средний рейтинг: {master['avg_rating']:.1f}/5.0",
            f"💬 Всего отзывов: {master['reviews_count']}"
        ]

        # Добавляем процент принятия заказов
        total_offers = accepted_offers + skipped_orders
        if total_offers > 0:
            accept_rate = (accepted_offers / total_offers) * 100
            text_lines.append(f"📊 Процент принятия: {accept_rate:.0f}%")

        return {"text": "\n".join(text_lines), "keyboard": None}

    except Exception as e:
        logging.error(f"[GET_MASTER_STATS_ERROR] {e}")
        return {"error": "❌ Ошибка при получении статистики"}

async def get_master_orders_data(user_id: str):
    """Общая функция для получения заказов мастера"""
    try:
        # Получаем ID мастера
        master = db.fetch_one("SELECT id, fio FROM masters WHERE contact = ?", (user_id,))

        if not master:
            return None

        master_id = master['id']

        # Активные заказы
        active = db.fetch_all("""
            SELECT id, category, district, when_text, status
            FROM requests
            WHERE master_id = ? AND status IN ('assigned', 'pending_confirmation')
            ORDER BY created_at DESC
        """, (master_id,))

        # Завершённые заказы
        completed = db.fetch_all("""
            SELECT r.id, r.category, r.completed_at, rev.rating
            FROM requests r
            LEFT JOIN reviews rev ON r.id = rev.request_id
            WHERE r.master_id = ? AND r.status = 'completed'
            ORDER BY r.completed_at DESC
            LIMIT 10
        """, (master_id,))

        # Пропущенные заказы
        skipped = db.fetch_all("""
            SELECT req.id, req.category, o.created_at
            FROM offers o
            JOIN requests req ON o.request_id = req.id
            WHERE o.master_id = ? AND o.status = 'skipped'
            ORDER BY o.created_at DESC
            LIMIT 5
        """, (master_id,))

        # Формируем сообщение
        text_lines = [f"📋 <b>Мои заказы</b>\n"]

        # Активные
        if active:
            text_lines.append(f"🟢 <b>АКТИВНЫЕ ({len(active)}):</b>")
            for order in active:
                status_text = "⏳ ждём подтверждения" if order['status'] == 'pending_confirmation' else "в работе"
                text_lines.append(
                    f"  #{order['id']} | {order['category']} | {status_text}\n"
                    f"  📍 {order['district']}\n"
                    f"  🗓 {order['when_text']}"
                )
            text_lines.append("")
        else:
            text_lines.append("🟢 <b>АКТИВНЫЕ:</b> нет\n")

        # Завершённые
        if completed:
            text_lines.append(f"✅ <b>ЗАВЕРШЁННЫЕ ({len(completed)}):</b>")
            for order in completed[:5]:
                date = datetime.fromisoformat(order['completed_at']).strftime('%d.%m.%Y')
                rating_text = f"⭐ {order['rating']}" if order['rating'] else "без отзыва"
                text_lines.append(f"  #{order['id']} | {order['category']} | {date} | {rating_text}")

            if len(completed) > 5:
                text_lines.append(f"  ... и ещё {len(completed) - 5} заказов")
            text_lines.append("")
        else:
            text_lines.append("✅ <b>ЗАВЕРШЁННЫЕ:</b> нет\n")

        # Пропущенные
        if skipped:
            text_lines.append(f"⏭ <b>ПРОПУЩЕННЫЕ ({len(skipped)}):</b>")
            for order in skipped[:3]:
                date = datetime.fromisoformat(order['created_at']).strftime('%d.%m.%Y')
                text_lines.append(f"  #{order['id']} | {order['category']} | {date}")

            if len(skipped) > 3:
                text_lines.append(f"  ... и ещё {len(skipped) - 3} заказов")

        return {"text": "\n".join(text_lines), "keyboard": None}

    except Exception as e:
        logging.error(f"[GET_MASTER_ORDERS_ERROR] {e}")
        return {"error": "❌ Ошибка при получении заказов"}

# ----------------- START / MENU --------
@dp.message(CommandStart())
async def start(m: Message):
    user_id = m.from_user.id
    
    # Лимит: 10 запусков бота в час
    if not rate_limiter.check_limit(user_id, "start_command", 10, 3600):
        remaining_time = rate_limiter.get_time_until_reset(user_id, "start_command", 3600)
        hours = remaining_time // 3600
        minutes = (remaining_time % 3600) // 60
        
        time_msg = f"{minutes} минут" if hours == 0 else f"{hours} час {minutes} минут"
        await m.answer(
            f"❌ Слишком много запросов. Попробуйте через {time_msg}.",
            reply_markup=main_menu_kb(str(c.from_user.id))
        )
        return
    
    await m.answer(
        "👋 Добро пожаловать в «Мастера Верхней Пышмы»!\n\n"
        "Здесь вы можете:\n"
        "• Найти проверенного мастера\n"
        "• Стать мастером и получать заказы\n\n"
        "📄 Ознакомьтесь с <a href='https://disk.yandex.ru/d/1mlvS2VtcJTiXg'>документами</a> перед использованием.",
        reply_markup=main_menu_kb(str(user_id)),
        disable_web_page_preview=True
    )

@dp.message(Command("menu"))
async def menu(m: Message):
    await start(m)

@dp.callback_query(F.data.startswith("go:"))
async def go_router(c: CallbackQuery, state: FSMContext):
    await state.clear()
    action = c.data.split(":")[1]
    user_id = c.from_user.id
    
    if action == "req":
        # Лимит: 3 новые заявки в час
        if not rate_limiter.check_limit(user_id, "new_request", 3, 3600):
            remaining = rate_limiter.get_remaining(user_id, "new_request", 3, 3600)
            remaining_time = rate_limiter.get_time_until_reset(user_id, "new_request", 3600)
            
            if remaining_time > 0:
                minutes = (remaining_time % 3600) // 60
                await c.answer(
                    f"❌ Лимит заявок исчерпан. Доступно через: {minutes} минут.\n"
                    f"💡 Можно создать: {remaining} заявок после сброса лимита",
                    show_alert=True
                )
            else:
                await c.answer(f"❌ Лимит заявок исчерпан. Можно создать: {remaining} заявок", show_alert=True)
            return
        
        await c.message.answer("Как вас зовут?")
        await state.set_state(Req.name)
        
    elif action == "master":
        # Лимит: 3 попытки регистрации мастера за 24 часа
        if not rate_limiter.check_limit(user_id, "master_registration", 3, 86400):
            remaining_time = rate_limiter.get_time_until_reset(user_id, "master_registration", 86400)
            hours = remaining_time // 10800
            
            await c.answer(
                f"❌ Регистрация мастера возможна 3 раза в сутки. Попробуйте через {hours} часов.",
                show_alert=True
            )
            return
        
        await c.message.answer("Анкета мастера. Укажите ваши ФИО:")
        await state.set_state(MasterForm.fio)
        
    elif action == "complaint":
        # Лимит: 5 жалоб в сутки
        if not rate_limiter.check_limit(user_id, "complaint", 5, 86400):
            remaining = rate_limiter.get_remaining(user_id, "complaint", 5, 86400)
            remaining_time = rate_limiter.get_time_until_reset(user_id, "complaint", 86400)
            
            # Форматируем время до сброса
            hours = remaining_time // 3600
            minutes = (remaining_time % 3600) // 60
            
            if hours > 0:
                time_msg = f"{hours} часов {minutes} минут"
            else:
                time_msg = f"{minutes} минут"
            
            await c.answer(
                f"❌ Лимит жалоб исчерпан.\n"
                f"💡 Можно отправить: {remaining} жалоб через {time_msg}",
                show_alert=True
            )
            return
        
        await c.message.answer("Жалоба. Кто вы? (клиент/мастер/другое)")
        await state.set_state(Complaint.who)
        
    elif action == "billing":
        await c.message.answer(
            "<b>Подписка и услуги для мастеров</b>\n\n"
            "🔹 Новым мастерам: 3 заказа бесплатно\n"
            "🔹 Далее подписка: 990 ₽/мес (безлимит)\n\n"
            "Доп.услуги:\n"
            "⚡ Приоритет заказов — 490 ₽/мес\n"
            "📌 Закреп анкеты — 190 ₽/нед\n",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="💳 Подписка 990 ₽", callback_data="pay:sub")],
                [InlineKeyboardButton(text="⚡ Приоритет 490 ₽", callback_data="pay:priority")],
                [InlineKeyboardButton(text="📌 Закреп 190 ₽", callback_data="pay:pin")],
                [InlineKeyboardButton(text="🏠 Меню", callback_data="go:menu")]
            ])
        )
    elif action == "menu":
        await c.message.edit_text(
            "Главное меню:", 
            reply_markup=main_menu_kb(str(c.from_user.id))
        )
    
    await c.answer()

@dp.callback_query(F.data == "master:cancel")
async def master_cancel(c: CallbackQuery, state: FSMContext):
    await state.clear()
    await c.message.edit_text("❌ Заполнение анкеты мастера отменено.", reply_markup=main_menu_kb(str(c.from_user.id)))
    await c.answer()

@dp.callback_query(F.data.startswith("review:"))
async def process_rating(c: CallbackQuery, state: FSMContext):
    """Обработка оценки от клиента"""
    try:
        _, request_id, rating = c.data.split(":")
        request_id, rating = int(request_id), int(rating)
        
        # Получаем информацию о заявке
        request = db.fetch_one("""
            SELECT master_id, contact 
            FROM requests 
            WHERE id = ?
        """, (request_id,))
        
        if not request:
            await c.answer("❌ Заявка не найдена")
            return
        
        master_id = request['master_id']
        
        # Сохраняем оценку
        db.execute("""
            INSERT INTO reviews (request_id, master_id, client_id, rating)
            VALUES (?, ?, ?, ?)
        """, (request_id, master_id, str(c.from_user.id), rating))
        db.commit()

        # Обновляем статистику мастера
        await update_master_stats(master_id)
        
        # Предлагаем написать текстовый отзыв
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="📝 Написать отзыв", callback_data=f"review_text:{request_id}")],
            [InlineKeyboardButton(text="✅ Готово", callback_data=f"review_done:{request_id}")]
        ])
        
        await c.message.edit_text(
            f"✅ Спасибо за вашу оценку: {rating} {get_rating_stars(rating)}\n\n"
            "Хотите добавить текстовый отзыв?",
            reply_markup=kb
        )
        
        await c.answer()
        
    except Exception as e:
        logging.error(f"[RATING_PROCESS_ERROR] {e}")
        await c.answer("❌ Ошибка при обработке оценки")

@dp.callback_query(F.data.startswith("review_text:"))
async def request_review_text(c: CallbackQuery, state: FSMContext):
    """Запрос текстового отзыва"""
    try:
        request_id = int(c.data.split(":")[1])
        
        # Сохраняем request_id в состоянии
        await state.update_data(review_request_id=request_id)
        await state.set_state(Complaint.text)  # Используем существующее состояние для текста
        
        await c.message.edit_text(
            "📝 Напишите ваш отзыв о работе мастера:\n\n"
            "• Что понравилось?\n"
            "• Что можно улучшить?\n"
            "• Рекомендуете ли вы этого мастера?\n\n"
            "Или отправьте /cancel для отмены"
        )
        
        await c.answer()
        
    except Exception as e:
        logging.error(f"[REVIEW_TEXT_ERROR] {e}")
        await c.answer("❌ Ошибка")

@dp.callback_query(F.data.startswith("review_done:"))
async def finish_review(c: CallbackQuery, state: FSMContext):
    """Завершение процесса отзыва"""
    try:
        request_id = int(c.data.split(":")[1])
        
        await c.message.edit_text(
            "✅ Спасибо за ваш отзыв! Он поможет другим пользователям выбрать надежного мастера."
        )
        
        await c.answer()
        
    except Exception as e:
        logging.error(f"[REVIEW_DONE_ERROR] {e}")
        await c.answer("❌ Ошибка")

@dp.callback_query(F.data.startswith("review_skip:"))
async def skip_review(c: CallbackQuery, state: FSMContext):
    """Пропуск отзыва"""
    try:
        request_id = int(c.data.split(":")[1])
        
        await c.message.edit_text(
            "👌 Хорошо! Если передумаете - всегда можете написать отзыв позже."
        )
        
        await c.answer()
        
    except Exception as e:
        logging.error(f"[REVIEW_SKIP_ERROR] {e}")
        await c.answer("❌ Ошибка")

@dp.callback_query(F.data.startswith("complete:"))
async def complete_order(c: CallbackQuery):
    """Мастер отмечает заказ как выполненный (ждём подтверждения клиента)"""
    try:
        _, request_id = c.data.split(":")
        request_id = int(request_id)
        
        user_id = str(c.from_user.id)
        
        # Проверяем, что заказ принадлежит этому мастеру
        request = db.fetch_one("""
            SELECT id, master_id, status, client_user_id 
            FROM requests 
            WHERE id = ? AND master_id = (SELECT id FROM masters WHERE contact = ?)
        """, (request_id, user_id))
        
        if not request:
            await c.answer("❌ Заказ не найден или у вас нет прав", show_alert=True)
            return
        
        if request['status'] == 'completed':
            await c.answer("❌ Этот заказ уже завершен", show_alert=True)
            return
        
        if request['status'] == 'pending_confirmation':
            await c.answer("⏳ Ожидаем подтверждения от клиента", show_alert=True)
            return
        
        # Меняем статус на "ожидает подтверждения"
        db.execute(
            "UPDATE requests SET status = 'pending_confirmation' WHERE id = ?", 
            (request_id,)
        )
        
        # Уведомляем мастера
        await c.message.edit_text(
            "⏳ Вы отметили заказ как выполненный.\n"
            "Ожидаем подтверждения от клиента."
        )
        
        # Запрашиваем подтверждение у клиента
        if request['client_user_id']:
            master_info = db.fetch_one("SELECT fio FROM masters WHERE id = ?", (request['master_id'],))
            master_name = master_info['fio'] if master_info else "Мастер"
            
            kb = InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="✅ Да, всё отлично", callback_data=f"confirm:{request_id}:yes")],
                [InlineKeyboardButton(text="❌ Есть проблемы", callback_data=f"confirm:{request_id}:no")]
            ])
            
            try:
                await bot.send_message(
                    int(request['client_user_id']),
                    f"👨‍🔧 <b>{master_name}</b> отметил заказ #{request_id} как выполненный.\n\n"
                    f"Работа действительно выполнена качественно?",
                    reply_markup=kb
                )
            except Exception as e:
                logging.error(f"[CLIENT_CONFIRM_ERROR] {e}")
        
        await c.answer()
        
    except Exception as e:
        logging.error(f"[COMPLETE_ORDER_ERROR] {e}")
        await c.answer("❌ Ошибка")

@dp.callback_query(F.data.startswith("confirm:"))
async def client_confirmation(c: CallbackQuery):
    """Клиент подтверждает или отклоняет завершение заказа"""
    try:
        _, request_id, answer = c.data.split(":")
        request_id = int(request_id)
        
        # Получаем информацию о заказе
        request = db.fetch_one("""
            SELECT id, master_id, status, name
            FROM requests 
            WHERE id = ?
        """, (request_id,))
        
        if not request:
            await c.answer("❌ Заказ не найден", show_alert=True)
            return
        
        if request['status'] == 'completed':
            await c.answer("✅ Заказ уже завершён", show_alert=True)
            return
        
        if answer == "yes":
            # Клиент подтвердил — завершаем заказ
            success = await mark_request_completed(request_id)
            
            if success:
                await c.message.edit_text(
                    "✅ Спасибо за подтверждение!\n"
                    "Пожалуйста, оцените работу мастера 👇"
                )
                
                # Уведомляем мастера
                master = db.fetch_one("SELECT contact FROM masters WHERE id = ?", (request['master_id'],))
                if master:
                    try:
                        await bot.send_message(
                            int(master['contact']),
                            f"✅ Клиент подтвердил выполнение заказа #{request_id}.\n"
                            f"Заказ завершён успешно! 🎉"
                        )
                    except Exception as e:
                        logging.error(f"[MASTER_NOTIFY_ERROR] {e}")
            else:
                await c.message.edit_text("❌ Ошибка при завершении заказа")
        
        elif answer == "no":
            # Клиент жалуется
            await c.message.edit_text(
                "😔 Нам очень жаль, что возникли проблемы.\n\n"
                "Администратор свяжется с вами для решения вопроса.\n"
                "Вы также можете написать жалобу через главное меню.",
                reply_markup=main_menu_kb(str(c.from_user.id))
            )
            
            # Возвращаем статус "assigned"
            db.execute("UPDATE requests SET status = 'assigned' WHERE id = ?", (request_id,))
            
            # Уведомляем админа
            await notify_admin(
                f"⚠️ <b>Проблема с заказом #{request_id}</b>\n\n"
                f"Клиент: {request['name']}\n"
                f"Не подтвердил выполнение работ.\n\n"
                f"Требуется разбирательство."
            )
            
            # Уведомляем мастера
            master = db.fetch_one("SELECT contact FROM masters WHERE id = ?", (request['master_id'],))
            if master:
                try:
                    await bot.send_message(
                        int(master['contact']),
                        f"⚠️ Клиент сообщил о проблемах с заказом #{request_id}.\n"
                        f"Администратор свяжется с вами."
                    )
                except Exception as e:
                    logging.error(f"[MASTER_NOTIFY_ERROR] {e}")
        
        await c.answer()
        
    except Exception as e:
        logging.error(f"[CLIENT_CONFIRMATION_ERROR] {e}")
        await c.answer("❌ Ошибка")

@dp.message(Command("delete_profile"))
async def delete_profile(m: Message):
    user_id = str(m.from_user.id)
    
    # Удаляем данные мастера
    db.execute("DELETE FROM masters WHERE contact = ?", (user_id,))
    
    # Удаляем заявки клиента (и по старому contact, и по новому client_user_id)
    db.execute("""
        DELETE FROM requests 
        WHERE contact = ? OR client_user_id = ?
    """, (user_id, user_id))
    
    # Удаляем жалобы, где пользователь указан как отправитель или мастер
    db.execute("DELETE FROM complaints WHERE who = ? OR master_id = ?", (user_id, user_id))
    
    await m.answer(
        "✅ Ваши данные удалены из сервиса в соответствии с Политикой конфиденциальности.\n"
        "Если вы захотите вернуться — просто начните заново.",
        reply_markup=main_menu_kb(str(m.from_user.id))
    )

@dp.message(Command("help"))
async def cmd_help(m: Message):
    """Помощь с использованием бота"""
    user_id = str(m.from_user.id)
    
    # Проверяем является ли пользователь мастером
    is_master = db.fetch_one("SELECT id FROM masters WHERE contact = ?", (user_id,))
    
    help_text = [
        "❓ <b>ПОМОЩЬ</b>\n",
        "🤖 <b>Основные функции бота:</b>",
        "",
        "📝 <b>Для клиентов:</b>",
        "• Оставить заявку — опишите задачу, мы подберём мастеров",
        "• Мастера свяжутся с вами напрямую",
        "• После выполнения — оставьте отзыв",
        "",
        "👨‍🔧 <b>Для мастеров:</b>",
        "• Зарегистрируйтесь и пройдите верификацию",
        "• Получайте заявки по вашим категориям",
        "• Первые 3 заказа бесплатно!",
        "",
        "💡 <b>Полезные команды:</b>",
        "• /menu — главное меню",
        "• /faq — частые вопросы",
        "• /support — связаться с поддержкой",
        "• /limits — ваши лимиты действий",
    ]
    
    # Команды для мастеров
    if is_master:
        help_text.extend([
            "",
            "👨‍🔧 <b>Команды для мастеров:</b>",
            "• /master — личный кабинет",
            "• /my_orders — история заказов",
            "• /my_reviews — мои отзывы",
            "• /my_stats — моя статистика",
        ])
    
    # Команды для админа
    if is_admin(m.from_user.id):
        help_text.extend([
            "",
            "👑 <b>Команды администратора:</b>",
            "• /stats — статистика сервиса",
            "• /cleanup_status — статус очистки документов", 
            "• /cleanup_now — принудительная очистка",
            "• /reviews <id> — отзывы на мастера"
        ])
    
    help_text.extend([
        "",
        "🔒 <b>Конфиденциальность:</b>",
        "• /delete_profile — удалить все данные",
        "",
        "💬 Если остались вопросы — /support"
    ])

    # Клавиатура с быстрыми действиями
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="❔ Частые вопросы", callback_data="help:faq"),
            InlineKeyboardButton(text="📞 Поддержка", callback_data="help:support")
        ],
        [InlineKeyboardButton(text="🏠 Главное меню", callback_data="go:menu")]
    ])

    await m.answer("\n".join(help_text), reply_markup=kb)

@dp.message(Command("faq"))
async def cmd_faq(m: Message):
    """Частые вопросы"""
    await show_faq(m.chat.id)

@dp.callback_query(F.data == "help:faq")
async def callback_faq(c: CallbackQuery):
    """FAQ через callback"""
    await show_faq(c.message.chat.id)
    await c.answer()

async def show_faq(chat_id: int):
    """Показать FAQ"""
    faq_text = [
        "❔ <b>ЧАСТЫЕ ВОПРОСЫ</b>\n",
        
        "<b>🙋‍♂️ Для клиентов:</b>\n",
        
        "❓ <b>Как заказать услугу?</b>",
        "• Нажмите «Оставить заявку»",
        "• Заполните форму с описанием задачи",
        "• Мы подберём 3-5 мастеров",
        "• Мастер свяжется с вами напрямую",
        "",
        
        "❓ <b>Сколько это стоит?</b>",
        "• Подбор мастеров — БЕСПЛАТНО",
        "• Цену обсуждаете напрямую с мастером",
        "",
        
        "❓ <b>Как выбрать мастера?</b>",
        "• Смотрите на рейтинг (⭐)",
        "• Читайте отзывы других клиентов",
        "• Все мастера проходят проверку",
        "",
        
        "❓ <b>Что если работа выполнена плохо?</b>",
        "• Не подтверждайте выполнение",
        "• Нажмите «Есть проблемы»",
        "• Администрация разберётся в ситуации",
        "",
        
        "<b>👨‍🔧 Для мастеров:</b>\n",
        
        "❓ <b>Как стать мастером?</b>",
        "• Нажмите «Стать мастером»",
        "• Заполните анкету",
        "• Пройдите проверку (по желанию)",
        "",
        
        "❓ <b>Сколько стоит?</b>",
        "• Первые 3 заказа — БЕСПЛАТНО",
        "• Далее: 990 ₽/мес (безлимит заказов)",
        "",
        
        "❓ <b>Как получать больше заказов?</b>",
        "• Поддерживайте высокий рейтинг (4.5+)",
        "• Быстро отвечайте на заявки",
        "• Оформите приоритет (490 ₽/мес)",
        "",
        
        "❓ <b>Что дают статусы?</b>",
        "• 🟡 Кандидат — базовый уровень",
        "• 🟢 Проверенный — прошли проверку документов",
        "• 💎 Верифицированный — НПД/ИП подтверждён",
        "",
        
        "💬 Не нашли ответ? — /support"
    ]
    
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📞 Связаться с поддержкой", callback_data="help:support")],
        [InlineKeyboardButton(text="🏠 Главное меню", callback_data="go:menu")]
    ])
    
    await bot.send_message(chat_id, "\n".join(faq_text), reply_markup=kb)

@dp.message(Command("support"))
async def cmd_support(m: Message):
    """Связаться с поддержкой"""
    await show_support(m.chat.id)

@dp.callback_query(F.data == "help:support")
async def callback_support(c: CallbackQuery):
    """Поддержка через callback"""
    await show_support(c.message.chat.id)
    await c.answer()

async def show_support(chat_id: int):
    """Показать контакты поддержки"""
    support_text = [
        "📞 <b>ПОДДЕРЖКА</b>\n",
        "Мы всегда готовы помочь!\n",
        
        "<b>Способы связи:</b>",
        "• 📱 Telegram: @am_burkov",
        "• 📧 Email: aburkov2017@yandex.ru",
        "• ⏰ Время работы: Пн-Пт 10:00-19:00",
        "",
        
        "<b>📝 Или оставьте жалобу:</b>",
        "Нажмите кнопку ниже, опишите проблему — мы свяжемся с вами в течение 24 часов.",
        "",
        
        "💡 <b>Перед обращением:</b>",
        "• Проверьте /faq — может ответ уже есть",
        "• Укажите номер заказа (если есть)",
        "• Опишите проблему подробно"
    ]
    
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🚨 Оставить жалобу", callback_data="go:complaint")],
        [InlineKeyboardButton(text="❔ Частые вопросы", callback_data="help:faq")],
        [InlineKeyboardButton(text="🏠 Главное меню", callback_data="go:menu")]
    ])
    
    await bot.send_message(chat_id, "\n".join(support_text), reply_markup=kb)

@dp.callback_query(F.data == "master:cabinet")
async def callback_master_cabinet(c: CallbackQuery):
    """Личный кабинет мастера (через callback)"""
    user_id = str(c.from_user.id)

    data = await get_master_cabinet_data(user_id)

    if not data:
        await c.answer(
            "❌ Вы не зарегистрированы как мастер",
            show_alert=True
        )
        return

    await c.message.answer(data["text"], reply_markup=data["keyboard"])
    await c.answer()

@dp.message(Command("master"))
async def cmd_master_cabinet(m: Message):
    """Личный кабинет мастера"""
    user_id = str(m.from_user.id)

    data = await get_master_cabinet_data(user_id)

    if not data:
        await m.answer(
            "❌ Вы не зарегистрированы как мастер.\n"
            "Хотите зарегистрироваться?",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="👨‍🔧 Стать мастером", callback_data="go:master")],
                [InlineKeyboardButton(text="🏠 Главное меню", callback_data="go:menu")]
            ])
        )
        return

    await m.answer(data["text"], reply_markup=data["keyboard"])

@dp.message(Command("limits"))
async def cmd_limits(m: Message):
    """Показать текущие лимиты пользователя"""
    user_id = m.from_user.id
    
    limits_info = [
        "📊 <b>Ваши текущие лимиты:</b>\n",
        f"🚀 Запуски бота: {rate_limiter.get_remaining(user_id, 'start_command', 10, 3600)}/10 (в час)",
        f"📝 Новые заявки: {rate_limiter.get_remaining(user_id, 'new_request', 3, 3600)}/3 (в час)",
        f"👨‍🔧 Регистрация мастера: {rate_limiter.get_remaining(user_id, 'master_registration', 1, 86400)}/1 (в сутки)",
        f"🚨 Жалобы: {rate_limiter.get_remaining(user_id, 'complaint', 5, 86400)}/5 (в сутки)",
        f"⚡ Действия с заказами: {rate_limiter.get_remaining(user_id, 'offer_actions', 10, 3600)}/10 (в час)",
        f"💬 Сообщения: {rate_limiter.get_remaining(user_id, 'any_message', 20, 60)}/20 (в минуту)",
        "",
        "💡 <i>Лимиты сбрасываются автоматически</i>"
    ]
    
    await m.answer("\n".join(limits_info))

@dp.message(Command("cleanup_now"))
async def cmd_cleanup_now(m: Message):
    """Принудительная очистка документов (только для админа)"""
    if not is_admin(m.from_user.id):
        await m.answer("❌ Эта команда доступна только администратору")
        return
    
    await m.answer("🔄 Запуск принудительной очистки документов...")
    
    try:
        # Проверяем сколько документов будет очищено
        pending_count = db.fetch_one("""
            SELECT COUNT(*) as count 
            FROM masters 
            WHERE created_at < datetime('now', '-72 hours')
              AND (passport_scan_file_id IS NOT NULL 
                   OR face_photo_file_id IS NOT NULL 
                   OR npd_ip_doc_file_id IS NOT NULL)
        """)['count']
        
        if pending_count == 0:
            await m.answer("✅ Нет документов для очистки (все моложе 72 часов)")
            return
        
        await m.answer(f"🧹 Найдено документов для очистки: {pending_count}\nЗапускаю процесс...")
        
        # Запускаем очистку
        await safe_cleanup_documents()
        
        await m.answer("✅ Принудительная очистка завершена")
        
    except Exception as e:
        logging.error(f"[MANUAL_CLEANUP_ERROR] {e}")
        await m.answer(f"❌ Ошибка при очистке: {str(e)[:500]}")

@dp.message(Command("cleanup_status"))
async def cmd_cleanup_status(m: Message):
    """Показать статус документов для очистки (только для админа)"""
    if not is_admin(m.from_user.id):
        await m.answer("❌ Эта команда доступна только администратору")
        return
    
    try:
        # Статистика по документам
        stats = db.fetch_one("""
            SELECT 
                COUNT(*) as total_masters,
                COUNT(CASE WHEN passport_scan_file_id IS NOT NULL THEN 1 END) as with_passport,
                COUNT(CASE WHEN face_photo_file_id IS NOT NULL THEN 1 END) as with_face_photo,
                COUNT(CASE WHEN npd_ip_doc_file_id IS NOT NULL THEN 1 END) as with_npd_doc,
                COUNT(CASE WHEN created_at < datetime('now', '-72 hours') 
                          AND (passport_scan_file_id IS NOT NULL 
                               OR face_photo_file_id IS NOT NULL 
                               OR npd_ip_doc_file_id IS NOT NULL) THEN 1 END) as pending_cleanup
            FROM masters
        """)
        
        # Детали по мастерам с документами старше 72 часов
        pending_masters = db.fetch_all("""
            SELECT id, fio, level, datetime(created_at) as created_time
            FROM masters 
            WHERE created_at < datetime('now', '-72 hours')
              AND (passport_scan_file_id IS NOT NULL 
                   OR face_photo_file_id IS NOT NULL 
                   OR npd_ip_doc_file_id IS NOT NULL)
            ORDER BY created_at ASC
            LIMIT 10
        """)
        
        report_lines = [
            "📊 <b>Статус очистки документов</b>",
            "",
            f"👥 Всего мастеров: {stats['total_masters']}",
            f"📄 С сканами паспорта: {stats['with_passport']}",
            f"📷 С фото лица: {stats['with_face_photo']}",
            f"🏢 С документами НПД/ИП: {stats['with_npd_doc']}",
            f"🧹 Ожидают очистки (>72ч): {stats['pending_cleanup']}",
            "",
            "<b>Ближайшие к очистке:</b>"
        ]
        
        if pending_masters:
            for master in pending_masters:
                time_ago = (datetime.now() - datetime.fromisoformat(master['created_time'])).days
                report_lines.append(
                    f"#{master['id']} {master['fio'] or 'Неизвестно'} "
                    f"({master['level']}) - {time_ago} дн. назад"
                )
        else:
            report_lines.append("✅ Нет документов для очистки")
        
        report_lines.extend([
            "",
            "💡 <i>Очистка выполняется автоматически каждые 24 часа</i>",
            "⚡ <i>Быстрая очистка: /cleanup_now</i>"
        ])
        
        await m.answer("\n".join(report_lines))
        
    except Exception as e:
        logging.error(f"[CLEANUP_STATUS_ERROR] {e}")
        await m.answer(f"❌ Ошибка получения статуса: {str(e)[:500]}")

@dp.message(Command("my_reviews"))
async def cmd_my_reviews(m: Message):
    """Показать отзывы на мастера (команда)"""
    user_id = str(m.from_user.id)

    data = await get_master_reviews_data(user_id)

    if not data:
        await m.answer("❌ Вы не зарегистрированы как мастер")
        return

    if "error" in data:
        await m.answer(data["error"])
        return

    await m.answer(data["text"])

@dp.callback_query(F.data == "master:reviews")
async def callback_master_reviews(c: CallbackQuery):
    """Показать отзывы на мастера (callback)"""
    user_id = str(c.from_user.id)

    data = await get_master_reviews_data(user_id)

    if not data:
        await c.answer("❌ Вы не зарегистрированы как мастер", show_alert=True)
        return

    if "error" in data:
        await c.answer(data["error"], show_alert=True)
        return

    await c.message.answer(data["text"])
    await c.answer()

@dp.message(Command("my_stats"))
async def cmd_my_stats(m: Message):
    """Статистика мастера (команда)"""
    user_id = str(m.from_user.id)

    data = await get_master_stats_data(user_id)

    if not data:
        await m.answer("❌ Вы не зарегистрированы как мастер")
        return

    if "error" in data:
        await m.answer(data["error"])
        return

    await m.answer(data["text"])

@dp.message(Command("my_requests"))
async def cmd_my_requests(m: Message):
    """Мои заявки (для клиентов)"""
    user_id = str(m.from_user.id)
    
    try:
        # Получаем заявки клиента
        requests = db.fetch_all("""
            SELECT id, category, district, status, created_at, master_id
            FROM requests 
            WHERE client_user_id = ?
            ORDER BY created_at DESC
            LIMIT 20
        """, (user_id,))
        
        if not requests:
            await m.answer(
                "📝 У вас пока нет заявок.\n\n"
                "Хотите оставить заявку?",
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(text="📝 Оставить заявку", callback_data="go:req")],
                    [InlineKeyboardButton(text="🏠 Главное меню", callback_data="go:menu")]
                ])
            )
            return
        
        # Группируем по статусам
        active = [r for r in requests if r['status'] in ('new', 'assigned', 'pending_confirmation')]
        completed = [r for r in requests if r['status'] == 'completed']
        
        text_lines = ["📝 <b>Мои заявки</b>\n"]
        
        # Активные
        if active:
            text_lines.append(f"🟢 <b>АКТИВНЫЕ ({len(active)}):</b>")
            for req in active[:5]:
                date = datetime.fromisoformat(req['created_at']).strftime('%d.%m.%Y')
                
                status_emoji = {
                    'new': '🆕',
                    'assigned': '👨‍🔧',
                    'pending_confirmation': '⏳'
                }
                
                status_text = {
                    'new': 'Ищем мастера',
                    'assigned': 'Мастер работает',
                    'pending_confirmation': 'Ждём подтверждения'
                }
                
                emoji = status_emoji.get(req['status'], '❓')
                status = status_text.get(req['status'], req['status'])
                
                text_lines.append(
                    f"  {emoji} #{req['id']} | {req['category']} | {date}\n"
                    f"  📍 {req['district']}\n"
                    f"  📊 Статус: {status}"
                )
            
            if len(active) > 5:
                text_lines.append(f"  ... и ещё {len(active) - 5} активных")
            text_lines.append("")
        
        # Завершённые
        if completed:
            text_lines.append(f"✅ <b>ЗАВЕРШЁННЫЕ ({len(completed)}):</b>")
            for req in completed[:5]:
                date = datetime.fromisoformat(req['created_at']).strftime('%d.%m.%Y')
                text_lines.append(f"  ✅ #{req['id']} | {req['category']} | {date}")
            
            if len(completed) > 5:
                text_lines.append(f"  ... и ещё {len(completed) - 5} завершённых")
        
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="📝 Новая заявка", callback_data="go:req")],
            [InlineKeyboardButton(text="🏠 Главное меню", callback_data="go:menu")]
        ])
        
        await m.answer("\n".join(text_lines), reply_markup=kb)
        
    except Exception as e:
        logging.error(f"[MY_REQUESTS_ERROR] {e}")
        await m.answer("❌ Ошибка при получении заявок")

@dp.callback_query(F.data == "master:stats")
async def callback_master_stats(c: CallbackQuery):
    """Статистика мастера (callback)"""
    user_id = str(c.from_user.id)

    data = await get_master_stats_data(user_id)

    if not data:
        await c.answer("❌ Вы не зарегистрированы как мастер", show_alert=True)
        return

    if "error" in data:
        await c.answer(data["error"], show_alert=True)
        return

    await c.message.answer(data["text"])
    await c.answer()

@dp.message(Command("my_orders"))
async def cmd_my_orders(m: Message):
    """История заказов мастера (команда)"""
    user_id = str(m.from_user.id)

    data = await get_master_orders_data(user_id)

    if not data:
        await m.answer("❌ Вы не зарегистрированы как мастер")
        return

    if "error" in data:
        await m.answer(data["error"])
        return

    await m.answer(data["text"])

@dp.callback_query(F.data == "master:orders")
async def callback_master_orders(c: CallbackQuery):
    """История заказов мастера (callback)"""
    user_id = str(c.from_user.id)

    data = await get_master_orders_data(user_id)

    if not data:
        await c.answer("❌ Вы не зарегистрированы как мастер", show_alert=True)
        return

    if "error" in data:
        await c.answer(data["error"], show_alert=True)
        return

    await c.message.answer(data["text"])
    await c.answer()

@dp.message(Command("reviews"))
async def cmd_reviews(m: Message):
    """Показать отзывы на мастера по ID (для админа)"""
    if not is_admin(m.from_user.id):
        await m.answer("❌ Эта команда доступна только администратору")
        return
    
    try:
        # Парсим ID мастера из команды: /reviews 123
        args = m.text.split()
        if len(args) < 2:
            await m.answer("❌ Использование: /reviews <id_мастера>")
            return
        
        master_id = int(args[1])
        
        # Получаем информацию о мастере
        master = db.fetch_one("SELECT fio FROM masters WHERE id = ?", (master_id,))
        if not master:
            await m.answer("❌ Мастер не найден")
            return
        
        # Получаем отзывы
        reviews = db.fetch_all("""
            SELECT r.rating, r.comment, r.created_at, req.id as request_id
            FROM reviews r
            JOIN requests req ON r.request_id = req.id
            WHERE r.master_id = ?
            ORDER BY r.created_at DESC
            LIMIT 20
        """, (master_id,))
        
        if not reviews:
            await m.answer(f"📝 У мастера #{master_id} пока нет отзывов")
            return
        
        review_lines = [f"⭐ <b>Отзывы на {master['fio']} (#{master_id})</b>\n"]
        
        for i, review in enumerate(reviews, 1):
            stars = get_rating_stars(review['rating'])
            date = datetime.fromisoformat(review['created_at']).strftime('%d.%m.%Y')
            
            review_lines.append(f"\n{i}. {stars} <i>({date})</i> - Заявка #{review['request_id']}")
            if review['comment']:
                review_lines.append(f"   💬 {review['comment'][:100]}{'...' if len(review['comment']) > 100 else ''}")
        
        await m.answer("\n".join(review_lines))
        
    except Exception as e:
        logging.error(f"[REVIEWS_ERROR] {e}")
        await m.answer("❌ Ошибка при получении отзывов")

@dp.message(Command("stats"))
async def cmd_stats(m: Message):
    """Статистика сервиса (только для админа)"""
    if not is_admin(m.from_user.id):
        await m.answer("❌ Доступно только администратору")
        return
    
    try:
        stats = db.fetch_one("""
            SELECT 
                (SELECT COUNT(*) FROM masters) as total_masters,
                (SELECT COUNT(*) FROM masters WHERE level = 'Верифицированный') as verified_masters,
                (SELECT COUNT(*) FROM masters WHERE level = 'Проверенный') as checked_masters,
                (SELECT COUNT(*) FROM masters WHERE level = 'Кандидат') as candidate_masters,
                (SELECT COUNT(*) FROM requests) as total_requests,
                (SELECT COUNT(*) FROM requests WHERE status = 'completed') as completed_requests,
                (SELECT COUNT(*) FROM requests WHERE status = 'new') as new_requests,
                (SELECT COUNT(*) FROM reviews) as total_reviews,
                (SELECT COUNT(*) FROM masters WHERE sub_until > datetime('now')) as active_subscriptions
        """)
        
        await m.answer(f"""
📊 <b>Статистика сервиса</b>

👨‍🔧 <b>Мастера:</b>
• Всего: {stats['total_masters']}
• Верифицированные: {stats['verified_masters']}
• Проверенные: {stats['checked_masters']}  
• Кандидаты: {stats['candidate_masters']}
• С активной подпиской: {stats['active_subscriptions']}

📝 <b>Заявки:</b>
• Всего: {stats['total_requests']}
• Новые: {stats['new_requests']}
• Выполнено: {stats['completed_requests']}

⭐ <b>Отзывы:</b>
• Всего: {stats['total_reviews']}
        """)
        
    except Exception as e:
        logging.error(f"[STATS_ERROR] {e}")
        await m.answer("❌ Ошибка получения статистики")

# ----------------- REQUEST FLOW --------
@dp.message(Req.name)
async def req_name(m: Message, state: FSMContext):
    user_id = m.from_user.id
    
    # Лимит: 3 новые заявки в час
    if not rate_limiter.check_limit(user_id, "new_request", 3, 3600):
        remaining = rate_limiter.get_remaining(user_id, "new_request", 3, 3600)
        remaining_time = rate_limiter.get_time_until_reset(user_id, "new_request", 3600)
        
        if remaining_time > 0:
            minutes = (remaining_time % 3600) // 60
            await m.answer(
                f"❌ Лимит заявок исчерпан. Доступно через: {minutes} минут.\n"
                f"💡 Можно создать: {remaining} заявок после сброса лимита"
            )
        else:
            await m.answer(f"❌ Лимит заявок исчерпан. Можно создать: {remaining} заявок")
        return
    
    await state.update_data(name=m.text.strip())
    await m.answer("Оставьте контакт для связи (телефон или @username):", reply_markup=share_phone_kb())
    await state.set_state(Req.contact)

@dp.message(Req.contact, F.contact)
async def req_contact_shared(m: Message, state: FSMContext):
    await state.update_data(contact=m.contact.phone_number)
    
    await m.answer(
        "✅ Контакт сохранён!",
        reply_markup=ReplyKeyboardRemove()
    )
    
    await m.answer(
        "Выберите категорию услуги:", 
        reply_markup=categories_kb()
    )
    await state.set_state(Req.category)

@dp.message(Req.contact)
async def req_contact_text(m: Message, state: FSMContext):
    await state.update_data(contact=m.text.strip())
    await m.answer("Выберите категорию услуги:", reply_markup=categories_kb(), reply_markup_remove=True)
    await state.set_state(Req.category)

@dp.callback_query(Req.category, F.data.startswith("cat:"))
async def req_category(c: CallbackQuery, state: FSMContext):
    code = c.data.split(":")[1]
    title = next((t for t, cc in CATS if cc == code), code)
    await state.update_data(category=title)
    await c.message.answer(
        "📍 Укажите адрес выполнения работ:\n"
        "(например: ул. Ленина, 25 или просто улица и номер дома)"
    )
    await state.set_state(Req.district)
    await c.answer()

@dp.message(Req.district)
async def req_district(m: Message, state: FSMContext):
    address = m.text.strip()
    
    # Минимальная валидация: хотя бы 5 символов
    if len(address) < 5:
        await m.answer(
            "❌ Адрес слишком короткий. Укажите улицу и номер дома.\n"
            "Например: ул. Ленина, 25"
        )
        return
    
    await state.update_data(district=address)
    await m.answer("Коротко опишите задачу:")
    await state.set_state(Req.desc)

@dp.message(Req.desc)
async def req_desc(m: Message, state: FSMContext):
    await state.update_data(description=m.text.strip())
    await m.answer("Когда нужно выполнить? (дата/время, удобные слоты)")
    await state.set_state(Req.when)

@dp.message(Req.when)
async def req_when(m: Message, state: FSMContext):
    await state.update_data(when_text=m.text.strip())
    d = await state.get_data()
    preview = (
        "<b>Проверьте заявку:</b>\n"
        f"👤 {d['name']}\n"
        f"📞 {d['contact']}\n"
        f"📂 {d['category']}\n"
        f"📍 Адрес: {d['district']}\n"
        f"📝 {d['description']}\n"
        f"🗓 {d['when_text']}\n\n"
        "Отправить?"
    )
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ Отправить", callback_data="req:submit"),
         InlineKeyboardButton(text="✏️ Исправить", callback_data="go:req")]
    ])
    await m.answer(preview, reply_markup=kb)
    await state.set_state(Req.preview)

@dp.callback_query(Req.preview, F.data=="req:submit")
async def req_submit(c: CallbackQuery, state: FSMContext):
    d = await state.get_data()
    client_user_id = str(c.from_user.id)
    
    result = db.execute(
        "INSERT INTO requests(name,contact,category,district,description,when_text,status,client_user_id) "
        "VALUES(?,?,?,?,?,?,?,?)",
        (d["name"], d["contact"], d["category"], d["district"], d["description"], d["when_text"], "new", client_user_id)
    )
    rid = result.lastrowid if result else None
    db.commit()

    # уведомление админу
    await notify_admin(
        f"🆕 <b>Заявка #{rid}</b>\n"
        f"👤 {d['name']} | {d['contact']}\n"
        f"📂 {d['category']}\n"
        f"📍 Адрес: {d['district']}\n"
        f"📝 {d['description']}\n"
        f"🗓 {d['when_text']}"
    )

    # рассылка мастерам
    await send_to_masters(rid, d["category"], d["district"])

    await c.message.edit_text(
        "✅ Заявка отправлена. Мы подберём 1–3 мастеров и свяжемся с вами.", 
        reply_markup=main_menu_kb(str(c.from_user.id))
    )
    await state.clear()
    await c.answer()

# ----------------- ANKETA MASTER -------
@dp.callback_query(F.data=="go:master")
async def go_master(c: CallbackQuery, state: FSMContext):
    user_id = c.from_user.id
    
    # Лимит: 3 попытки регистрации мастера за 24 часа
    if not rate_limiter.check_limit(user_id, "master_registration", 3, 86400):
        remaining_time = rate_limiter.get_time_until_reset(user_id, "master_registration", 86400)
        hours = remaining_time // 10800
        
        await c.answer(
            f"❌ Регистрация мастера возможна 3 раза в сутки. Попробуйте через {hours} часов.",
            show_alert=True
        )
        return
    
    await state.clear()
    await c.message.answer("Анкета мастера. Укажите ваши ФИО:")
    await state.set_state(MasterForm.fio)
    await c.answer()

async def cancel_master_registration(m: Message, state: FSMContext):
    """Универсальная отмена регистрации мастера"""
    await state.clear()
    await m.answer(
        "❌ Регистрация мастера отменена.\n"
        "Вы можете начать заново в любое время.",
        reply_markup=ReplyKeyboardRemove()
    )
    await m.answer("Главное меню:", reply_markup=main_menu_kb(str(m.from_user.id)))

@dp.message(MasterForm.fio)
async def mf_fio(m: Message, state: FSMContext):
    if m.text and m.text.strip() == "❌ Отмена":
        await cancel_master_registration(m, state)
        return

    await state.update_data(fio=m.text.strip(), uid=str(m.from_user.id))
    await m.answer(
        "Оставьте номер телефона (кнопкой ниже):",
        reply_markup=ReplyKeyboardMarkup(
            keyboard=[
                [KeyboardButton(text="📱 Отправить номер", request_contact=True)],
                [KeyboardButton(text="❌ Отмена")]
            ],
            resize_keyboard=True, 
            one_time_keyboard=True
        )
    )
    await state.set_state(MasterForm.phone)

def normalize_phone(raw: str) -> str | None:
    """
    Приводим номер к формату +7XXXXXXXXXX
    Принимаем только валидные российские номера: +7 9... или 8 9...
    """
    digits = re.sub(r"\D", "", raw)  # убираем всё, кроме цифр
    if digits.startswith("7") and len(digits) == 11:  # +7XXXXXXXXXX → уже норм
        return "+7" + digits[1:]
    elif digits.startswith("8") and len(digits) == 11:  # 8XXXXXXXXXX → конвертируем
        return "+7" + digits[1:]
    elif digits.startswith("9") and len(digits) == 10:  # 9XXXXXXXXX → добавим +7
        return "+7" + digits
    return None

@dp.message(MasterForm.phone, F.contact)
async def mf_phone_contact(m: Message, state: FSMContext):
    normalized = normalize_phone(m.contact.phone_number)
    if not normalized:
        await m.answer("❌ Номер из Telegram невалидный. Введите вручную в формате +7 9XXXXXXXXX или 8 9XXXXXXXXX.")
        return
    
    await state.update_data(phone=normalized, sel_cats=[])
    
    await m.answer(
        "✅ Номер сохранён!",
        reply_markup=ReplyKeyboardRemove()
    )
    
    await m.answer(
        "Выберите 1–2 категории (нажимайте, чтобы отмечать/снимать чек):",
        reply_markup=build_cats_kb([])
    )
    
    await state.set_state(MasterForm.categories)

@dp.message(MasterForm.phone, F.text)
async def mf_phone_text(m: Message, state: FSMContext):
    if m.text and m.text.strip() == "❌ Отмена":
        await cancel_master_registration(m, state)
        return
    
    normalized = normalize_phone(m.text.strip())
    if normalized:
        await state.update_data(phone=normalized, sel_cats=[])
        
        await m.answer(
            "✅ Номер сохранён!",
            reply_markup=ReplyKeyboardRemove()
        )
        
        await m.answer(
            "Выберите 1–2 категории (нажимайте, чтобы отмечать/снимать чек):",
            reply_markup=build_cats_kb([])
        )
        
        await state.set_state(MasterForm.categories)
    else:
        await m.answer(
            "❌ Неверный формат номера.\n\n"
            "Допустимые форматы:\n"
            "• +7 9XXXXXXXXX\n"
            "• 8 9XXXXXXXXX\n\n"
            "Попробуйте снова или нажмите кнопку «📱 Отправить номер»."
        )

@dp.callback_query(MasterForm.categories, F.data.startswith("mcat:toggle:"))
async def mcat_toggle(c: CallbackQuery, state: FSMContext):
    title = c.data.split("mcat:toggle:", 1)[1]
    d = await state.get_data()
    sel = d.get("sel_cats", [])
    if title in sel:
        sel.remove(title)
    else:
        if len(sel) >= 2:
            await c.answer("Можно выбрать не более двух категорий.", show_alert=True)
        else:
            sel.append(title)
    await state.update_data(sel_cats=sel)
    await c.message.edit_reply_markup(reply_markup=build_cats_kb(sel))
    await c.answer()

@dp.callback_query(MasterForm.categories, F.data == "mcat:done")
async def mcat_done(c: CallbackQuery, state: FSMContext):
    d = await state.get_data()
    sel = d.get("sel_cats", [])
    if not (1 <= len(sel) <= 2):
        await c.answer("Выберите 1–2 категории.", show_alert=True)
        return
    # сохраняем выбранное в categories_auto (строкой вида "Ремонт, Уборка")
    cats_str = ", ".join(sel)
    await state.update_data(categories_auto=cats_str)
    await c.message.edit_text(
        f"Категории: {cats_str}\nТеперь укажите ваш опыт по годам:",
        reply_markup=exp_bucket_kb()
    )
    await state.set_state(MasterForm.exp_bucket)
    await c.answer()

@dp.callback_query(MasterForm.exp_bucket, F.data.startswith("exp:"))
async def mf_exp_bucket(c: CallbackQuery, state: FSMContext):
    bucket = c.data.split(":")[1]
    mapping = {"<=1":"до 1 года","1-3":"1–3 года","3-5":"3–5 лет","5-10":"5–10 лет",">10":"более 10 лет"}
    await state.update_data(exp_bucket=mapping.get(bucket, bucket))
    await c.message.answer("Опишите кратко опыт и навыки (1–3 предложения):")
    await state.set_state(MasterForm.exp_text)
    await c.answer()

@dp.message(MasterForm.exp_text)
async def mf_exp_text(m: Message, state: FSMContext):
    if m.text and m.text.strip() == "❌ Отмена":
        await cancel_master_registration(m, state)
        return
    
    await state.update_data(exp_text=m.text.strip())
    await m.answer(
        "Портфолио: пришлите фото/ссылки на ваши работы или напишите «нет»:",
        reply_markup=ReplyKeyboardMarkup(
            keyboard=[[KeyboardButton(text="❌ Отмена")]],
            resize_keyboard=True,
            one_time_keyboard=False
        )
    )
    await state.set_state(MasterForm.portfolio)

@dp.message(MasterForm.portfolio)
async def mf_portfolio(m: Message, state: FSMContext):
    if m.text and m.text.strip() == "❌ Отмена":
        await cancel_master_registration(m, state)
        return

    await state.update_data(portfolio=m.text.strip())

    await m.answer(
        "Укажите контакты 2–3 клиентов для рекомендаций (или напишите «нет»):",
        reply_markup=ReplyKeyboardMarkup(
            keyboard=[[KeyboardButton(text="❌ Отмена")]],
            resize_keyboard=True,
            one_time_keyboard=False
        )
    )
    await state.set_state(MasterForm.references)

@dp.message(MasterForm.references)
async def mf_references(m: Message, state: FSMContext):
    if m.text and m.text.strip() == "❌ Отмена":
        await cancel_master_registration(m, state)
        return

    await state.update_data(references=m.text.strip())

    d = await state.get_data()
    # НЕ авто-категоризируем — оставляем выбранные пользователем
    cats_selected = d.get("categories_auto", "")  # сюда мы сохранили строку "Ремонт, Уборка" в mcat_done
    await state.update_data(categories_auto=cats_selected)

    # Присваиваем базовый статус
    await state.update_data(level="Кандидат", verified=0, has_npd_ip=0)

    # Предложение пройти проверку
    kb = InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text="Да, пройти проверку", callback_data="mf:verify:yes"),
        InlineKeyboardButton(text="Нет, оставить Кандидатом", callback_data="mf:verify:no")
    ]])
    await m.answer(
        f"Категория/Категории: {cats_selected or '—'}\n"
        "Сейчас ваш статус: <b>Кандидат</b>.\n"
        "Хотите пройти дополнительную проверку документов и получить статус <b>Проверенный</b>?",
        reply_markup=kb
    )
    await state.set_state(MasterForm.verify_offer)

@dp.callback_query(MasterForm.verify_offer, F.data == "mf:verify:no")
async def mf_verify_no(c: CallbackQuery, state: FSMContext):
    d = await state.get_data()
    cats_auto = d.get("categories_auto","")
    skill_tier = "Новичок"  # 0 выполненных заказов
    result = db.execute("""
        INSERT INTO masters(fio,contact,phone,exp_bucket,exp_text,portfolio,references,
                            level,verified,has_npd_ip,categories_auto,orders_completed,skill_tier,
                            free_orders_left,is_active)
        VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,1)
    """, (d["fio"], d["uid"], d.get("phone",""), d.get("exp_bucket",""), d.get("exp_text",""),
            d.get("portfolio",""), d.get("references",""), "Кандидат", 0, 0, cats_auto, 0, skill_tier, FREE_ORDERS_START))
    mid = result.lastrowid if result else None
    db.commit()

    await notify_admin(admin_master_card(mid))

    await c.message.edit_text(
        "✅ Анкета сохранена. Статус: Кандидат.", 
        reply_markup=main_menu_kb(str(c.from_user.id))
    )
    
    # ✅ УБИРАЕМ REPLY-КЛАВИАТУРУ:
    await c.message.answer(
        "🎉 Поздравляем! Теперь вы можете получать заказы!",
        reply_markup=ReplyKeyboardRemove()
    )
    
    await state.clear()
    await c.answer()

@dp.callback_query(MasterForm.verify_offer, F.data == "mf:verify:yes")
async def mf_verify_yes(c: CallbackQuery, state: FSMContext):
    # Текст заявления
    statement = (
        "📄 <b>Заявление о добровольном предоставлении данных для верификации</b>\n\n"
        "Я добровольно предоставляю следующие сведения и документы для разовой проверки подлинности в рамках получения статуса в сервисе «Мастера Верхней Пышмы»:\n"
        "• Реквизиты паспорта (серия, номер, кем выдан, дата выдачи);\n"
        "• Скан паспорта (для статуса «Проверенный»);\n"
        "• ИНН и документ о статусе НПД/ИП (для статуса «Верифицированный»).\n\n"
        "<b>Я понимаю и подтверждаю следующее:</b>\n"
        "1. Данные предоставляются исключительно для разовой верификации и не будут сохранены в базе данных после проверки.\n"
        "2. Все сканы документов будут безвозвратно удалены в течение 72 часов с момента завершения проверки.\n"
        "3. Администрация не является оператором персональных данных в смысле Федерального закона №152-ФЗ, поскольку не осуществляет автоматизированную обработку и не создает информационную систему ПДн.\n"
        "4. В канале будет опубликована только анонимизированная информация (имя без фамилии, фото мастера, категория, контакт).\n"
        "5. Я вправе отозвать своё заявление в любой момент.\n"
        "6. Я ознакомлен с [Политикой конфиденциальности] и [Пользовательским соглашением] сервиса (https://disk.yandex.ru/d/1mlvS2VtcJTiXg).\n\n"
        "✅ Нажимая кнопку ниже, я подтверждаю, что:\n"
        "• Достиг 18 лет;\n"
        "• Предоставляю данные добровольно;\n"
        "• Понимаю условия временного предоставления документов.\n\n"
    )
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ Я согласен", callback_data="consent:given")]
    ])
    await c.message.edit_text(statement, reply_markup=kb, disable_web_page_preview=True)
    await state.set_state(MasterForm.consent)
    await c.answer()

@dp.callback_query(MasterForm.consent, F.data == "consent:given")
async def consent_given(c: CallbackQuery, state: FSMContext):
    await c.message.edit_text(
        "Отлично! Теперь введите паспортные данные в одной строке:\n"
        "«серия и номер; кем выдан; дата выдачи; дата рождения»."
    )
    await state.set_state(MasterForm.passport_info)
    await c.answer()

@dp.message(MasterForm.passport_info)
async def mf_passport_info(m: Message, state: FSMContext):
    if m.text and m.text.strip() == "❌ Отмена":
        await cancel_master_registration(m, state)
        return
    
    # Текст паспорта НЕ сохраняем — только для визуальной сверки админом
    await m.answer(
        "Прикрепите <b>скан паспорта</b> (фото документа):",
        reply_markup=ReplyKeyboardMarkup(
            keyboard=[[KeyboardButton(text="❌ Отмена")]],
            resize_keyboard=True,
            one_time_keyboard=False
        )
    )
    await state.set_state(MasterForm.passport_scan)

@dp.message(MasterForm.passport_scan, F.photo)
async def mf_passport_scan(m: Message, state: FSMContext):
    await state.update_data(passport_scan_file_id=m.photo[-1].file_id)
    await m.answer(
        "Прикрепите <b>ваше фото</b> (используем для карточки мастера):",
        reply_markup=ReplyKeyboardMarkup(
            keyboard=[[KeyboardButton(text="❌ Отмена")]],
            resize_keyboard=True,
            one_time_keyboard=False
        )
    )
    await state.set_state(MasterForm.face_photo)

# обработчик текста (если пришлёт не фото):
@dp.message(MasterForm.passport_scan)
async def mf_passport_scan_invalid(m: Message, state: FSMContext):
    if m.text and m.text.strip() == "❌ Отмена":
        await cancel_master_registration(m, state)
        return
    
    await m.answer("❌ Пожалуйста, прикрепите ФОТО документа (не текст).")

@dp.message(MasterForm.face_photo, F.photo)
async def mf_face_photo(m: Message, state: FSMContext):
    await state.update_data(face_photo_file_id=m.photo[-1].file_id)

    # теперь мастер — Проверенный
    d = await state.get_data()
    cats_auto = d.get("categories_auto","")
    skill_tier = calc_skill_tier(0)
    result = db.execute("""
      INSERT INTO masters(fio, contact, phone, exp_bucket, exp_text, portfolio, references,
                          level, verified, has_npd_ip, passport_scan_file_id, face_photo_file_id,
                          categories_auto, orders_completed, skill_tier, free_orders_left, is_active)
      VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,1)
    """, (
        d["fio"], d["uid"], d.get("phone",""), d.get("exp_bucket",""), d.get("exp_text",""),
        d.get("portfolio",""), d.get("references",""), "Проверенный", 1, 0,
        d.get("passport_scan_file_id",""), d.get("face_photo_file_id",""),
        cats_auto, 0, skill_tier, FREE_ORDERS_START
    ))
    mid = result.lastrowid if result else None
    db.commit()

    await notify_admin(admin_master_card(mid))

    # предложим апгрейд до Верифицированного (НПД/ИП)
    kb = InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text="Да, у меня НПД/ИП", callback_data=f"mf:npd:yes:{mid}"),
        InlineKeyboardButton(text="Нет", callback_data=f"mf:npd:no:{mid}")
    ]])
    
    await m.answer(
        "Статус обновлён: <b>Проверенный</b>.\n"
        "Вы зарегистрированы как самозанятый или ИП?\n"
        "Если да — можно получить статус <b>Верифицированный</b>.", 
        reply_markup=kb
    )
    
    await m.answer(
        "Выберите вариант кнопками выше 👆",
        reply_markup=ReplyKeyboardRemove()
    )
    
    await state.set_state(MasterForm.npd_offer)

@dp.message(MasterForm.face_photo)
async def mf_face_photo_invalid(m: Message, state: FSMContext):
    if m.text and m.text.strip() == "❌ Отмена":
        await cancel_master_registration(m, state)
        return
    
    await m.answer("❌ Пожалуйста, прикрепите ФОТО (не текст).")

@dp.callback_query(MasterForm.npd_offer, F.data.startswith("mf:npd:"))
async def mf_npd_offer(c: CallbackQuery, state: FSMContext):
    _, _, ans, mid = c.data.split(":")
    mid = int(mid)
    
    if ans == "no":
        await notify_admin(admin_master_card(mid))
        
        await c.message.edit_text(
            "✅ Анкета сохранена. Статус: Проверенный.", 
            reply_markup=main_menu_kb(str(c.from_user.id))
        )
        
        await c.message.answer(
            "🎉 Поздравляем! Теперь вы можете получать заказы!",
            reply_markup=ReplyKeyboardRemove()
        )
        
        await state.clear()
        await c.answer()
        return

    # “yes” — продолжаем к ИНН
    await state.update_data(current_mid=mid)
    await c.message.edit_text("Введите ваш ИНН (10 или 12 цифр):")
    await state.set_state(MasterForm.inn_cert)
    await c.answer()

@dp.message(MasterForm.inn_cert)
async def mf_inn_cert(m: Message, state: FSMContext):
    if m.text and m.text.strip() == "❌ Отмена":
        await cancel_master_registration(m, state)
        return
    
    inn = m.text.strip()
    if not inn.isdigit() or len(inn) not in (10, 12):
        await m.answer(
            "❌ ИНН некорректный. Введите 10 или 12 цифр:",
            reply_markup=ReplyKeyboardMarkup(
                keyboard=[[KeyboardButton(text="❌ Отмена")]],
                resize_keyboard=True,
                one_time_keyboard=False
            )
        )
        return
    
    await state.update_data(inn_cert=inn)
    await m.answer(
        "Прикрепите документ, подтверждающий самозанятость/ИП (фото/скан):",
        reply_markup=ReplyKeyboardMarkup(
            keyboard=[[KeyboardButton(text="❌ Отмена")]],
            resize_keyboard=True,
            one_time_keyboard=False
        )
    )
    await state.set_state(MasterForm.npd_doc)

@dp.message(MasterForm.npd_doc, F.photo)
async def mf_npd_doc(m: Message, state: FSMContext):
    d = await state.get_data()
    mid = d.get("current_mid")
    file_id = m.photo[-1].file_id
    inn = d.get("inn_cert","")
    
    db.execute("""
        UPDATE masters
        SET has_npd_ip=1,
            level='Верифицированный',
            npd_ip_doc_file_id=?,
            inn=?
        WHERE id=?
    """, (file_id, inn, mid))

    await notify_admin(admin_master_card(mid))

    await m.answer(
        "✅ Анкета сохранена. Статус: Верифицированный.", 
        reply_markup=main_menu_kb(str(m.from_user.id))
    )
    
    await m.answer(
        "🎉 Поздравляем! Теперь вы можете получать заказы!",
        reply_markup=ReplyKeyboardRemove()
    )
    
    await state.clear()

@dp.message(MasterForm.npd_doc)
async def mf_npd_doc_invalid(m: Message, state: FSMContext):
    if m.text and m.text.strip() == "❌ Отмена":
        await cancel_master_registration(m, state)
        return
    
    await m.answer("❌ Пожалуйста, прикрепите ФОТО документа (не текст).")

# ----------------- BILLING -------------
@dp.callback_query(F.data=="pay:sub")
async def pay_sub(c: CallbackQuery):
    if not PAY_PROVIDER_TOKEN:
        await c.answer("Платёжный провайдер не настроен.", show_alert=True); return
    prices = [LabeledPrice(label="Подписка (30 дней)", amount=SUB_PRICE_RUB)]
    await bot.send_invoice(
        chat_id=c.from_user.id,
        title="Подписка",
        description="Безлимит заказов (30 дней)",
        payload="sub_30d",
        provider_token=PAY_PROVIDER_TOKEN,
        currency="RUB",
        prices=prices
    )
    await c.answer()

@dp.callback_query(F.data=="pay:priority")
async def pay_priority(c: CallbackQuery):
    if not PAY_PROVIDER_TOKEN:
        await c.answer("Платёжный провайдер не настроен.", show_alert=True); return
    prices = [LabeledPrice(label="Приоритет (30 дней)", amount=PRIORITY_PRICE_RUB)]
    await bot.send_invoice(
        chat_id=c.from_user.id,
        title="Приоритет заказов",
        description="Ранний доступ к рассылкам (30 дней)",
        payload="priority_30d",
        provider_token=PAY_PROVIDER_TOKEN,
        currency="RUB",
        prices=prices
    )
    await c.answer()

@dp.callback_query(F.data=="pay:pin")
async def pay_pin(c: CallbackQuery):
    if not PAY_PROVIDER_TOKEN:
        await c.answer("Платёжный провайдер не настроен.", show_alert=True); return
    prices = [LabeledPrice(label="Закреп (7 дней)", amount=PIN_PRICE_RUB)]
    await bot.send_invoice(
        chat_id=c.from_user.id,
        title="Закреп анкеты",
        description="Выше видимость в канале (7 дней)",
        payload="pin_7d",
        provider_token=PAY_PROVIDER_TOKEN,
        currency="RUB",
        prices=prices
    )
    await c.answer()

@dp.pre_checkout_query()
async def checkout(pre: PreCheckoutQuery):
    await bot.answer_pre_checkout_query(pre.id, ok=True)

@dp.message(F.successful_payment)
async def payment_done(m: Message):
    payload = m.successful_payment.invoice_payload
    uid = str(m.from_user.id)
    if payload == "sub_30d":
        until = (datetime.utcnow() + timedelta(days=SUB_DURATION_DAYS)).strftime("%Y-%m-%d %H:%M:%S")
        db.execute("UPDATE masters SET sub_until=? WHERE contact=?", (until, uid))
        await m.answer("✅ Подписка активна на 30 дней.")
    elif payload == "priority_30d":
        until = (datetime.utcnow() + timedelta(days=PRIORITY_DURATION_DAYS)).strftime("%Y-%m-%d %H:%M:%S")
        db.execute("UPDATE masters SET priority_until=? WHERE contact=?", (until, uid))
        await m.answer("✅ Приоритет включён на 30 дней.")
    elif payload == "pin_7d":
        until = (datetime.utcnow() + timedelta(days=PIN_DURATION_DAYS)).strftime("%Y-%m-%d %H:%M:%S")
        db.execute("UPDATE masters SET pin_until=? WHERE contact=?", (until, uid))
        await m.answer("✅ Анкета закреплена на 7 дней.")

# ----------------- MATCHING ------------
async def send_to_masters(request_id: int, category: str, district: str):
    # выбираем активных мастеров, у кого авто-категории подходят
    rows = db.fetch_all("""
      SELECT id, fio, contact, level, priority_until, sub_until, categories_auto
      FROM masters
      WHERE is_active=1
    """)

    # отфильтруем по категории заявки (простое вхождение)
    def cat_match(cats_auto: str, request_category: str) -> bool:
        if not cats_auto:
            return False  # Мастер без категорий НЕ получает заказы
        
        # Нормализуем категорию заявки (убираем эмодзи и лишнее)
        clean_request = re.sub(r"[^а-яА-Я]", "", request_category).strip().lower()
        
        # Нормализуем категории мастера
        master_cats = []
        for cat in cats_auto.split(","):
            # Убираем подкатегории (например "Ремонт/электрика" -> "ремонт")
            main_cat = cat.split("/")[0].strip()
            clean_cat = re.sub(r"[^а-яА-Я]", "", main_cat).strip().lower()
            if clean_cat:
                master_cats.append(clean_cat)
        
        # Проверяем вхождение (гибкое совпадение)
        return any(clean_request in mc or mc in clean_request for mc in master_cats)

    rows = [r for r in rows if cat_match(r[6], category)]

    # сортировка: приоритет -> подписка -> уровень
    def sort_key(r):
        _id, _fio, _contact, _level, pr_until, sub_until, _cats = r
        pr = 1 if is_active(pr_until) else 0
        sub = 1 if is_active(sub_until) else 0
        lvl_rank = {"ТОП":3, "Верифицированный":2, "Проверенный":1, "Кандидат":0}.get(_level,0)
        return (-pr, -sub, -lvl_rank)

    rows = sorted(rows, key=sort_key)[:5]

    # Получаем полную информацию о заявке
    request_data = db.fetch_one("""
        SELECT name, description, when_text 
        FROM requests 
        WHERE id = ?
    """, (request_id,))

    if request_data:
        text = (
            f"🆕 <b>Новая заявка #{request_id}</b>\n\n"
            f"📂 Категория: {category}\n"
            f"📍 Адрес: {district}\n"
            f"📝 Описание: {request_data['description']}\n"
            f"🗓 Когда: {request_data['when_text']}\n\n"
            f"❗️ Контакты клиента будут отправлены после согласия."
        )
    else:
        text = (f"🆕 Заявка #{request_id}\n"
                f"Категория: {category}\n"
                f"Адрес: {district}")

    for mid, fio, contact, level, _, _, _ in rows:
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [
                InlineKeyboardButton(text="✅ Беру", callback_data=f"offer:take:{request_id}:{mid}"),
                InlineKeyboardButton(text="⏭ Пропустить", callback_data=f"offer:skip:{request_id}:{mid}")
            ]
        ])
        db.execute("INSERT INTO offers(request_id, master_id, status) VALUES(?,?, 'sent')", (request_id, mid))
        db.commit()
        try:
            chat_id = int(contact)  # в анкете мы сохранили user_id мастера в contact
        except:
            chat_id = ADMIN_CHAT_ID  # на всякий случай
        try:
            await bot.send_message(chat_id, text, reply_markup=kb)
        except Exception as e:
            error_msg = str(e).lower()
            if "blocked" in error_msg or "bot was blocked" in error_msg or "user is deactivated" in error_msg:
                # Деактивируем мастера, если он заблокировал бота
                db.execute("UPDATE masters SET is_active = 0 WHERE id = ?", (mid,))
                logging.info(f"[MASTER_DEACTIVATED] Master #{mid} blocked the bot")
            else:
                logging.warning(f"[MASTER_NOTIFY_ERROR] Master #{mid}: {e}")

# ----------------- TAKE ORDER ----------
@dp.callback_query(F.data.startswith("offer:"))
async def offer_actions(c: CallbackQuery):
    user_id = c.from_user.id
    
    # Лимит: 10 действий с заказами в час (взять/пропустить)
    if not rate_limiter.check_limit(user_id, "offer_actions", 10, 3600):
        await c.answer("❌ Слишком много действий. Подождите немного.", show_alert=True)
        return
    
    _, action, req_id, master_id = c.data.split(":")
    req_id, master_id = int(req_id), int(master_id)
    row = db.fetch_one("SELECT status, name, contact FROM requests WHERE id=?", (req_id,))
    if not row:
        await c.answer("Заказ не найден", show_alert=True)
        return
    status, client_name, client_contact = row['status'], row['name'], row['contact']

    if action == "skip":
        db.execute("UPDATE offers SET status='skipped' WHERE request_id=? AND master_id=?", (req_id, master_id))
        await c.answer("Пропущено"); return

    if action == "take":
        # Проверяем, что callback нажал именно тот мастер, которому пришло уведомление
        current_master = db.fetch_one("SELECT id FROM masters WHERE contact = ?", (str(c.from_user.id),))
        if not current_master or current_master['id'] != master_id:
            await c.answer("❌ Ошибка авторизации", show_alert=True)
            return
        
        if status != "new":
            await c.answer("Заказ уже взят другим мастером", show_alert=True)
            return

        m = db.fetch_one("SELECT sub_until, free_orders_left FROM masters WHERE id=?", (master_id,))
        if not m:
            await c.answer("Мастер не найден", show_alert=True)
            return
        sub_until, free_left = m['sub_until'], m['free_orders_left']

        allowed = False
        if is_active(sub_until):
            allowed = True
        elif (free_left or 0) > 0:
            allowed = True
            db.execute("UPDATE masters SET free_orders_left=free_orders_left-1 WHERE id=?", (master_id,))
        else:
            kb = InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="💳 Оформить подписку (990 ₽/мес)", callback_data="pay:sub")]
            ])
            await c.message.reply("❌ У вас закончились 3 бесплатных заказа. Оформите подписку, чтобы брать заказы без ограничений.", reply_markup=kb)
            await c.answer(); return

        db.execute("UPDATE requests SET status='assigned', master_id=? WHERE id=?", (master_id, req_id))

        # Редактируем исходное сообщение
        await c.message.edit_text("✅ Заказ закреплён за вами!")

        # Получаем полную информацию о заявке для отправки мастеру
        request_full = db.fetch_one("""
            SELECT name, contact, description, when_text, district, client_user_id 
            FROM requests 
            WHERE id = ?
        """, (req_id,))

        if request_full:
            # Уведомляем клиента
            if request_full['client_user_id']:
                master_info = db.fetch_one("SELECT fio, phone FROM masters WHERE id = ?", (master_id,))
                if master_info:
                    master_name = master_info['fio'] or "Мастер"
                    master_phone = master_info['phone'] or "не указан"
                    try:
                        await bot.send_message(
                            int(request_full['client_user_id']),
                            f"✅ <b>Ваш заказ #{req_id} взят в работу!</b>\n\n"
                            f"👨‍🔧 Мастер: {master_name}\n"
                            f"📞 Телефон: {master_phone}\n\n"
                            f"Мастер свяжется с вами в ближайшее время."
                        )
                    except Exception as e:
                        logging.warning(f"[CLIENT_NOTIFY_ERROR] {e}")
            
            # Отправляем детали заказа мастеру
            kb = InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="✅ Завершить заказ", callback_data=f"complete:{req_id}")]
            ])
            
            await bot.send_message(
                c.from_user.id,
                f"📋 <b>Детали заказа #{req_id}</b>\n\n"
                f"👤 Клиент: {request_full['name']}\n"
                f"📞 Контакт: {request_full['contact']}\n"
                f"📍 Адрес: {request_full['district']}\n"
                f"📝 Описание: {request_full['description']}\n"
                f"🗓 Когда: {request_full['when_text']}\n\n"
                f"💬 Свяжитесь с клиентом для уточнения деталей.\n"
                f"После выполнения работ нажмите кнопку ниже:",
                reply_markup=kb
            )

        db.execute("UPDATE offers SET status='taken' WHERE request_id=? AND master_id=?", (req_id, master_id))
        await notify_admin(f"🔗 Заказ #{req_id} взят мастером #{master_id}. Клиент: {client_name} | {client_contact}")
        await c.answer()

# ----------------- COMPLAINT FLOW ------
@dp.message(Complaint.who)
async def comp_who(m: Message, state: FSMContext):
    await state.update_data(who=m.text.strip())
    await m.answer("ID заказа (если знаете) или «нет»:")
    await state.set_state(Complaint.order_id)

@dp.message(Complaint.order_id)
async def comp_order(m: Message, state: FSMContext):
    await state.update_data(order_id=m.text.strip())
    await m.answer("ID мастера (если знаете) или «нет»:")
    await state.set_state(Complaint.master_id)

@dp.message(Complaint.master_id)
async def comp_master(m: Message, state: FSMContext):
    await state.update_data(master_id=m.text.strip())
    await m.answer("Опишите проблему коротко:")
    await state.set_state(Complaint.text)

@dp.message(Complaint.text)
async def comp_text(m: Message, state: FSMContext):
    """Обработка текстового отзыва или жалобы"""
    try:
        data = await state.get_data()
        request_id = data.get('review_request_id')
        
        if not request_id:
            # Это не отзыв, а обычная жалоба - обрабатываем как раньше
            d = await state.get_data()
            db.execute("INSERT INTO complaints(who,order_id,master_id,text) VALUES(?,?,?,?)",
                      (d["who"], d["order_id"], d["master_id"], m.text.strip()))
            db.commit()
            await notify_admin(f"🚨 Жалоба: {json.dumps(d, ensure_ascii=False)}")
            await m.answer("✅ Жалоба отправлена. Мы свяжемся с вами.", reply_markup=main_menu_kb(str(c.from_user.id)))
            await state.clear()
            return
        
        # Это текстовый отзыв - сохраняем
        request = db.fetch_one("SELECT master_id FROM requests WHERE id = ?", (request_id,))
        if not request:
            await m.answer("❌ Заявка не найдена")
            await state.clear()
            return
        
        # Обновляем отзыв текстом
        db.execute("""
            UPDATE reviews 
            SET comment = ?
            WHERE request_id = ?
        """, (m.text.strip(), request_id))
        
        await m.answer(
            "✅ Спасибо за развернутый отзыв! Он очень важен для нашего сообщества.",
            reply_markup=main_menu_kb(str(c.from_user.id))
        )
        
        await state.clear()
        
    except Exception as e:
        logging.error(f"[REVIEW_TEXT_PROCESS_ERROR] {e}")
        await m.answer("❌ Ошибка при сохранении отзыва")
        await state.clear()

# ----------------- BILLING MENU --------
@dp.callback_query(F.data=="go:billing")
async def go_billing(c: CallbackQuery, state: FSMContext):
    await c.message.answer(
        "<b>Подписка и услуги для мастеров</b>\n\n"
        "🔹 Новым мастерам: 3 заказа бесплатно\n"
        "🔹 Далее подписка: 990 ₽/мес (безлимит)\n\n"
        "Доп.услуги:\n"
        "⚡ Приоритет заказов — 490 ₽/мес\n"
        "📌 Закреп анкеты — 190 ₽/нед\n",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="💳 Подписка 990 ₽", callback_data="pay:sub")],
            [InlineKeyboardButton(text="⚡ Приоритет 490 ₽", callback_data="pay:priority")],
            [InlineKeyboardButton(text="📌 Закреп 190 ₽", callback_data="pay:pin")],
            [InlineKeyboardButton(text="🏠 Меню", callback_data="go:menu")]
        ])
    )
    await c.answer()

# ----------------- MAIN ----------------
async def periodic_cleanup():
    """
    Запускает очистку документов по расписанию:
    - Каждые 24 часа - полная очистка
    - Каждые 6 часов - быстрая проверка
    """
    logging.info("[PERIODIC_CLEANUP] Started periodic cleanup service")
    cleanup_counter = 0  # Счетчик для rate limiter
    
    full_cleanup_interval = 24 * 3600  # 24 часа
    quick_check_interval = 6 * 3600    # 6 часов
    
    while True:
        try:
            # Автозавершение заказов, висящих в "pending_confirmation" больше 24 часов
            try:
                pending_requests = db.fetch_all("""
                    SELECT id, master_id, client_user_id
                    FROM requests 
                    WHERE status = 'pending_confirmation'
                      AND datetime(created_at) < datetime('now', '-24 hours')
                """)
            
                for req in pending_requests:
                    request_id = req['id']
                
                    # Завершаем заказ
                    await mark_request_completed(request_id)
                
                    # Уведомляем клиента
                    if req['client_user_id']:
                        try:
                            await bot.send_message(
                                int(req['client_user_id']),
                                f"⏰ Заказ #{request_id} автоматически завершён через 24 часа.\n"
                                f"Пожалуйста, оцените работу мастера:"
                            )
                        except Exception as e:
                            logging.error(f"[AUTO_COMPLETE_NOTIFY_ERROR] {e}")
                
                    logging.info(f"[AUTO_COMPLETE] Request #{request_id} auto-completed after 24h")
            
                if pending_requests:
                    logging.info(f"[AUTO_COMPLETE] Completed {len(pending_requests)} pending requests")
                
            except Exception as e:
                logging.error(f"[AUTO_COMPLETE_ERROR] {e}")

            # Полная очистка каждые 24 часа
            await safe_cleanup_documents()
            logging.info(f"[PERIODIC_CLEANUP] Full cleanup completed. Next in {full_cleanup_interval/3600} hours")
            
            # Ждем 24 часа до следующей полной очистки
            # Но каждые 6 часов делаем быструю проверку и логируем статус
            for i in range(4):  # 24 / 6 = 4 интервала
                await asyncio.sleep(quick_check_interval)
                
                # Быстрая проверка: просто логируем статус
                pending_count = db.fetch_one("""
                    SELECT COUNT(*) as count 
                    FROM masters 
                    WHERE created_at < datetime('now', '-72 hours')
                      AND (passport_scan_file_id IS NOT NULL 
                           OR face_photo_file_id IS NOT NULL 
                           OR npd_ip_doc_file_id IS NOT NULL)
                """)['count']
                
                if pending_count > 0:
                    logging.info(f"[CLEANUP_STATUS] Documents pending cleanup: {pending_count}")
                else:
                    logging.info("[CLEANUP_STATUS] No documents pending cleanup")
                
                # Каждые 6 часов чистим rate limiter
                cleanup_counter += 1
                if cleanup_counter >= 4:  # 4 * 6 часов = 24 часа
                    rate_limiter.cleanup_old_entries()
                    cleanup_counter = 0
                    
        except Exception as e:
            logging.error(f"[PERIODIC_CLEANUP_ERROR] {e}")
            
            # В случае ошибки ждем 1 час и пробуем снова
            await asyncio.sleep(3600)

async def main():
    # Снимаем вебхук
    try:
        await bot.delete_webhook(drop_pending_updates=True)
        logging.info("[BOT] Webhook deleted")
    except Exception as e:
        logging.warning(f"[DEL_WEBHOOK] {e}")

    # Уведомление админа
    if ADMIN_CHAT_ID:
        try:
            await bot.send_message(ADMIN_CHAT_ID, "✅ Бот запущен (v3, с улучшенной безопасностью)")
        except Exception as e:
            logging.error(f"[ADMIN_NOTIFY_ERROR] {e}")

    # Запускаем фоновую очистку
    asyncio.create_task(periodic_cleanup())
    logging.info("[BOT] Background tasks started")

    # Запускаем бота
    logging.info("[BOT] Starting polling...")
    try:
        await dp.start_polling(bot)
    except Exception as e:
        logging.error(f"[BOT_ERROR] {e}")
    finally:
        db.close()
        logging.info("[BOT] Stopped")

if __name__ == "__main__":
    asyncio.run(main())