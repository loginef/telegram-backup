# 2_forward_by_id.py
from telethon import TelegramClient
import sqlite3
import asyncio
import time
import logging
from datetime import datetime

# === CHANGE THESE ===
api_id = 1234567
api_hash = 'TODO'
phone = '+TODO'
source_username = 'TODO'
target_channel = -TODO

# Настройки
BATCH_SIZE = 50        # сколько сообщений обрабатывать за раз (не путать с чанками)
DELAY = 0.5            # задержка между сообщениями
MAX_RETRIES = 10       # макс. число попыток
INITIAL_DELAY = 1      # начальная задержка (сек)
MAX_DELAY = 30         # макс. задержка при экспоненциальном росте
SKIP_SERVICE = True    # True = пропускать служебные (ChatAction и т.д.)
# ==========================

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(message)s',
    handlers=[
        logging.FileHandler('forward.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Подключаемся к БД
conn = sqlite3.connect('archive.db', check_same_thread=False)
cursor = conn.cursor()

client = TelegramClient('session_archiver', api_id, api_hash)

async def exponential_backoff(attempt):
    delay = min(INITIAL_DELAY * (2 ** attempt), MAX_DELAY)
    jitter = delay * 0.1
    await asyncio.sleep(delay + jitter)

async def send_with_retry(func, msg_id, action="operation"):
    """
    func: функция, возвращающая корутину (например: lambda: client.send_message(...))
    """
    for attempt in range(MAX_RETRIES):
        try:
            # Создаём новую корутину на каждой попытке
            coroutine = func()
            result = await coroutine
            logger.info(f"✅ Успешно: {action} (msg_id={msg_id}, попытка {attempt+1})")
            return result
        except Exception as e:
            wait_time = min(INITIAL_DELAY * (2 ** attempt), MAX_DELAY)
            logger.warning(f"⚠️ Ошибка {action} (msg_id={msg_id}, попытка {attempt+1}/{MAX_RETRIES}): {e}")
            if attempt == MAX_RETRIES - 1:
                logger.error(f"❌ Не удалось: {action} после {MAX_RETRIES} попыток: msg_id={msg_id}")
                return None
            await exponential_backoff(attempt)
    return None


async def main():
    logger.info("Подключаюсь к Telegram...")
    await client.start(phone)
    logger.info("✅ Авторизация прошла успешно!")

    try:
        channel_entity = await client.get_entity(target_channel)
        logger.info(f"✅ Целевой канал найден: {target_channel}")
    except Exception as e:
        logger.error(f"❌ Не удалось найти канал {target_channel}: {e}")
        return

        # === Загружаем source_entity заранее ===
    try:
        source_entity = await client.get_entity(source_username)
        logger.info(f"✅ Найден исходный чат: {source_username}")
    except Exception as e:
        logger.error(f"❌ Не удалось найти исходный чат {source_username}: {e}")
        return

    # === Читаем сообщения ===
    query = '''
        SELECT id, date, is_service
        FROM messages
        WHERE (status IS NULL OR status != 'sent')
    '''
    if SKIP_SERVICE:
        query += ' AND is_service = 0'
    query += ' ORDER BY date ASC, id ASC'

    cursor.execute(query)
    rows = cursor.fetchall()

    if not rows:
        logger.info("✅ Нет сообщений для отправки.")
        return

    logger.info(f"Найдено {len(rows)} сообщений. Начинаем строгую пересылку...")

    current_date = None
    total_sent = 0

    for row in rows:
        msg_id, date_str, is_service = row
        msg_date = datetime.fromisoformat(date_str).date()

        # --- Заголовок дня ---
        if msg_date != current_date:
            current_date = msg_date
            header = f"📅 {msg_date.year}/{msg_date.month:02d}/{msg_date.day:02d}"

            # Создаём функцию, а не корутину
            send_header = lambda: client.send_message(channel_entity, header)
            result = None
            while result is None:
                result = await send_with_retry(send_header, msg_id=f"header_{msg_date}", action="send_date_header")
                if result is None:
                    logger.warning(f"🔁 Не удалось отправить заголовок {header}. Повтор...")
                    await asyncio.sleep(5)
            time.sleep(0.5)

        # --- Пересылка сообщения ---
        # Создаём функцию, а не корутину
        def make_forward():
            return client.forward_messages(
                channel_entity,
                from_peer=source_entity,
                messages=[msg_id]
            )

        result = None
        while result is None:
            result = await send_with_retry(make_forward, msg_id=msg_id, action="forward_by_id")
            if result is None:
                logger.warning(f"🔁 Сообщение {msg_id} не отправлено. Ждём и повторяем...")
                await asyncio.sleep(5)

        total_sent += 1
        cursor.execute('UPDATE messages SET status = ? WHERE id = ?', ('sent', msg_id))
        conn.commit()

        logger.info(f"📨 Успешно отправлено: msg_id={msg_id}")
        time.sleep(DELAY)

    logger.info(f"✅ Все сообщения отправлены. Всего: {total_sent}")

with client:
    client.loop.run_until_complete(main())

conn.close()
