# 1_fetch_all.py
from telethon import TelegramClient
import sqlite3
import asyncio
import time
import logging
import json
from datetime import datetime

# === CHANGE THESE ===
api_id = 1234567
api_hash = 'TODO'
phone = '+TODO'
source_username = 'TODO'

# Настройки выгрузки
BATCH_SIZE = 500  # Не меняй без нужды
DELAY = 1.5       # Пауза между пачками (в секундах)
# ====================================

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(message)s',
    handlers=[
        logging.FileHandler('fetch.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Подключаемся к БД
conn = sqlite3.connect('archive.db')
cursor = conn.cursor()

# Создаём таблицу
cursor.execute('''
    CREATE TABLE IF NOT EXISTS messages (
        id INTEGER PRIMARY KEY,
        date TEXT NOT NULL,
        sender_id INTEGER,
        is_outgoing INTEGER NOT NULL,
        is_service INTEGER NOT NULL,
        message_text TEXT,
        media_type TEXT,
        reply_to_msg_id INTEGER,
        data_json TEXT NOT NULL,
        status TEXT  -- 'fetched'
    )
''')
conn.commit()

client = TelegramClient('session_archiver', api_id, api_hash)

def datetime_handler(obj):
    if isinstance(obj, datetime):
        return obj.isoformat()
    elif isinstance(obj, bytes):
        return obj.hex()  # на случай, если есть бинарные данные
    raise TypeError(f"Object of type {type(obj)} is not JSON serializable")

async def main():
    logger.info("Запуск: подключение к Telegram...")
    await client.start(phone)
    logger.info("✅ Авторизация успешна!")

    try:
        entity = await client.get_entity(source_username)
        logger.info(f"✅ Найден собеседник: {entity.first_name or entity.title}")
    except Exception as e:
        logger.error(f"❌ Не удалось найти собеседника '{source_username}': {e}")
        return

    logger.info("Начинаем выгрузку всех сообщений...")

    offset = 0
    total_saved = 0
    total_fetched = 0

    while True:
        try:
            messages = await client.get_messages(
                entity,
                limit=BATCH_SIZE,
                offset_id=offset,
                wait_time=10
            )

            if not messages:
                logger.info("🔚 Больше нет сообщений.")
                break

            batch_saved = 0
            for msg in messages:
                total_fetched += 1

                try:
                    # Преобразуем в словарь и JSON
                    msg_dict = msg.to_dict()
                    msg_json = json.dumps(msg_dict, ensure_ascii=False, indent=None, default=datetime_handler)

                    # Определяем тип медиа
                    media_type = type(msg.media).__name__ if msg.media else None
                    sender_id = msg.from_id.user_id if msg.from_id else None
                    is_service = 1 if msg.action is not None else 0

                    cursor.execute('''
                        INSERT OR IGNORE INTO messages (
                            id, date, sender_id, is_outgoing,
                            is_service, message_text, media_type,
                            reply_to_msg_id, data_json, status
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ''', (
                        msg.id,
                        msg.date.isoformat(),
                        sender_id,
                        1 if msg.out else 0,
                        is_service,
                        msg.message,
                        media_type,
                        msg.reply_to_msg_id,
                        msg_json,
                        'fetched'
                    ))
                    batch_saved += 1
                except Exception as e:
                    logger.error(f"❌ Ошибка при обработке msg_id={msg.id}: {e}")

            conn.commit()
            total_saved += batch_saved
            logger.info(f"✅ Пачка: загружено={len(messages)}, сохранено={batch_saved}, всего={total_saved}")

            offset = messages[-1].id
            time.sleep(DELAY)

        except Exception as e:
            logger.error(f"❌ Ошибка при загрузке: {e}")
            logger.info("Пауза 5 сек и повтор...")
            time.sleep(5)
            continue

    logger.info(f"🎉 Выгрузка завершена. Всего: {total_saved} сообщений.")
    logger.info("📍 Данные сохранены в `archive.db` как JSON.")

with client:
    client.loop.run_until_complete(main())

conn.close()
