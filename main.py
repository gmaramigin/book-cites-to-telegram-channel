import asyncio
import logging
import random
import json
from datetime import datetime

from telegram import Bot, InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.ext import (
    ApplicationBuilder,
    CommandHandler,
    CallbackQueryHandler,
    CallbackContext
)
from notion_client import AsyncClient
from apscheduler.schedulers.background import BackgroundScheduler

# 🔑 API Токены
NOTION_TOKEN = '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'
DATABASE_ID = '95d95ad5de6f48dc8b31f3bfe1706645'
TELEGRAM_TOKEN = '7461688626:AAGnm898fTCfy2l7uEi-vudGM9nni6GveZ4'
CHANNEL_ID = '@emaramigin_blog'

# 📚 Инициализация клиентов
notion = AsyncClient(auth=NOTION_TOKEN)
bot = Bot(token=TELEGRAM_TOKEN)

# 🧠 Глобальные переменные для управления цитатами
quote_pool = []  # Список цитат для отправки
used_quotes = []  # Список использованных цитат
CACHE_FILE = 'quotes_cache.json'

# 🛡️ Логирование
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)


# 🗂️ Кэширование данных
def save_cache(quotes):
    try:
        with open(CACHE_FILE, 'w', encoding='utf-8') as f:
            json.dump(quotes, f, ensure_ascii=False, indent=4)
        logging.info("✅ Цитаты сохранены в кэш.")
    except Exception as e:
        logging.error(f"❌ Ошибка при сохранении кэша: {e}")


def load_cache():
    try:
        with open(CACHE_FILE, 'r', encoding='utf-8') as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        logging.warning("⚠️ Кэш не найден. Будет выполнена загрузка из Notion.")
        return []


# 🎯 Получение всех цитат из Notion с использованием асинхронных запросов
async def fetch_quotes():
    global quote_pool, used_quotes
    try:
        logging.info("✅ Подключение к Notion API для загрузки цитат.")

        quotes = []
        next_cursor = None

        while True:
            query_params = {'database_id': DATABASE_ID, 'page_size': 100}
            if next_cursor:
                query_params['start_cursor'] = next_cursor

            response = await notion.databases.query(**query_params)
            pages = response['results']

            tasks = [process_notion_page(page) for page in pages]
            results = await asyncio.gather(*tasks)

            for result in results:
                if result:
                    quotes.extend(result)

            next_cursor = response.get('next_cursor')
            if not next_cursor:
                break

        if quotes:
            random.shuffle(quotes)
            quote_pool = quotes
            used_quotes = []
            save_cache(quotes)  # Сохраняем в кэш
            logging.info(f"✅ Загружено {len(quotes)} цитат из Notion.")
        else:
            logging.warning("⚠️ В базе данных Notion нет цитат.")
    except Exception as e:
        logging.error(f"❌ Ошибка при получении цитат: {e}")


# 📦 Обработка одной страницы в Notion
async def process_notion_page(page):
    try:
        page_id = page['id']
        properties = page['properties']

        author = "Неизвестный автор"
        if 'Author' in properties and properties['Author'].get('title'):
            author = properties['Author']['title'][0]['text']['content']

        book_name = "Неизвестная книга"
        if "The Book's name" in properties and properties["The Book's name"].get('rich_text'):
            book_name = properties["The Book's name"]['rich_text'][0]['text']['content']

        page_content = await notion.blocks.children.list(block_id=page_id)
        book_quotes = []
        for block in page_content['results']:
            if block['type'] == 'quote' and block['quote']['rich_text']:
                for text_block in block['quote']['rich_text']:
                    quote = text_block['text']['content']
                    if quote.strip():
                        book_quotes.append(quote)

        return [{'quote': quote, 'author': author, 'book_name': book_name} for quote in book_quotes]
    except Exception as e:
        logging.error(f"❌ Ошибка при обработке страницы: {e}")
        return []


# 🚀 Получение следующей цитаты
def get_next_quote():
    global quote_pool, used_quotes

    if not quote_pool:
        logging.info("🔄 Перезагрузка цитат...")
        quote_pool.extend(load_cache())
        if not quote_pool:
            asyncio.run(fetch_quotes())

    if not quote_pool:
        logging.warning("⚠️ Цитаты закончились.")
        return None

    next_quote = quote_pool.pop()
    used_quotes.append(next_quote)

    if not quote_pool:
        quote_pool = used_quotes.copy()
        random.shuffle(quote_pool)
        used_quotes = []

    return next_quote


# 🚀 Автоматическая отправка цитаты в Telegram-канал
async def send_quote_to_channel():
    try:
        quote_data = get_next_quote()

        if quote_data is None:
            await bot.send_message(chat_id=CHANNEL_ID, text="⚠️ Цитаты закончились. Пополните базу данных в Notion.")
            logging.warning("⚠️ Цитаты закончились.")
        else:
            message_template = f"""

🌟 *Цитата дня*
✍️ *{quote_data['author']}*
📖 *{quote_data['book_name']}*

💬 _"{quote_data['quote']}"_
"""
            await bot.send_message(chat_id=CHANNEL_ID, text=message_template, parse_mode='Markdown')
            logging.info("✅ Цитата успешно отправлена в канал.")
    except Exception as e:
        logging.error(f"❌ Ошибка при отправке цитаты: {e}")


# 🛠️ Команды для Telegram
async def start(update: Update, context: CallbackContext):
    keyboard = [
        [InlineKeyboardButton("📖 Отправить цитату в канал", callback_data='send_to_channel')]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    await update.message.reply_text('👋 Привет! Нажмите на кнопку ниже, чтобы отправить цитату в канал.', reply_markup=reply_markup)


async def button(update: Update, context: CallbackContext):
    query = update.callback_query
    await query.answer()
    await send_quote_to_channel()


# 🕒 Планировщик
scheduler = BackgroundScheduler()
scheduler.add_job(lambda: asyncio.run(fetch_quotes()), 'cron', hour=4, minute=41)
scheduler.add_job(lambda: asyncio.run(send_quote_to_channel()), 'cron', hour=4, minute=42)
scheduler.start()


def main():
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(fetch_quotes())

    application = ApplicationBuilder().token(TELEGRAM_TOKEN).build()
    application.add_handler(CommandHandler('start', start))
    application.add_handler(CallbackQueryHandler(button))
    application.run_polling()


if __name__ == '__main__':
    main()
