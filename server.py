import os
import asyncio
from flask import Flask
from bot import main as run_bot
from threading import Thread

app = Flask(__name__)

@app.route('/')
def home():
    return "Telegram Bot is running!", 200

# Запуск бота в фоне
def start_bot():
    asyncio.run(run_bot())

if __name__ == "__main__":
    # Запускаем бота в отдельном потоке
    bot_thread = Thread(target=start_bot)
    bot_thread.start()

    # Запускаем Flask-сервер
    app.run(host='0.0.0.0', port=int(os.getenv("PORT", 10000)))