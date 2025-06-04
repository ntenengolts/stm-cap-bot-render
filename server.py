from flask import Flask
import threading
from bot import main as run_bot

app = Flask(__name__)

@app.route('/')
def home():
    return "Telegram Bot is running!", 200

# Запуск бота в фоне
def start_bot():
    run_bot()  # твой основной код бота

if __name__ == "__main__":
    # Запускаем бота в отдельном потоке
    bot_thread = threading.Thread(target=start_bot)
    bot_thread.start()

    # Запускаем Flask-сервер
    app.run(host='0.0.0.0', port=int(os.getenv("PORT", 10000)))