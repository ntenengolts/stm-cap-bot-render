import os
from aiohttp import web
import asyncio
from bot import bot, dp  # импортируем из bot.py

WEBHOOK_PATH = "/webhook"
WEBHOOK_HOST = os.getenv("WEBHOOK_HOST", "https://stm-cap-bot-render-9p6w.onrender.com")
WEBHOOK_URL = f"{WEBHOOK_HOST}{WEBHOOK_PATH}"
WEBAPP_HOST = "0.0.0.0"
WEBAPP_PORT = int(os.getenv("PORT", 8000))


async def on_startup(app: web.Application):
    # Устанавливаем webhook
    await bot.set_webhook(WEBHOOK_URL)
    print("Webhook установлен:", WEBHOOK_URL)


async def on_shutdown(app: web.Application):
    # Снимаем webhook при остановке
    await bot.delete_webhook()
    await bot.session.close()
    print("Webhook снят")


# Основной обработчик для Telegram (POST /webhook)
async def handle(request: web.Request):
    update = await request.json()
    await dp.feed_webhook_update(bot, update)
    return web.Response()


# Healthcheck для Render и cron-job (GET /)
async def healthcheck(request: web.Request):
    return web.Response(text="OK: bot is alive")


def main():
    app = web.Application()
    app.router.add_post(WEBHOOK_PATH, handle)   # Telegram слать будет сюда
    app.router.add_get("/", healthcheck)        # cron-job и Render проверяют сюда

    app.on_startup.append(on_startup)
    app.on_shutdown.append(on_shutdown)

    web.run_app(app, host=WEBAPP_HOST, port=WEBAPP_PORT)


if __name__ == "__main__":
    main()