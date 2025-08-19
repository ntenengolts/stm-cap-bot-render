import os
from aiohttp import web
import asyncio
from bot import bot, dp  # импортируем из bot.py

WEBHOOK_PATH = "/webhook"
WEBHOOK_HOST = os.getenv("WEBHOOK_HOST", "https://your-app.onrender.com")
WEBHOOK_URL = f"{WEBHOOK_HOST}{WEBHOOK_PATH}"
WEBAPP_HOST = "0.0.0.0"
WEBAPP_PORT = int(os.getenv("PORT", 8000))


async def on_startup(app: web.Application):
    await bot.set_webhook(WEBHOOK_URL)
    print("Webhook установлен:", WEBHOOK_URL)


async def on_shutdown(app: web.Application):
    await bot.delete_webhook()
    await bot.session.close()


async def handle(request: web.Request):
    update = await request.json()
    await dp.feed_webhook_update(bot, update)
    return web.Response()


def main():
    app = web.Application()
    app.router.add_post(WEBHOOK_PATH, handle)
    app.on_startup.append(on_startup)
    app.on_shutdown.append(on_shutdown)

    web.run_app(app, host=WEBAPP_HOST, port=WEBAPP_PORT)


if __name__ == "__main__":
    main()
