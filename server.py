import os
from http.server import BaseHTTPRequestHandler, HTTPServer
from threading import Thread
import asyncio
from bot import main as run_bot  # async def main()


# Простой HTTP-сервер для keep-alive
class KeepAliveHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.send_header("Content-type", "text/html")
        self.end_headers()
        self.wfile.write(b"Telegram Bot is running!")


def start_http_server():
    port = int(os.getenv("PORT", 8000))
    server_address = ('', port)
    httpd = HTTPServer(server_address, KeepAliveHandler)
    print(f"Serving on port {port}")
    httpd.serve_forever()


async def start_bot():
    await run_bot()  # Вызываем бота как корутину


if __name__ == "__main__":
    # Запускаем HTTP-сервер в отдельном потоке
    server_thread = Thread(target=start_http_server)
    server_thread.daemon = True
    server_thread.start()

    # Основной поток остаётся за ботом
    asyncio.run(start_bot())