from dotenv import load_dotenv
import os
from aiogram import Bot, Dispatcher, types, F
from aiogram.types import Message, InlineKeyboardButton, InlineKeyboardMarkup, CallbackQuery
from aiogram.utils.keyboard import InlineKeyboardBuilder
from aiogram.filters import Command
import asyncio
from googleapiclient.discovery import build
from google.oauth2 import service_account
import json
import datetime
import pytz


load_dotenv()  # Загружаем переменные из .env

# Получаем значения из окружения
TOKEN = os.getenv("TOKEN")
SPREADSHEET_ID = os.getenv("SPREADSHEET_ID")

# Путь к файлу с ключом сервисного аккаунта
SERVICE_ACCOUNT_FILE = 'stm-cap-bot-33d82026f843.json'
SCOPES = ['https://www.googleapis.com/auth/spreadsheets']

# Константы
DATA_RANGE = 'БД!A2:C1000'
ACCESS_RANGE = 'Доступ!A2:C1000'
MESSAGES_RANGE = 'Сообщения!A2:B100'

_sheets_service = None  # кэш для повторного использования


if not os.path.exists(SERVICE_ACCOUNT_FILE):
    service_account_json = os.getenv("GOOGLE_CREDENTIALS_JSON")
    if service_account_json is None:
        raise ValueError("Переменная окружения GOOGLE_CREDENTIALS_JSON не установлена!")
    
    try:
        # Проверяем, что это валидный JSON
        json.loads(service_account_json)
    except json.JSONDecodeError:
        raise ValueError("GOOGLE_CREDENTIALS_JSON содержит некорректный JSON")

    # Сохраняем JSON в файл
    with open(SERVICE_ACCOUNT_FILE, "w") as f:
        f.write(service_account_json)


# Инициализация
bot = Bot(token=TOKEN)
dp = Dispatcher()


def authenticate():
    """Authenticate using the service account and create a service to access Google Sheets."""
    creds = service_account.Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE, scopes=SCOPES)
    
    # Создаем сервис для работы с Google Sheets
    service = build('sheets', 'v4', credentials=creds)
    return service


# Получение данных
def get_sheet_data(range_name):
    service = authenticate()  # Получаем авторизованный сервис
    sheet = service.spreadsheets()
    result = sheet.values().get(spreadsheetId=SPREADSHEET_ID, range=range_name).execute()
    return result.get('values', [])


# Проверка доступа
def get_user_access_status(username: str, telegram_id: int) -> str:
    """
    Возвращает тип доступа:
    - 'admin' — полный доступ
    - 'user' — обычный пользователь
    - None — доступ запрещён
    """
    data = get_sheet_data(ACCESS_RANGE)
    for row in data:
        if len(row) < 3:
            continue

        sheet_username = row[0].strip().lower() if row[0] else ""
        sheet_telegram_id = row[1].strip() if row[1] else ""
        access_level = row[2].strip().lower()

        if access_level not in ("yes", "admin"):
            continue

        # Сопоставление по username
        if sheet_username and sheet_username == username.strip().lower():
            return access_level

        # Сопоставление по telegram_id
        if sheet_telegram_id and sheet_telegram_id == str(telegram_id):
            return access_level

    return None  # Нет совпадений — доступ запрещён


def is_user_allowed(username: str, telegram_id: int) -> bool:
    status = get_user_access_status(username, telegram_id)
    return status is not None  # есть хоть какой-то доступ

def is_user_admin(username: str, telegram_id: int) -> bool:
    status = get_user_access_status(username, telegram_id)
    return status == "admin"


def get_system_message(key: str) -> str:
    messages = get_sheet_data(MESSAGES_RANGE)
    for row in messages:
        if row and row[0].strip().lower() == key.strip().lower():
            return row[1]
    return "⚠️ Упс... Что-то пошло не так. Попробуй снова или обратись к администратору."


# Функция логирования запросов
def log_user_request(username: str, telegram_id: int, message_text: str):
    """Записывает информацию о запросе пользователя в таблицу 'Логи'."""
    service = authenticate()  # Получаем авторизованный сервис
    sheet = service.spreadsheets()

    # Подготовка данных для записи
    timezone = pytz.timezone("Europe/Moscow")  # Указываем нужный часовой пояс
    now = datetime.datetime.now(timezone)

    date_str = now.strftime("%Y-%m-%d")  # Получаем дату
    time_str = now.strftime("%H:%M:%S")  # Получаем время

    values = [[date_str, time_str, username, telegram_id, message_text]]  # Данные, которые хотим записать

    # Запись данных в таблицу
    sheet.values().append(
        spreadsheetId=SPREADSHEET_ID,
        range="Логи!A2",  # Начинаем с первой строки
        valueInputOption="RAW",
        body={"values": values}
    ).execute()


# Функция логирования нажатия кнопок
def log_user_action(username: str, telegram_id: int, action: str):
    service = authenticate()
    sheet = service.spreadsheets()

    timezone = pytz.timezone("Europe/Moscow")
    now = datetime.datetime.now(timezone)

    date_str = now.strftime("%Y-%m-%d")
    time_str = now.strftime("%H:%M:%S")

    values = [[date_str, time_str, username, telegram_id, action]]

    sheet.values().append(
        spreadsheetId=SPREADSHEET_ID,
        range="Логи!A2",
        valueInputOption="RAW",
        body={"values": values}
    ).execute()


# Команда /start
@dp.message(Command("start"))
async def send_welcome(message: Message):
    username = message.from_user.username or ""
    telegram_id = message.from_user.id

    # Логируем запрос пользователя
    log_user_request(username, telegram_id, "start")

    if not is_user_allowed(username, telegram_id):
        await message.answer(get_system_message('access_denied'), parse_mode='HTML')
        return

    await message.answer(get_system_message('start_text'), parse_mode='HTML')


# Обработка сообщений и показ кнопок
@dp.message(lambda message: not message.text.startswith("/"))
async def handle_message(message: Message):
    username = message.from_user.username or ""
    telegram_id = message.from_user.id

    # Логируем запрос пользователя
    log_user_request(username, telegram_id, message.text)

    if not is_user_allowed(username, telegram_id):
        await message.answer(get_system_message('access_denied'), parse_mode='HTML')
        return

    text = message.text.lower()
    data = get_sheet_data(DATA_RANGE)


    # 1. Проверка на точное совпадение
    for row in data:
        if row and row[0].lower() == text:
            await message.answer(f"{get_system_message('exact_match_found')} \n{row[1]}", parse_mode='HTML')
            return


    # 2. Поиск частичных совпадений
    matches = []
    for row in data:
        if row and text in row[0].lower():
            matches.append(row)

    if matches:
        builder = InlineKeyboardBuilder()
        for row in matches:
            button_text = f"{row[0]}"
            callback_data = f"def:{row[0]}"
            builder.button(text=button_text, callback_data=callback_data)

        builder.adjust(2)  # по одному элементу (кнопки) в строке

        await message.answer(get_system_message('choose_option'), reply_markup=builder.as_markup(), parse_mode='HTML')
    else:
        await message.answer(get_system_message('no_match'), parse_mode='HTML')


# Обработка нажатия кнопки
@dp.callback_query(F.data.startswith("def:"))
async def handle_definition_callback(callback: CallbackQuery):
    key = callback.data.split("def:")[1].lower()

    username = callback.from_user.username or ""
    telegram_id = callback.from_user.id
    log_user_action(username, telegram_id, f"{key}")

    data = get_sheet_data(DATA_RANGE)

    for row in data:
        if row and row[0].lower() == key:
            await callback.message.answer(f"{get_system_message('exact_match_found')} \n{row[1]}", parse_mode='HTML')
            await callback.answer()
            return

    await callback.message.answer(get_system_message('not_found_on_callback'), parse_mode='HTML')
    await callback.answer()


# Команда /type
@dp.message(Command("type"))
async def list_groups(message: Message):
    # Получаем имя пользователя и ID из сообщения
    username = message.from_user.username or ""
    telegram_id = message.from_user.id

    # Логируем запрос пользователя
    log_user_request(username, telegram_id, "type")

    # Проверка доступа через таблицу "Доступ"
    if not is_user_allowed(username, telegram_id):
        await message.answer(get_system_message('access_denied'), parse_mode='HTML')
        return

    # Загружаем данные из таблицы "БД" (столбцы A, B и C)
    data = get_sheet_data(DATA_RANGE)
    groups = set()  # Используем множество, чтобы исключить дубликаты

    # Собираем уникальные названия групп из третьего столбца
    for row in data:
        if len(row) >= 3:  # Убедимся, что в строке есть третий столбец
            group = row[2].strip()
            if group:
                groups.add(group)

    # Если группы не найдены, уведомляем пользователя
    if not groups:
        await message.answer(get_system_message('not_found_types'), parse_mode='HTML')
        return

    # Создаём инлайн-кнопки с названиями групп
    builder = InlineKeyboardBuilder()
    for group in sorted(groups):  # Отсортируем группы по алфавиту
        builder.button(text=group, callback_data=f"group:{group}")  # callback_data будет обрабатываться позже

    builder.adjust(2)  # Располагаем кнопки по две в строке

    # Показываем пользователю список групп
    await message.answer(get_system_message('choose_option'), reply_markup=builder.as_markup(), parse_mode='HTML')


# Обработка нажатия на кнопку с группой
@dp.callback_query(F.data.startswith("group:"))
async def show_items_in_group(callback: CallbackQuery):
    # Извлекаем название выбранной группы из callback_data
    selected_group = callback.data.split("group:")[1].strip()

    username = callback.from_user.username or ""
    telegram_id = callback.from_user.id
    log_user_action(username, telegram_id, f"{selected_group}")

    # Загружаем данные из таблицы "БД"
    data = get_sheet_data(DATA_RANGE)

    # Фильтруем строки, принадлежащие выбранной группе
    items = [row for row in data if len(row) >= 3 and row[2].strip() == selected_group]

    # Если в группе нет элементов, сообщаем пользователю
    if not items:
        await callback.message.answer(get_system_message('not_found_elements'), parse_mode='HTML')
        await callback.answer()
        return

    # Создаём инлайн-кнопки с названиями элементов группы
    builder = InlineKeyboardBuilder()
    for row in items:
        builder.button(text=row[0], callback_data=f"def:{row[0]}")  # Эти callback будут обрабатываться основным хендлером

    # ⬅️ Кнопка "Назад"
    builder.button(text="⬅️ Назад", callback_data="back_to_groups")

    builder.adjust(2)  # Располагаем кнопки по две в строке

    # Показываем пользователю список элементов группы
    await callback.message.answer(get_system_message('choose_option'), reply_markup=builder.as_markup(), parse_mode='HTML')
    await callback.answer()  # Закрываем спиннер "Загрузка..."


# Обработка нажатия на кнопку "Назад"
@dp.callback_query(F.data == "back_to_groups")
async def back_to_group_selection(callback: CallbackQuery):
    data = get_sheet_data(DATA_RANGE)
    groups = set()

    for row in data:
        if len(row) >= 3:
            group = row[2].strip()
            if group:
                groups.add(group)

    if not groups:
        await callback.message.answer(get_system_message('not_found_types'), parse_mode='HTML')
        await callback.answer()
        return

    builder = InlineKeyboardBuilder()
    for group in sorted(groups):
        builder.button(text=group, callback_data=f"group:{group}")

    builder.adjust(2)

    await callback.message.answer(get_system_message('choose_option'), reply_markup=builder.as_markup(), parse_mode='HTML')
    await callback.answer()


# Команда /help
@dp.message(Command("help"))
async def show_help(message: Message):

    # Показываем пользователю справку (команда /help)
    await message.answer(get_system_message('help_text'), parse_mode='HTML')


# Добавляем новые функции для рассылки пользователям
def get_active_users() -> set:
    """Возвращает set с telegram_id всех, кто писал боту."""
    logs = get_sheet_data("Логи!A2:E10000")
    active_users = set()
    
    for row in logs:
        if len(row) >= 4 and row[3].strip().isdigit():
            active_users.add(int(row[3]))
    
    return active_users

async def get_allowed_active_users() -> list:
    """Возвращает пользователей, которые:
    1. Писали боту (есть в Логах)
    2. Имеют доступ yes (проверка по username или telegram_id)
    """
    active_users = get_active_users()
    access_data = get_sheet_data(ACCESS_RANGE)
    allowed_users = []
    
    for row in access_data:
        if len(row) < 3:
            continue
        
        username = row[0].strip().lower() if len(row) >= 1 else ""
        telegram_id = int(row[1].strip()) if len(row) >= 2 and row[1].strip() else None
        access_level = row[2].strip().lower()

        # Теперь проверяем на 'yes' или 'admin'
        if access_level not in ('yes', 'admin'):
            continue
        
        # Проверяем по telegram_id
        if telegram_id and telegram_id in active_users:
            allowed_users.append({'telegram_id': telegram_id, 'username': username})
            continue
        
        # Проверяем по username (если telegram_id не указан)
        if username:
            for user_id in active_users:
                try:
                    user = await bot.get_chat(user_id)
                    if user.username and user.username.lower() == username:
                        allowed_users.append({
                            'telegram_id': user_id,
                            'username': username
                        })
                        break
                except Exception:
                    continue
    
    return allowed_users


def log_broadcast(admin_id: int, message: str, success_count: int):
    """Логирует факт рассылки"""
    service = authenticate()
    timezone = pytz.timezone("Europe/Moscow")
    now = datetime.datetime.now(timezone)
    
    values = [[now.strftime("%Y-%m-%d"), now.strftime("%H:%M:%S"), 
              admin_id, message[:100], success_count]]
    
    service.spreadsheets().values().append(
        spreadsheetId=SPREADSHEET_ID,
        range="Рассылки!A2",
        valueInputOption="RAW",
        body={"values": values}
    ).execute()


# Добавляем новый хендлер - команда send_all
@dp.message(Command("send_all"))
async def send_broadcast(message: Message):
    username = message.from_user.username or ""
    telegram_id = message.from_user.id

    if not is_user_admin(username, telegram_id):
        await message.answer(get_system_message('access_denied'), parse_mode='HTML')
        return

    broadcast_text = message.text.replace('/send_all', '').strip()
    if not broadcast_text:
        await message.answer(get_system_message('send_all_info'), parse_mode='HTML')
        return

    recipients = await get_allowed_active_users()
    if not recipients:
        await message.answer("🔍 Нет активных пользователей для рассылки")
        return

    await message.answer(f"⏳ Рассылка для {len(recipients)} пользователей...")

    success_count = 0
    for user in recipients:
        try:
            await bot.send_message(
                chat_id=user['telegram_id'],
                text=broadcast_text,
                parse_mode='HTML'
            )
            success_count += 1
            await asyncio.sleep(0.1)
        except Exception as e:
            print(f"Ошибка отправки для {user['telegram_id']}: {e}")

    await message.answer(f"✅ Успешно отправлено: {success_count}/{len(recipients)}")
    log_broadcast(message.from_user.id, broadcast_text, success_count)


# Запуск
async def main():
    print("Бот запущен!")
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
