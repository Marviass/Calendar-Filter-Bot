import os
import re
import datetime
import requests
import telebot
import uuid
from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton
from icalendar import Calendar, Event
from flask import Flask, Response, request
from threading import Thread

# === НАСТРОЙКИ ===
BOT_TOKEN = os.environ.get("BOT_TOKEN") 
bot = telebot.TeleBot(BOT_TOKEN)
# Если запускаешь локально, оставь так. На хостинге замени на реальный домен.
BASE_URL = "https://tsucalbot.onrender.com" 

bot = telebot.TeleBot(BOT_TOKEN)
app = Flask(__name__)

# Хранилище настроек (в идеале — база данных, но для начала сойдет словарь)
# Структура: {link_id: {group_id: ..., subgroup: ..., excluded: [...]}}
links_db = {}

GROUPS = {
    "012301": "1c172ef9-04e5-11ee-814d-005056bc52bb",
    "012302": "7bb46631-04e5-11ee-814d-005056bc52bb"
}

TYPE_EMOJI = {
    "LECTURE": "🔴", "PRACTICE": "🔵", "LABORATORY": "💠",
    "SEMINAR": "🟡", "EXAM": "🎓"
}

# === ЛОГИКА ГЕНЕРАЦИИ (ВЫНЕСЕНА) ===

def get_filtered_ics(group_id, subgroup, excluded_list):
    # Берем расписание на месяц вперед для автообновления
    start = datetime.date.today().strftime("%Y-%m-%d")
    end = (datetime.date.today() + datetime.timedelta(days=30)).strftime("%Y-%m-%d")
    
    url = "https://intime.tsu.ru/api/web/v1/schedule/group"
    params = {"id": group_id, "dateFrom": start, "dateTo": end}
    
    try:
        r = requests.get(url, params=params, timeout=10)
        data = r.json()
    except: return b""

    cal = Calendar()
    cal.add('prodid', '-//TSU Webcal//')
    cal.add('version', '2.0')
    cal.add('X-WR-CALNAME', 'Расписание ТГУ')
    cal.add('REFRESH-INTERVAL;VALUE=DURATION', 'PT12H') # Подсказка календарю обновляться раз в 12 часов

    shift = 7 * 3600
    for day in data.get('grid', []):
        base_dt = datetime.datetime.strptime(day['date'], "%Y-%m-%d")
        for lesson in day.get('lessons', []):
            if lesson.get('type') == 'EMPTY': continue
            
            title = lesson.get('title', '')
            # Фильтр подгруппы
            l_sub = None
            match = re.search(r'\(([А-Г|а-г])\)', title)
            if match: l_sub = match.group(1).lower()
            
            if l_sub and l_sub != subgroup.lower(): continue
            
            # Чистка названия для проверки исключений
            clean_title = re.sub(r'\(.*?\)', '', title).replace('(', '').replace(')', '').strip()
            if clean_title.lower() in [e.lower() for e in excluded_list]: continue

            # Создаем событие
            event = Event()
            emoji = TYPE_EMOJI.get(lesson.get('lessonType'), "🔹")
            event.add('summary', f"{emoji} {clean_title}")
            
            s_sec = lesson.get('starts', 0) + shift
            e_sec = lesson.get('ends', 0) + shift
            event.add('dtstart', base_dt + datetime.timedelta(seconds=s_sec))
            event.add('dtend', base_dt + datetime.timedelta(seconds=e_sec))
            
            # Аудитория + Группа
            g_name = lesson['groups'][0].get('name', '') if lesson.get('groups') else ""
            aud_data = lesson.get('audience')
            if aud_data:
                full_aud = aud_data.get('name', '')
                aud_m = re.search(r'^\d+\s*\(\d+\)', full_aud)
                short_aud = aud_m.group(0) if aud_m else full_aud
                event.add('location', f"{short_aud}, {g_name}")
            
            cal.add_component(event)
    
    return cal.to_ical()

# === FLASK ROUTES (ДЛЯ КАЛЕНДАРЕЙ) ===

@app.route('/get_cal/<link_id>')
def serve_calendar(link_id):
    if link_id not in links_db:
        return "Link not found", 404
    
    config = links_db[link_id]
    ics_data = get_filtered_ics(config['group_id'], config['subgroup'], config['excluded'])
    
    return Response(ics_data, mimetype='text/calendar', 
                    headers={"Content-disposition":"attachment; filename=schedule.ics"})

# === ТЕЛЕГРАМ БОТ (ИНТЕРФЕЙС) ===

user_data = {}

@bot.callback_query_handler(func=lambda c: c.data == "save")
def handle_save(c):
    cid = c.message.chat.id
    # Генерируем уникальный ID для этой настройки
    link_id = str(uuid.uuid4())[:8]
    links_db[link_id] = {
        'group_id': user_data[cid]['group_id'], # Нужно убедиться, что мы сохранили group_id в user_data
        'subgroup': user_data[cid]['subgroup'],
        'excluded': list(user_data[cid]['excluded'])
    }
    
    webcal_url = f"{BASE_URL}/get_cal/{link_id}"
    
    msg = (
        "<b>🔗 Твоя ссылка для автообновления:</b>\n"
        f"<code>{webcal_url}</code>\n\n"
        "<b>Как добавить:</b>\n"
        "1. <b>iPhone:</b> Настройки -> Календарь -> Учетные записи -> Новая -> Другое -> Подписной календарь.\n"
        "2. <b>Google:</b> В веб-версии нажать '+' у 'Другие календари' -> Добавить по URL."
    )
    bot.send_message(cid, msg, parse_mode="HTML")

# === ЗАПУСК ===

def run_bot():
    print("🤖 Бот запускается...")
    while True:
        try:
            bot.remove_webhook() # Очистка старых соединений
            bot.polling(none_stop=True, interval=0, timeout=20)
        except Exception as e:
            print(f"❌ Ошибка бота: {e}")
            import time
            time.sleep(5)

if __name__ == "__main__":
    # Запускаем бота в отдельном "потоке", чтобы он не мешал веб-серверу
    from threading import Thread
    thread = Thread(target=run_bot)
    thread.daemon = True # Поток умрет вместе с основным процессом
    thread.start()

    # Запускаем Flask на порту Render
    port = int(os.environ.get("PORT", 10000))
    print(f"🚀 Веб-сервер запущен на порту {port}")
    app.run(host='0.0.0.0', port=port)



