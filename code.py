import os
import re
import datetime
import requests
import telebot
import sqlite3
import json
import traceback
import threading
import time
import logging
from openai import OpenAI
from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton
from icalendar import Calendar, Event, Alarm

# === 0. ЛОГИРОВАНИЕ ===
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s',
                    handlers=[logging.FileHandler("bot.log", encoding='utf-8'), logging.StreamHandler()])
logger = logging.getLogger(__name__)

# === 1. КОНФИГУРАЦИЯ ===
BOT_TOKEN = os.getenv("BOT_TOKEN")
ADMIN_IDS_RAW = os.getenv("ADMIN_IDS", "1362280774") 
ADMIN_IDS = [x.strip() for x in ADMIN_IDS_RAW.split(",") if x.strip()]

# Настройка универсального ИИ-клиента
AI_API_KEY = os.getenv("AI_API_KEY")
AI_BASE_URL = os.getenv("AI_BASE_URL", "https://routerai.ru/api/v1")
AI_MODEL = os.getenv("AI_MODEL", "google/gemini-3.1-flash-lite-preview")

if AI_API_KEY:
    ai_client = OpenAI(api_key=AI_API_KEY, base_url=AI_BASE_URL)
else:
    ai_client = None
    logger.warning("⚠️ AI_API_KEY не найден. ИИ-функции отключены.")

if not BOT_TOKEN:
    raise ValueError("❌ Токен бота не найден!")

class CrashHandler(telebot.ExceptionHandler):
    def handle(self, exception):
        logger.error(f"⚠️ Ошибка: {exception}")
        return True

bot = telebot.TeleBot(BOT_TOKEN, exception_handler=CrashHandler())

PERSISTENT_DIR = "/data"
DB_FILE = os.path.join(PERSISTENT_DIR, "users.db") if os.path.exists(PERSISTENT_DIR) else "users.db"

TYPE_EMOJI_CIRCLES = {"LECTURE": "🔴", "PRACTICE": "🔵", "LABORATORY": "💠", "SEMINAR": "🟡", "EXAM": "🎓", "CONTROL_WORK": "📝", "CONSULTATION": "❓"}
TYPE_EMOJI_ICONS = {"LECTURE": "🗣️", "PRACTICE": "✍️", "LABORATORY": "🔬", "SEMINAR": "👥", "EXAM": "🎯", "CONTROL_WORK": "⏱️", "CONSULTATION": "💡"}

user_data = {}
user_state = {}

# === ЗАГРУЗКА БАЗ ТГУ ===
ALL_GROUPS, GROUP_NAMES, ALL_TEACHERS, TEACHER_NAMES = {}, {}, {}, {}

def load_databases():
    global ALL_GROUPS, GROUP_NAMES, ALL_TEACHERS, TEACHER_NAMES
    try:
        with open("all_groups.json", "r", encoding="utf-8") as f:
            ALL_GROUPS = json.load(f)
            GROUP_NAMES = {v: k for k, v in ALL_GROUPS.items()}
    except: pass
    try:
        with open("all_teachers.json", "r", encoding="utf-8") as f:
            ALL_TEACHERS = json.load(f)
            TEACHER_NAMES = {v: k for k, v in ALL_TEACHERS.items()}
    except: pass

def get_entity_info(entity_id):
    if entity_id in GROUP_NAMES: return "group", GROUP_NAMES[entity_id]
    if entity_id in TEACHER_NAMES: return "teacher", TEACHER_NAMES[entity_id]
    return "group", "Неизвестно"

def shorten_name(name):
    parts = name.strip().split()
    if len(parts) >= 3: return f"{parts[0]} {parts[1][0]}.{parts[2][0]}."
    elif len(parts) == 2: return f"{parts[0]} {parts[1][0]}."
    return name

# === 2. БАЗА ДАННЫХ И ПОЛЬЗОВАТЕЛИ ===
def init_db():
    with sqlite3.connect(DB_FILE) as conn:
        conn.execute('''CREATE TABLE IF NOT EXISTS users (
                        user_id INTEGER PRIMARY KEY,
                        group_id TEXT, subgroup TEXT, excluded TEXT)''')
        for col, col_type, default in [
            ("short_fio", "INTEGER", "0"), ("reminder", "INTEGER", "15"), ("emoji_style", "INTEGER", "0"),
            ("auto_day", "INTEGER", "-1"), ("auto_time", "INTEGER", "8"), ("remind_type", "INTEGER", "0")
        ]:
            try: conn.execute(f"ALTER TABLE users ADD COLUMN {col} {col_type} DEFAULT {default}")
            except: pass
            
        conn.execute('''CREATE TABLE IF NOT EXISTS overrides (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        user_id INTEGER,
                        target_date TEXT, action TEXT, old_subject TEXT, 
                        new_subject TEXT, new_time TEXT, new_loc TEXT)''')
        
        # Добавил old_time для точечного удаления пар
        for col in ["new_type", "new_teacher", "old_time"]:
            try: conn.execute(f"ALTER TABLE overrides ADD COLUMN {col} TEXT")
            except: pass

def get_user_settings(user_id):
    with sqlite3.connect(DB_FILE) as conn:
        row = conn.execute("SELECT group_id, subgroup, excluded, short_fio, reminder, emoji_style, auto_day, auto_time, remind_type FROM users WHERE user_id=?", (user_id,)).fetchone()
    if row:
        raw_sub = row[1]
        try:
            subs = json.loads(raw_sub) if raw_sub else []
            if not isinstance(subs, list): subs = [str(raw_sub)]
        except: subs = [str(raw_sub)] if raw_sub else []
        return {
            "entity_id": row[0], "subgroups": subs, "excluded": set(json.loads(row[2])) if row[2] else set(),
            "short_fio": bool(row[3]), "reminder": int(row[4] if row[4] is not None else 15),
            "emoji_style": int(row[5] if row[5] is not None else 0),
            "auto_day": int(row[6] if row[6] is not None else -1), "auto_time": int(row[7] if row[7] is not None else 8),
            "remind_type": int(row[8] if len(row)>8 and row[8] is not None else 0)
        }
    return None

def save_user_settings(user_id, settings):
    with sqlite3.connect(DB_FILE) as conn:
        conn.execute('''INSERT OR REPLACE INTO users (user_id, group_id, subgroup, excluded, short_fio, reminder, emoji_style, auto_day, auto_time, remind_type)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''', 
                     (user_id, settings['entity_id'], json.dumps(settings.get('subgroups', [])), json.dumps(list(settings['excluded'])), 
                      int(settings.get('short_fio', False)), settings.get('reminder', 15), settings.get('emoji_style', 0),
                      settings.get('auto_day', -1), settings.get('auto_time', 8), settings.get('remind_type', 0)))

# === 3. ЯДРО ПАРСИНГА И ICS ===
class ScheduleCore:
    @staticmethod
    def extract_auditory_number(location):
        if not location: return ""
        patterns = [r'(\d+[а-яА-Яa-zA-Z]*)\s*\(\s*(\d+)\s*\)', r'(\d+[а-яА-Яa-zA-Z]*)\s*/\s*(\d+)', r'(\d+[а-яА-Яa-zA-Z]*)-(\d+)', r'ауд\.?\s*(\d+[а-яА-Яa-zA-Z]*)\s*корп\.?\s*(\d+)', r'каб\.?\s*(\d+[а-яА-Яa-zA-Z]*)\s*корп\.?\s*(\d+)']
        for p in patterns:
            match = re.search(p, location, re.IGNORECASE)
            if match: return f"{match.group(1).strip()} ({match.group(2).strip()})"
        simple_match = re.search(r'\b(\d{2,}[а-яА-Яa-zA-Z]*)\b', location)
        return simple_match.group(1) if simple_match else location

    @staticmethod
    def clean_discipline_name(text):
        if not text: return text
        text = text.strip()
        if ',' in text: text = text.split(',')[0].strip()
        while True:
            last_paren = text.rfind(')')
            if last_paren != -1:
                first_paren = text.rfind('(', 0, last_paren)
                if first_paren != -1:
                    if last_paren == len(text) - 1: text = text[:first_paren].strip()
                    else: break
                else: text = text[:last_paren].strip()
            else: break
        if len(text) > 2:
            last_char = text[-1].upper()
            if last_char in ['А', 'Б', 'В', 'Г', 'A', 'B', 'C', 'D'] and text[-2].isspace(): text = text[:-1].strip()
        group_match = re.search(r'(\d{6})$', text)
        if group_match: text = text[:-6].strip()
        return re.sub(r'\(+\)+', '', text).strip()

    @staticmethod
    def fetch_json(entity_id, date_from, date_to, entity_type):
        url = "https://intime.tsu.ru/api/web/v1/schedule/professor" if entity_type == "teacher" else "https://intime.tsu.ru/api/web/v1/schedule/group"
        try:
            r = requests.get(url, params={"id": entity_id, "dateFrom": date_from, "dateTo": date_to}, timeout=10)
            return r.json() if r.status_code == 200 else None
        except: return None

    @staticmethod
    def extract_dynamic_subgroups(raw_data):
        subgroups = set()
        if not raw_data or not raw_data.get('grid'): return ['а', 'б', 'в', 'г']
        for day in raw_data['grid']:
            for lesson in day.get('lessons', []):
                for g in lesson.get('groups', []):
                    if g.get('isSubgroup'):
                        match = re.search(r'\((.*?)\)', g.get('name', ''))
                        if match: subgroups.add(match.group(1).lower())
        res = sorted(list(subgroups))
        return res if res else ['а', 'б', 'в', 'г']

    @classmethod
    def parse_events(cls, raw_data):
        events = []
        if not raw_data or not raw_data.get('grid'): return events
        shift = 7 * 3600 
        for day in raw_data.get('grid') or []:
            lessons = day.get('lessons') or []
            if not lessons: continue
            base_date = datetime.datetime.strptime(day['date'], "%Y-%m-%d")
            for lesson in lessons:
                if lesson.get('type') == 'EMPTY': continue
                clean_title = cls.clean_discipline_name(lesson.get('title') or 'Без названия')
                groups_data = lesson.get("groups") or []
                
                locs = []
                aud = lesson.get('audience')
                if isinstance(aud, dict):
                    val = aud.get('shortName') or aud.get('name') or aud.get('number') or ""
                    if val: locs.append(str(val))
                short_loc = cls.extract_auditory_number(", ".join(locs))
                
                teachs = []
                for k in ['teachers', 'lecturers']:
                    for t in (lesson.get(k) or []):
                        if isinstance(t, dict): val = t.get('shortName') or t.get('fullName') or t.get('name') or ""
                        if val: teachs.append(str(val))
                prof = lesson.get('professor')
                if isinstance(prof, dict):
                    val = prof.get('fullName') or prof.get('shortName') or prof.get('name') or ""
                    if val: teachs.append(str(val))
                
                starts, ends = lesson.get('starts') or 0, lesson.get('ends') or 0
                dtstart = base_date + datetime.timedelta(seconds=starts + shift)
                dtend = base_date + datetime.timedelta(seconds=ends + shift)
                events.append({
                    'uid': f"tsu-{dtstart.strftime('%Y%m%d%H%M')}-{hash(clean_title)}@calbot", 
                    'type': lesson.get('type') or '', 'lesson_type': lesson.get('lessonType') or '', 
                    'clean_title': clean_title, 'groups_data': groups_data, 'short_loc': short_loc, 
                    'teachers': ", ".join(teachs), 'dtstart': dtstart, 'dtend': dtend
                })
        return events

    @staticmethod
    def generate_ics(events, settings, entity_type):
        cal = Calendar()
        cal.add('prodid', '-//TSU Smart Calendar//')
        cal.add('version', '2.0')
        cal.add('calscale', 'GREGORIAN')
        emoji_dict = TYPE_EMOJI_ICONS if settings.get('emoji_style') == 1 else TYPE_EMOJI_CIRCLES
        count = 0
        user_subgroups = [s.lower() for s in settings.get('subgroups', [])]
        
        for ev in events:
            if ev['clean_title'].lower() in settings['excluded'] and not ev.get('is_override', False): 
                continue
                
            if entity_type == "group" and ev['type'] == "LESSON":
                keep_lesson = False
                for group in ev['groups_data']:
                    g_name = group.get("name", "").lower()
                    if not group.get("isSubgroup", False): keep_lesson = True; break
                    for sub in user_subgroups:
                        if f"({sub})" in g_name: keep_lesson = True; break
                    if keep_lesson: break
                if not keep_lesson: continue
            
            e = Event()
            e.add('summary', f"{emoji_dict.get(ev['lesson_type'], '🔹')} {ev['clean_title']}")
            if ev['short_loc']: e.add('location', ev['short_loc'])
            if entity_type == "teacher": e.add('description', "👥 Группы")
            else: e.add('description', ", ".join([shorten_name(t.strip()) for t in ev['teachers'].split(',')]) if (settings.get('short_fio') and ev['teachers']) else (ev['teachers'] if ev['teachers'] else "Не назначен"))
            
            e.add('dtstart', ev['dtstart'])
            e.add('dtend', ev['dtend'])
            e.add('dtstamp', datetime.datetime.now(datetime.timezone.utc))
            e.add('uid', ev['uid'])

            reminder = settings.get('reminder', 15)
            if reminder > 0 and settings.get('remind_type', 0) == 0:
                alarm = Alarm()
                alarm.add('action', 'DISPLAY')
                alarm.add('description', ev['clean_title'])
                alarm.add('trigger', datetime.timedelta(minutes=-reminder))
                e.add_component(alarm)
            cal.add_component(e)
            count += 1
        return cal.to_ical(), count

# === 3.5 ИНЖЕКТОР ИИ-ИЗМЕНЕНИЙ (ИСПРАВЛЕННЫЙ) ===
def apply_ai_overrides(events, user_id, df_str, dt_str):
    with sqlite3.connect(DB_FILE) as conn:
        # Безопасное извлечение с поддержкой старых версий БД
        rows = conn.execute("SELECT * FROM overrides WHERE user_id=?", (user_id,)).fetchall()
        cursor = conn.execute("SELECT * FROM overrides LIMIT 0")
        cols = [desc[0] for desc in cursor.description]
        
    overrides = []
    for row in rows:
        d = dict(zip(cols, row))
        overrides.append((
            d.get('target_date'), d.get('action'), d.get('old_subject'),
            d.get('new_subject'), d.get('new_time'), d.get('new_loc'),
            d.get('new_type', ''), d.get('new_teacher', ''), d.get('old_time', '')
        ))
    
    if not overrides: return events
    final_events = []
    
    for ev in events:
        ev_date = ev['dtstart'].strftime("%Y-%m-%d")
        ev_time = ev['dtstart'].strftime("%H:%M")
        skip = False
        for ov in overrides:
            t_date, action, old_sub, new_sub, n_time, n_loc, n_type, n_teacher, old_time = ov
            if t_date == ev_date and old_sub and old_sub.lower() in ev['clean_title'].lower():
                # ИСПРАВЛЕНИЕ: Удаляем только если совпадает время старой пары
                if old_time and old_time != ev_time:
                    continue 
                if action in ["cancel", "replace"]: skip = True
        if not skip: final_events.append(ev)
            
    for ov in overrides:
        t_date, action, old_sub, new_sub, n_time, n_loc, n_type, n_teacher, old_time = ov
        if action in ["replace", "add"] and new_sub:
            # ИСПРАВЛЕНИЕ: Блокируем "призраков", добавляя пару только в нужный диапазон дат
            if not (df_str <= t_date <= dt_str):
                continue
                
            try:
                base_dt = datetime.datetime.strptime(t_date, "%Y-%m-%d")
                if n_time and ":" in n_time:
                    h, m = map(int, n_time.split(":"))
                    dtstart = base_dt.replace(hour=h, minute=m)
                else: dtstart = base_dt.replace(hour=9, minute=0)
                dtend = dtstart + datetime.timedelta(minutes=90)
                
                loc_str = n_loc.strip() if n_loc else "Не указано"
                if "⚠️" not in loc_str: loc_str += " ⚠️"
                
                clean_title = new_sub.strip()
                if clean_title:
                    clean_title = clean_title[0].upper() + clean_title[1:]
                
                valid_types = ["LECTURE", "PRACTICE", "LABORATORY", "SEMINAR", "EXAM", "CONTROL_WORK", "CONSULTATION"]
                l_type = n_type if n_type in valid_types else "LECTURE"
                
                final_events.append({
                    'uid': f"ai-override-{hash(new_sub + t_date)}@calbot",
                    'type': 'LESSON', 
                    'lesson_type': l_type,
                    'clean_title': clean_title,
                    'groups_data': [{'name': 'Все', 'isSubgroup': False}],
                    'short_loc': loc_str,
                    'teachers': n_teacher or "",
                    'dtstart': dtstart, 'dtend': dtend,
                    'is_override': True
                })
            except: pass
            
    final_events.sort(key=lambda x: x['dtstart'])
    return final_events

def update_disciplines_for_subgroup(cid, raw, e_type):
    if not raw: return
    if e_type == "teacher":
        user_data[cid]['all_disc'] = sorted(list(set(e['clean_title'] for e in ScheduleCore.parse_events(raw))))
        return
    user_subs = [s.lower() for s in user_data[cid].get('subgroups', [])]
    events = ScheduleCore.parse_events(raw)
    filtered = set()
    for ev in events:
        if ev['type'] == "LESSON":
            keep = False
            for g in ev['groups_data']:
                g_name = g.get("name", "").lower()
                if not g.get("isSubgroup", False): keep = True; break
                for sub in user_subs:
                    if f"({sub})" in g_name: keep = True; break
                if keep: break
            if keep: filtered.add(ev['clean_title'])
        else: filtered.add(ev['clean_title'])
    user_data[cid]['all_disc'] = sorted(list(filtered))

# === 4. UI И КЛАВИАТУРЫ (ПОЭТАПНЫЙ ОНБОРДИНГ) ===
def markup_roles():
    m = InlineKeyboardMarkup()
    m.row(InlineKeyboardButton("👨‍🎓 Я студент", callback_data="role_student"), InlineKeyboardButton("👨‍🏫 Я преподаватель", callback_data="role_teacher"))
    return m

def markup_main_menu(e_type):
    m = InlineKeyboardMarkup()
    m.add(InlineKeyboardButton("📅 Сформировать расписание", callback_data="menu_dates"))
    if e_type == "group":
        m.row(InlineKeyboardButton("👤 Профиль", callback_data="menu_profile"), InlineKeyboardButton("📚 Предметы", callback_data="menu_disc"))
    else: m.add(InlineKeyboardButton("👤 Профиль", callback_data="menu_profile"))
    m.row(InlineKeyboardButton("🛠 Настройки", callback_data="menu_prefs"), InlineKeyboardButton("💬 Обратная связь", callback_data="menu_feedback"))
    return m

def markup_profile(cid, e_type):
    m = InlineKeyboardMarkup(row_width=1)
    if e_type == "group":
        s = user_data.get(cid)
        subs_text = ", ".join([sub.upper() for sub in s.get('subgroups', [])]) if s.get('subgroups') else "Не выбрано"
        m.add(InlineKeyboardButton(f"💠 Изменить подгруппу", callback_data="menu_subgroups"))
    m.add(InlineKeyboardButton("🔄 Перенастроить профиль", callback_data="reset_profile"))
    m.add(InlineKeyboardButton("⬅️ В меню", callback_data="back_to_main"))
    return m

def markup_subgroups(cid, is_onboarding=False):
    s = user_data.get(cid, {})
    m = InlineKeyboardMarkup(row_width=2)
    avail_subs = s.get('subgroups_list', ['а', 'б', 'в', 'г']) 
    chosen_subs = s.get('subgroups', [])
    buttons = [InlineKeyboardButton(f"{'✅' if sub in chosen_subs else '❌'} Подгруппа {sub.upper()}", callback_data=f"tsub_{sub}_{int(is_onboarding)}") for sub in avail_subs]
    m.add(*buttons)
    if is_onboarding: m.add(InlineKeyboardButton("Продолжить ➡️", callback_data="onb_step_disc"))
    else: m.add(InlineKeyboardButton("⬅️ Назад", callback_data="menu_profile"))
    return m

def markup_disc(cid, is_onboarding=False):
    s = user_data[cid]
    m = InlineKeyboardMarkup(row_width=1)
    if is_onboarding: m.add(InlineKeyboardButton("Продолжить ➡️", callback_data="onb_step_emoji"))
    else: m.add(InlineKeyboardButton("⬅️ Назад", callback_data="back_to_main"))
    for i, d in enumerate(s.get('all_disc', [])):
        st = "❌" if d.lower() in s['excluded'] else "✅"
        m.add(InlineKeyboardButton(f"{st} {d[:35]}", callback_data=f"do_{i}" if is_onboarding else f"d_{i}"))
    return m

def markup_onb_emoji(cid):
    s = user_data[cid]
    m = InlineKeyboardMarkup(row_width=1)
    m.add(InlineKeyboardButton(f"🎨 Выбрано: {'Emoji 🔬' if s.get('emoji_style') == 1 else 'Кружки (как в TSUInTime) 🔴'}", callback_data="toggle_emoji_onb"))
    m.add(InlineKeyboardButton("Продолжить ➡️", callback_data="onb_step_remind"))
    return m

def markup_onb_remind(cid):
    s = user_data[cid]
    m = InlineKeyboardMarkup(row_width=1)
    rem_val = s.get('reminder', 15)
    m.add(InlineKeyboardButton(f"⏱ Уведомление: за {rem_val} мин" if rem_val > 0 else "Выкл.", callback_data="toggle_remind_onb"))
    m.add(InlineKeyboardButton("Продолжить ➡️", callback_data="onb_step_rtype"))
    return m

def markup_onb_rtype(cid):
    s = user_data[cid]
    m = InlineKeyboardMarkup(row_width=1)
    r_type = "От приложения 📅" if s.get('remind_type', 0) == 0 else "От бота 🤖"
    m.add(InlineKeyboardButton(f"🔔 Источник: {r_type}", callback_data="toggle_rtype_onb"))
    m.add(InlineKeyboardButton("Продолжить ➡️", callback_data="onb_step_auto"))
    return m

def markup_onb_auto(cid):
    s = user_data[cid]
    m = InlineKeyboardMarkup(row_width=1)
    day_map = {-1: "Выкл. ❌", 5: "Суббота", 6: "Воскресенье"}
    time_map = {8: "08:00", 13: "13:00", 19: "19:00"}
    d_val = s.get('auto_day', -1)
    m.add(InlineKeyboardButton(f"📬 День рассылки: {day_map.get(d_val)}", callback_data="toggle_aday_onb"))
    if d_val != -1: m.add(InlineKeyboardButton(f"🕒 Время: {time_map.get(s.get('auto_time', 8))}", callback_data="toggle_atime_onb"))
    m.add(InlineKeyboardButton("Продолжить ➡️", callback_data="onb_step_ai"))
    return m

def markup_onb_ai():
    m = InlineKeyboardMarkup()
    m.add(InlineKeyboardButton("Понятно! Завершить настройку ✅", callback_data="onboard_finish"))
    return m

def markup_prefs(cid):
    s = user_data[cid]
    m = InlineKeyboardMarkup(row_width=1)
    rem_val = s.get('reminder', 15)
    m.add(InlineKeyboardButton(f"⏱ Уведомления: за {rem_val} мин" if rem_val > 0 else "Выкл.", callback_data="toggle_remind"))
    if rem_val > 0:
        r_type = "От приложения 📅" if s.get('remind_type', 0) == 0 else "От бота 🤖"
        m.add(InlineKeyboardButton(f"🔔 Источник: {r_type}", callback_data="toggle_rtype"))
    m.add(InlineKeyboardButton(f"🎨 Значки: {'Emoji 🔬' if s.get('emoji_style') == 1 else 'Кружки 🔴'}", callback_data="toggle_emoji"))
    day_map = {-1: "Выкл ❌", 5: "Суббота", 6: "Воскресенье"}
    time_map = {8: "08:00", 13: "13:00", 19: "19:00"}
    d_val = s.get('auto_day', -1)
    m.add(InlineKeyboardButton(f"📬 Авто-рассылка: {day_map.get(d_val)}", callback_data="toggle_aday"))
    if d_val != -1: m.add(InlineKeyboardButton(f"🕒 Время: {time_map.get(s.get('auto_time', 8))}", callback_data="toggle_atime"))
    m.add(InlineKeyboardButton("⬅️ Назад", callback_data="back_to_main"))
    return m

def markup_dates(entity_id):
    m = InlineKeyboardMarkup()
    m.add(InlineKeyboardButton("📅 Текущая неделя", callback_data=f"date_curr_{entity_id}"))
    m.add(InlineKeyboardButton("⏭️ Следующая неделя", callback_data=f"date_next_{entity_id}"))
    m.add(InlineKeyboardButton("✍️ Свои даты", callback_data=f"date_manual_{entity_id}"))
    m.add(InlineKeyboardButton("⬅️ Назад", callback_data="back_to_main"))
    return m

def markup_download():
    m = InlineKeyboardMarkup(row_width=1)
    m.add(InlineKeyboardButton("☕ Поддержать автора", url="https://pay.cloudtips.ru/p/8e989559"))
    m.row(InlineKeyboardButton("🆘 Помощь", callback_data="help_save"), InlineKeyboardButton("🏠 В меню", callback_data="back_to_main"))
    return m

def markup_os():
    m = InlineKeyboardMarkup(row_width=1)
    m.add(InlineKeyboardButton("🤖 Android (Google Календарь)", callback_data="help_android"))
    m.add(InlineKeyboardButton("🍏 iPhone (iOS Календарь)", callback_data="help_ios"))
    m.add(InlineKeyboardButton("🪟 Windows / ПК", callback_data="help_windows"))
    m.add(InlineKeyboardButton("⬅️ Назад", callback_data="back_to_download"))
    return m

def show_main_menu(cid, message_id=None):
    settings = user_data.get(cid) or get_user_settings(cid)
    if not settings or not settings.get('entity_id'): return
    e_type, e_name = get_entity_info(settings['entity_id'])
    
    if e_type == "group":
        subs_str = ",".join([s.upper() for s in settings.get('subgroups', [])])
        profile_text = f"👨‍🎓 Студент группы **{e_name} ({subs_str})**"
    else:
        profile_text = f"👨‍🏫 **{e_name}**"
        
    text = f"🏠 Главное меню\nПрофиль: {profile_text}"
    if message_id: 
        try: bot.edit_message_text(text, cid, message_id, parse_mode="Markdown", reply_markup=markup_main_menu(e_type))
        except: pass
    else: 
        bot.send_message(cid, text, parse_mode="Markdown", reply_markup=markup_main_menu(e_type))

# === 5. ФОНОВЫЕ ПРОЦЕССЫ ===
def auto_mailing_loop():
    last_run_hour = -1
    while True:
        now_tomsk = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(hours=7)
        if now_tomsk.minute == 0 and now_tomsk.hour != last_run_hour:
            last_run_hour = now_tomsk.hour
            try:
                with sqlite3.connect(DB_FILE) as conn:
                    rows = conn.execute("SELECT user_id FROM users WHERE auto_day=? AND auto_time=?", (now_tomsk.weekday(), now_tomsk.hour)).fetchall()
                for row in rows:
                    uid = row[0]
                    s = get_user_settings(uid)
                    if not s or not s['entity_id']: continue
                    e_type, _ = get_entity_info(s['entity_id'])
                    df = (now_tomsk.date() - datetime.timedelta(days=now_tomsk.weekday()) + datetime.timedelta(days=7)).strftime("%Y-%m-%d")
                    dt = (now_tomsk.date() - datetime.timedelta(days=now_tomsk.weekday()) + datetime.timedelta(days=13)).strftime("%Y-%m-%d")
                    raw = ScheduleCore.fetch_json(s['entity_id'], df, dt, e_type)
                    if raw and raw.get('grid'):
                        evs = apply_ai_overrides(ScheduleCore.parse_events(raw), uid, df, dt)
                        f_ical, count = ScheduleCore.generate_ics(evs, s, e_type)
                        bot.send_document(uid, (f"TSU_Mailing_{df}.ics", f_ical), caption=f"📬 **Еженедельная рассылка!** Найдено {count} пар.", reply_markup=markup_download())
            except Exception as e: logger.error(f"Ошибка рассылки: {e}")
        time.sleep(40)

daily_alerts_cache = [] 
def bot_alerts_loop():
    global daily_alerts_cache
    last_fetch_day, last_minute = None, -1
    while True:
        now = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(hours=7)
        if last_fetch_day != now.date() and now.hour >= 4:
            daily_alerts_cache.clear()
            try:
                with sqlite3.connect(DB_FILE) as conn:
                    rows = conn.execute("SELECT user_id FROM users WHERE remind_type=1").fetchall()
                df = now.strftime("%Y-%m-%d")
                for row in rows:
                    uid = row[0]
                    s = get_user_settings(uid)
                    if not s or not s['entity_id'] or s.get('reminder', 0) == 0: continue
                    e_type, _ = get_entity_info(s['entity_id'])
                    raw = ScheduleCore.fetch_json(s['entity_id'], df, df, e_type)
                    if not raw: continue
                    events = apply_ai_overrides(ScheduleCore.parse_events(raw), uid, df, df)
                    user_subs = [sub.lower() for sub in s.get('subgroups', [])]
                    for ev in events:
                        if ev['clean_title'].lower() in s['excluded'] and not ev.get('is_override', False): continue
                        if e_type == "group" and ev['type'] == "LESSON":
                            keep = False
                            for g in ev.get('groups_data', []):
                                g_name = g.get("name", "").lower()
                                if not g.get("isSubgroup", False): keep = True; break
                                for sub in user_subs:
                                    if f"({sub})" in g_name: keep = True; break
                                if keep: break
                            if not keep: continue
                        alert_time = ev['dtstart'] - datetime.timedelta(minutes=s['reminder'])
                        if alert_time > now:
                            daily_alerts_cache.append({'uid': uid, 'alert_time': alert_time, 'title': ev['clean_title'], 'loc': ev['short_loc']})
            except Exception as e: logger.error(f"Будильники: {e}")
            last_fetch_day = now.date()

        if now.minute != last_minute:
            last_minute = now.minute
            to_remove = []
            for alert in daily_alerts_cache:
                if now >= alert['alert_time'] and now < alert['alert_time'] + datetime.timedelta(minutes=2):
                    try: bot.send_message(alert['uid'], f"🔔 **Напоминание!**\nСкоро: **{alert['title']}**\n📍 Ауд: {alert['loc']}")
                    except: pass
                    to_remove.append(alert)
            for r in to_remove:
                if r in daily_alerts_cache: daily_alerts_cache.remove(r)
        time.sleep(30)

# === 6. ОБРАБОТЧИКИ АДМИНА ===
@bot.message_handler(commands=['ahelp', 'stats', 'top', 'broadcast'])
def admin_commands(m):
    if str(m.chat.id) not in ADMIN_IDS: return
    cmd = m.text.split()[0]
    if cmd == '/ahelp':
        bot.send_message(m.chat.id, "🛠 **Админка:**\n`/stats`, `/top`, `/broadcast [текст]`", parse_mode="Markdown")
    elif cmd == '/stats':
        with sqlite3.connect(DB_FILE) as conn: total = conn.execute("SELECT COUNT(*) FROM users").fetchone()[0]
        bot.send_message(m.chat.id, f"📊 Пользователей: **{total}**", parse_mode="Markdown")
    elif cmd == '/broadcast':
        text = m.text.replace("/broadcast", "").strip()
        if text:
            with sqlite3.connect(DB_FILE) as conn: users = conn.execute("SELECT user_id FROM users").fetchall()
            for u in users:
                try: bot.send_message(u[0], f"📢 **Сообщение от разработчика:**\n\n{text}", parse_mode="Markdown")
                except: pass
            bot.send_message(m.chat.id, "✅ Рассылка завершена!")

# === 7. ОБРАБОТЧИКИ ПОЛЬЗОВАТЕЛЯ ===
@bot.message_handler(commands=['start'])
def start(m):
    settings = get_user_settings(m.chat.id)
    if settings and settings['entity_id']:
        user_data[m.chat.id] = settings
        e_type, e_name = get_entity_info(settings['entity_id'])
        if e_name != "Неизвестно":
            show_main_menu(m.chat.id)
            return
    bot.send_message(m.chat.id, "Привет! Для начала мне нужно задать пару вопросов:\n Кем Вы являетесь в ТГУ?", reply_markup=markup_roles())

@bot.message_handler(commands=['asalways'])
def as_always(m):
    settings = get_user_settings(m.chat.id)
    if not settings or not settings.get('entity_id'): return
    user_data[m.chat.id] = settings 
    today = datetime.date.today()
    mon = today - datetime.timedelta(days=today.weekday()) + datetime.timedelta(days=7)
    df, dt = mon.strftime("%Y-%m-%d"), (mon + datetime.timedelta(days=6)).strftime("%Y-%m-%d")
    load_schedule(m.chat.id, settings['entity_id'], df, dt)
    
@bot.message_handler(commands=['clearchanges'])
def clear_ai_changes(m):
    cid = m.chat.id
    try:
        with sqlite3.connect(DB_FILE) as conn:
            # Сначала считаем, сколько изменений было у пользователя (просто для красивого ответа)
            cursor = conn.execute("SELECT COUNT(*) FROM overrides WHERE user_id=?", (cid,))
            count = cursor.fetchone()[0]
            
            if count > 0:
                # Удаляем все изменения конкретно этого юзера
                conn.execute("DELETE FROM overrides WHERE user_id=?", (cid,))
                bot.reply_to(
                    m, 
                    f"🧹 **Готово!**\nУдалено {count} Ваших изменений (переносов/отмен).\n\nТеперь Ваше расписание снова на 100% совпадает с официальными данными ТГУ.", 
                    parse_mode="Markdown",
                    reply_markup=InlineKeyboardMarkup().add(InlineKeyboardButton("🏠 В меню", callback_data="back_to_main"))
                )
            else:
                bot.reply_to(
                    m, 
                    "У вас пока нет сохраненных изменений от ИИ. Ваше расписание и так официальное! 🎓", 
                    reply_markup=InlineKeyboardMarkup().add(InlineKeyboardButton("🏠 В меню", callback_data="back_to_main"))
                )
    except Exception as e:
        logger.error(f"Ошибка при очистке изменений: {e}")
        bot.reply_to(m, "❌ Произошла ошибка при очистке базы данных. Попробуйте позже.")

@bot.message_handler(func=lambda m: user_state.get(m.chat.id) == "wait_feedback")
def handle_feedback(m):
    user_state[m.chat.id] = None
    info = f"От: {m.from_user.first_name} (@{m.from_user.username} / ID: {m.chat.id})"
    for admin_id in ADMIN_IDS:
        try: bot.send_message(admin_id, f"📩 **Feedback:**\n`{info}`\n\n{m.text}", parse_mode="Markdown")
        except: pass
    bot.reply_to(m, "✅ Отправлено!", reply_markup=InlineKeyboardMarkup().add(InlineKeyboardButton("🏠 В меню", callback_data="back_to_main")))

@bot.message_handler(func=lambda m: user_state.get(m.chat.id) in ["wait_group", "wait_teacher"])
def search_entity_handler(m):
    state, query = user_state[m.chat.id], m.text.strip().lower()
    matches = []
    if state == "wait_group":
        for name, eid in ALL_GROUPS.items():
            if query in name.lower(): matches.append(('group', name, eid))
    else:
        for name, eid in ALL_TEACHERS.items():
            if query in name.lower(): matches.append(('teacher', name, eid))
        
    if not matches: bot.reply_to(m, "❌ Не найдено. Попробуйте еще раз:"); return
    if len(matches) == 1:
        e_type, e_name, e_id = matches[0]
        user_data[m.chat.id] = {'entity_id': e_id, 'subgroups': [], 'excluded': set(), 'short_fio': False, 'reminder': 15, 'emoji_style': 0, 'auto_day': -1, 'auto_time': 8, 'remind_type': 0}
        msg = bot.send_message(m.chat.id, "⏳ Анализ...")
        today = datetime.date.today()
        raw = None
        try: raw = ScheduleCore.fetch_json(e_id, (today - datetime.timedelta(days=7)).strftime("%Y-%m-%d"), (today + datetime.timedelta(days=21)).strftime("%Y-%m-%d"), e_type)
        except: pass
        user_state[m.chat.id] = None
        
        if e_type == "group":
            user_data[m.chat.id]['raw_temp'] = raw
            user_data[m.chat.id]['subgroups_list'] = ScheduleCore.extract_dynamic_subgroups(raw)
            if user_data[m.chat.id]['subgroups_list']: user_data[m.chat.id]['subgroups'] = [user_data[m.chat.id]['subgroups_list'][0]]
            save_user_settings(m.chat.id, user_data[m.chat.id])
            bot.edit_message_text("🎓 **Выберите подгруппу (-ы)**", m.chat.id, msg.message_id, parse_mode="Markdown", reply_markup=markup_subgroups(m.chat.id, True))
        else:
            if raw: user_data[m.chat.id]['all_disc'] = sorted(list(set(e['clean_title'] for e in ScheduleCore.parse_events(raw))))
            save_user_settings(m.chat.id, user_data[m.chat.id])
            bot.edit_message_text("🎨 **Выберите условные обозначения занятий:**", m.chat.id, msg.message_id, parse_mode="Markdown", reply_markup=markup_onb_emoji(m.chat.id))
    elif len(matches) <= 15:
        markup = InlineKeyboardMarkup(row_width=1)
        for et, en, ei in matches: markup.add(InlineKeyboardButton(en, callback_data=f"sel_{'g' if et == 'group' else 't'}_{ei}"))
        bot.send_message(m.chat.id, f"🔍 Выберите:", reply_markup=markup)
    else: bot.reply_to(m, "Найдено слишком много совпадений.")

@bot.message_handler(func=lambda m: str(user_state.get(m.chat.id, "")).startswith("wait_date_"))
def handle_manual_date(m):
    eid = user_state[m.chat.id].replace("wait_date_", "")
    try:
        p = m.text.replace(" ", "").split("-")
        df, dt = datetime.datetime.strptime(p[0], "%d.%m.%Y").strftime("%Y-%m-%d"), datetime.datetime.strptime(p[1], "%d.%m.%Y").strftime("%Y-%m-%d")
        user_state[m.chat.id] = None
        load_schedule(m.chat.id, eid, df, dt)
    except: bot.reply_to(m, "❌ Ожидается: ДД.ММ.ГГГГ - ДД.ММ.ГГГГ")

# === 🧠 ИИ-ОБРАБОТЧИК ДЛЯ ПЕРЕНОСОВ ПАР ===
@bot.message_handler(func=lambda m: m.text and not m.text.startswith('/') and user_state.get(m.chat.id) not in ["wait_group", "wait_teacher", "wait_feedback"])
def handle_smart_message(m):
    cid = m.chat.id
    if not ai_client or cid not in user_data: return
    
    msg = bot.reply_to(m, "🤖 Анализирую расписание...")
    
    s = user_data[cid]
    e_type, _ = get_entity_info(s['entity_id'])
    
    today = datetime.date.today()
    df = today.strftime("%Y-%m-%d")
    dt = (today + datetime.timedelta(days=7)).strftime("%Y-%m-%d")
    
    raw = ScheduleCore.fetch_json(s['entity_id'], df, dt, e_type)
    events = ScheduleCore.parse_events(raw) if raw else []
    
    schedule_context = "ОФИЦИАЛЬНОЕ РАСПИСАНИЕ ПОЛЬЗОВАТЕЛЯ НА БЛИЖАЙШИЕ ДНИ:\n"
    if not events:
        schedule_context += "Пар не найдено.\n"
    else:
        user_subs = [sub.lower() for sub in s.get('subgroups', [])]
        type_map = {"LECTURE": "Лекция", "PRACTICE": "Практика", "LABORATORY": "Лабораторная", "SEMINAR": "Семинар"}
        for ev in events:
            if ev['clean_title'].lower() in s['excluded']: continue
            if e_type == "group" and ev['type'] == "LESSON":
                keep = False
                for g in ev.get('groups_data', []):
                    g_name = g.get("name", "").lower()
                    if not g.get("isSubgroup", False): keep = True; break
                    for sub in user_subs:
                        if f"({sub})" in g_name: keep = True; break
                    if keep: break
                if not keep: continue
                
            date_str = ev['dtstart'].strftime("%Y-%m-%d")
            date_ru = ev['dtstart'].strftime("%d.%m.%Y")
            time_str = ev['dtstart'].strftime("%H:%M")
            ru_type = type_map.get(ev.get('lesson_type', ''), 'Пара')
            sys_type = ev.get('lesson_type', 'LECTURE') 
            teacher = ev.get('teachers', 'Не назначен')
            schedule_context += f"- {date_ru} ({date_str}) в {time_str} | {ev['clean_title']} (Системный тип: {sys_type}, Вывод: {ru_type}) | Ауд: {ev['short_loc']} | Преп: {teacher}\n"

    history = user_state.get(f"ai_history_{cid}", "")
    user_text = f"История переписки:\n{history}\n\nНовый ответ: {m.text}" if history else f"Сообщение пользователя: {m.text}"

    system_prompt = f"""
    Ты умный ассистент студента. Сегодня {today.strftime('%d.%m.%Y')}.
    Твоя задача — сопоставить сообщение пользователя с его РЕАЛЬНЫМ расписанием и сформировать JSON-ответ.
    
    {schedule_context}
    
    ПРАВИЛА АНАЛИЗА:
    1. ИДЕНТИФИКАЦИЯ ПАРЫ: Внимательно сверяй сообщение с расписанием. Если в один день стоит НЕСКОЛЬКО пар с одинаковым названием (например, лекция и практика), а пользователь не уточнил, какую именно менять/отменять — СТРОГО возвращай "status": "clarify".
    2. УТОЧНЕНИЕ ("clarify"): Если не хватает данных (не ясна дата, точный предмет, тип пары или время), верни "status": "clarify". В поле "message" задай короткий, вежливый вопрос и ОБЯЗАТЕЛЬНО перечисли доступные варианты из расписания (например: "Какую именно пару отменить: лекцию в 10:35 или практику в 14:15?").
    3. УСПЕХ ("success"): Если однозначно понятно, о какой паре речь, верни "status": "success".
    4. ТИП ПАРЫ ("new_type"): СТРОГО используй системные значения: LECTURE, PRACTICE, LABORATORY, SEMINAR, EXAM, CONTROL_WORK, CONSULTATION. Если пара переносится/заменяется, скопируй тип из старой пары, либо определи по контексту (например, "лаба" = LABORATORY).
    5. ВРЕМЯ ("old_time"): ОБЯЗАТЕЛЬНО скопируй точное время старой пары (HH:MM) из расписания. Это критически важно для удаления нужной пары.
    6. СОХРАНЕНИЕ ДАННЫХ: Если аудитория, преподаватель или время не меняются — скопируй их из исходного расписания в новые поля.
    
    ВЫВОД СТРОГО В JSON (начиная с {{ и заканчивая }}):
    {{
        "status": "success" или "clarify",
        "message": "Твой уточняющий вопрос с вариантами (или null, если success)",
        "action": "cancel", "replace" или "add",
        "date": "YYYY-MM-DD",
        "date_ru": "ДД.ММ.ГГГГ",
        "old_subject": "Точное название предмета из расписания (или null при add)",
        "old_time": "HH:MM (время отменяемой/заменяемой пары из расписания) или null",
        "new_subject": "Название новой пары (без указания типа и слова изменено) или null",
        "new_type": "СТРОГО СИСТЕМНОЕ ЗНАЧЕНИЕ (LECTURE, PRACTICE и т.д.) или null",
        "new_teacher": "ФИО преподавателя или null",
        "new_time": "HH:MM (новое время) или null",
        "new_loc": "Номер аудитории или null"
    }}
    """
    
    try:
        response = ai_client.chat.completions.create(
            model=os.getenv("AI_MODEL", "google/gemini-3.1-flash-lite-preview"),
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_text}
            ],
            temperature=0.1
        )
        
        raw_text = response.choices[0].message.content or ""
        match = re.search(r'\{.*\}', raw_text, re.DOTALL)
        clean_json = match.group(0) if match else raw_text.replace('```json', '').replace('```', '').strip()
            
        try:
            data = json.loads(clean_json)
        except json.JSONDecodeError:
            logger.error(f"❌ ИИ вернул не валидный JSON: '{raw_text}'")
            bot.edit_message_text("🤖 Кажется, нейросеть запуталась. Попробуй перефразировать.", cid, msg.message_id)
            return
        
        if data.get('status') == 'clarify':
            user_state[f"ai_history_{cid}"] = user_text + f"\nТвой вопрос: {data.get('message')}"
            bot.edit_message_text(f"🤖 **Уточнение:**\n{data.get('message')}", cid, msg.message_id, parse_mode="Markdown")
            return
            
        user_state.pop(f"ai_history_{cid}", None)
        
        text = f"✅ **Всё понял!**\n📅 {data.get('date_ru')}\n"
        if data.get('action') == 'cancel': 
            text += f"Убираем: {data.get('old_subject')}"
        else: 
            type_map_ru = {"LECTURE": "Лекция", "PRACTICE": "Практика", "LABORATORY": "Лаба", "SEMINAR": "Семинар"}
            ru_type = type_map_ru.get(data.get('new_type'), 'Пара')
            
            teacher_str = f"\nПреподаватель: {data.get('new_teacher')}" if data.get('new_teacher') else ""
            loc_str = data.get('new_loc') or "Не указано"
            
            clean_new_sub = data.get('new_subject', '').strip()
            if clean_new_sub: clean_new_sub = clean_new_sub[0].upper() + clean_new_sub[1:]
            
            text += f"🆕 Изменяем на: {clean_new_sub} ({ru_type}) в {data.get('new_time')}\nАуд.: {loc_str} ⚠️{teacher_str}"
            if data.get('old_subject'):
                text += f"\n*(Вместо: {data.get('old_subject')})*"
            
        user_state[f"ai_{cid}"] = data
        kb = InlineKeyboardMarkup().add(InlineKeyboardButton("💾 Сохранить изменения", callback_data="ai_save"), InlineKeyboardButton("Отмена", callback_data="back_to_main"))
        bot.edit_message_text(text, cid, msg.message_id, reply_markup=kb, parse_mode="Markdown")
        
    except Exception as e: 
        logger.error(f"AI Request Error: {e}")
        bot.edit_message_text("❌ Ошибка при связи с сервером ИИ. Попробуйте чуть позже.", cid, msg.message_id)

def load_schedule(cid, eid, df, dt, mid=None):
    text = "⏳ Получаю расписание..."
    if mid: bot.edit_message_text(text, cid, mid)
    else: msg = bot.send_message(cid, text); mid = msg.message_id
    e_type, e_name = get_entity_info(eid)
    try:
        raw = ScheduleCore.fetch_json(eid, df, dt, e_type)
        if not raw or not raw.get('grid'):
            bot.edit_message_text("❌ Пусто.", cid, mid, reply_markup=InlineKeyboardMarkup().add(InlineKeyboardButton("⬅️ Назад", callback_data="back_to_main")))
            return
            
        events = apply_ai_overrides(ScheduleCore.parse_events(raw), cid, df, dt) 
        
        if events:
            user_data[cid]['name'] = f"TSU_{e_name}_{df}.ics"
            f_ical, count = ScheduleCore.generate_ics(events, user_data[cid], e_type)
            bot.delete_message(cid, mid)
            bot.send_document(cid, (user_data[cid]['name'], f_ical), caption=f"📊 В расписании найдено {count} занятий.\nЗагружайте расписание!", reply_markup=markup_download())
        else: bot.edit_message_text("❌ Пусто.", cid, mid, reply_markup=InlineKeyboardMarkup().add(InlineKeyboardButton("⬅️ Назад", callback_data="back_to_main")))
    except: bot.edit_message_text("❌ Ошибка.", cid, mid, reply_markup=InlineKeyboardMarkup().add(InlineKeyboardButton("⬅️ Назад", callback_data="back_to_main")))

@bot.callback_query_handler(func=lambda c: True)
def cb(c):
    cid = c.message.chat.id
    if cid not in user_data and get_user_settings(cid): user_data[cid] = get_user_settings(cid)
    
    if c.data == "ai_save":
        data = user_state.get(f"ai_{cid}")
        if data:
            with sqlite3.connect(DB_FILE) as conn:
                conn.execute("INSERT INTO overrides (user_id, target_date, action, old_subject, new_subject, new_time, new_loc, new_type, new_teacher, old_time) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                             (cid, data.get('date'), data.get('action'), data.get('old_subject'), data.get('new_subject'), data.get('new_time'), data.get('new_loc'), data.get('new_type'), data.get('new_teacher'), data.get('old_time')))
            bot.edit_message_text("✅ Изменения сохранены! Скачайте расписание, чтобы они применились.", cid, c.message.message_id, reply_markup=InlineKeyboardMarkup().add(InlineKeyboardButton("🏠 В меню", callback_data="back_to_main")))
            user_state.pop(f"ai_{cid}", None)
            
    elif c.data.startswith("tsub_"):
        _, sub, is_onb_str = c.data.split("_")
        is_onb = bool(int(is_onb_str))
        subs = user_data[cid].get('subgroups', [])
        if sub in subs: subs.remove(sub)
        else: subs.append(sub)
        user_data[cid]['subgroups'] = subs
        save_user_settings(cid, user_data[cid])
        bot.edit_message_reply_markup(cid, c.message.message_id, reply_markup=markup_subgroups(cid, is_onb))

    # --- ШАГИ ОНБОРДИНГА ---
    elif c.data == "onb_step_disc":
        raw = user_data[cid].pop('raw_temp', None)
        update_disciplines_for_subgroup(cid, raw, "group")
        save_user_settings(cid, user_data[cid])
        bot.edit_message_text("📚 **Выберите предметы, которые в дальнейшем будут исключены из расписания: \nВыбранные дисциплины можно будет изменить в любой момент", cid, c.message.message_id, parse_mode="Markdown", reply_markup=markup_disc(cid, True))

    elif c.data == "onb_step_emoji": 
        bot.edit_message_text("🎨 **Здесь можно выбрать условные обозначения для пар в календаре:", cid, c.message.message_id, parse_mode="Markdown", reply_markup=markup_onb_emoji(cid))
    
    elif c.data == "onb_step_remind":
        bot.edit_message_text("⏱ Здесь можно настроить время уведомлений о ближайшем занятии", cid, c.message.message_id, parse_mode="Markdown", reply_markup=markup_onb_remind(cid))
        
    elif c.data == "onb_step_rtype":
        bot.edit_message_text("🔔 **Выберите источник уведомлений:", cid, c.message.message_id, parse_mode="Markdown", reply_markup=markup_onb_rtype(cid))
        
    elif c.data == "onb_step_auto":
        bot.edit_message_text("📬 **Я могу еженедельно отправлять персонализированный файл с расписанием в чат . Остается только выбрать время:", cid, c.message.message_id, parse_mode="Markdown", reply_markup=markup_onb_auto(cid))

    elif c.data == "onb_step_ai":
        ai_text = "🤖  Сообщение от разработчика.\nНа данный момент тестируется исопльзование ИИ в боте с целью изменения расписания на следующую неделю в случае переноса/отмены занятия. \nПри случае, попробуйте переслать боту сообщение (например, от старосты) о переносе пары и проверьте функционал. \nБуду рад получить обратную связь"
        bot.edit_message_text(ai_text, cid, c.message.message_id, parse_mode="Markdown", reply_markup=markup_onb_ai())

    elif c.data == "onboard_finish":
        bot.edit_message_text("🎉 **Настройка завершена!** Выберите период:", cid, c.message.message_id, parse_mode="Markdown", reply_markup=markup_dates(user_data[cid]['entity_id']))

    elif c.data == "role_student": user_state[cid] = "wait_group"; bot.edit_message_text("Напишите в чат номер своей группы \n(Например: 012345):", cid, c.message.message_id)
    elif c.data == "role_teacher": user_state[cid] = "wait_teacher"; bot.edit_message_text("Введите в чат Вашу фамилию:", cid, c.message.message_id)
    
    elif c.data.startswith("sel_g_") or c.data.startswith("sel_t_"):
        eid = c.data[6:]
        e_type = 'group' if c.data.startswith("sel_g_") else 'teacher'
        user_data[cid] = {'entity_id': eid, 'subgroups': [], 'excluded': set(), 'short_fio': False, 'reminder': 15, 'emoji_style': 0, 'auto_day': -1, 'auto_time': 8, 'remind_type': 0}
        today = datetime.date.today()
        raw = None
        try: raw = ScheduleCore.fetch_json(eid, (today - datetime.timedelta(days=7)).strftime("%Y-%m-%d"), (today + datetime.timedelta(days=21)).strftime("%Y-%m-%d"), e_type)
        except: pass
        
        if e_type == "group":
            user_data[cid]['raw_temp'] = raw
            user_data[cid]['subgroups_list'] = ScheduleCore.extract_dynamic_subgroups(raw)
            if user_data[cid]['subgroups_list']: user_data[cid]['subgroups'] = [user_data[cid]['subgroups_list'][0]]
            save_user_settings(cid, user_data[cid])
            bot.edit_message_text("🎓 **Выберите подгруппы**", cid, c.message.message_id, parse_mode="Markdown", reply_markup=markup_subgroups(cid, True))
        else:
            save_user_settings(cid, user_data[cid])
            bot.edit_message_text("🎨 **Условные обозначения**", cid, c.message.message_id, parse_mode="Markdown", reply_markup=markup_onb_emoji(cid))

    elif c.data == "menu_dates": bot.edit_message_text("Период:", cid, c.message.message_id, reply_markup=markup_dates(user_data[cid]['entity_id']))
    elif c.data == "menu_profile": bot.edit_message_text("Профиль:", cid, c.message.message_id, reply_markup=markup_profile(cid, get_entity_info(user_data[cid]['entity_id'])[0]))
    
    elif c.data == "menu_disc": 
        try: bot.edit_message_text("⏳ Обновляю список предметов...", cid, c.message.message_id)
        except: pass
        try:
            e_type = get_entity_info(user_data[cid]['entity_id'])[0]
            raw = ScheduleCore.fetch_json(user_data[cid]['entity_id'], (datetime.date.today() - datetime.timedelta(days=7)).strftime("%Y-%m-%d"), (datetime.date.today() + datetime.timedelta(days=21)).strftime("%Y-%m-%d"), e_type)
            update_disciplines_for_subgroup(cid, raw, e_type)
        except: pass
        try: bot.edit_message_text("Предметы:", cid, c.message.message_id, reply_markup=markup_disc(cid, False))
        except: pass
        
    elif c.data == "menu_prefs": bot.edit_message_text("Настройки:", cid, c.message.message_id, reply_markup=markup_prefs(cid))
    elif c.data == "menu_subgroups": bot.edit_message_text("Подгруппы:", cid, c.message.message_id, reply_markup=markup_subgroups(cid, False))
    elif c.data == "menu_feedback": user_state[cid] = "wait_feedback"; bot.edit_message_text("Напишите сообщение:", cid, c.message.message_id)

    elif c.data == "reset_profile":
        with sqlite3.connect(DB_FILE) as conn: conn.execute("DELETE FROM users WHERE user_id=?", (cid,))
        user_data.pop(cid, None)
        bot.edit_message_text("Кто вы?", cid, c.message.message_id, reply_markup=markup_roles())

    elif c.data.startswith("date_"):
        _, p, eid = c.data.split("_", 2)
        if p == "manual": user_state[cid] = f"wait_date_{eid}"; bot.send_message(cid, "ДД.ММ.ГГГГ - ДД.ММ.ГГГГ:")
        else:
            mon = datetime.date.today() - datetime.timedelta(days=datetime.date.today().weekday())
            if p == "next": mon += datetime.timedelta(days=7)
            load_schedule(cid, eid, mon.strftime("%Y-%m-%d"), (mon + datetime.timedelta(days=6)).strftime("%Y-%m-%d"), c.message.message_id)

    elif c.data.startswith("toggle_"):
        s = user_data[cid]
        is_onb = c.data.endswith("_onb")
        cmd = c.data.replace("_onb", "")
        if cmd == "toggle_remind": s['reminder'] = 10 if s.get('reminder', 15) == 15 else (0 if s.get('reminder', 15) == 10 else 15)
        elif cmd == "toggle_fio": s['short_fio'] = not s.get('short_fio', False)
        elif cmd == "toggle_emoji": s['emoji_style'] = 1 if s.get('emoji_style', 0) == 0 else 0
        elif cmd == "toggle_aday": s['auto_day'] = 5 if s.get('auto_day', -1) == -1 else (6 if s.get('auto_day', -1) == 5 else -1)
        elif cmd == "toggle_atime": s['auto_time'] = 13 if s.get('auto_time', 8) == 8 else (19 if s.get('auto_time', 8) == 13 else 8)
        elif cmd == "toggle_rtype": s['remind_type'] = 1 if s.get('remind_type', 0) == 0 else 0
        save_user_settings(cid, s)
        
        if is_onb:
            if cmd == "toggle_emoji": mk = markup_onb_emoji(cid)
            elif cmd == "toggle_remind": mk = markup_onb_remind(cid)
            elif cmd == "toggle_rtype": mk = markup_onb_rtype(cid)
            else: mk = markup_onb_auto(cid)
            bot.edit_message_reply_markup(cid, c.message.message_id, reply_markup=mk)
        else:
            bot.edit_message_reply_markup(cid, c.message.message_id, reply_markup=markup_prefs(cid))

    elif c.data.startswith("d_") or c.data.startswith("do_"):
        is_onb = c.data.startswith("do_")
        idx = int(c.data.split('_')[1])
        if 'all_disc' not in user_data[cid] or idx >= len(user_data[cid]['all_disc']):
            bot.answer_callback_query(c.id, "⚠️ Ошибка. Вернитесь в меню.", show_alert=True); return
        name = user_data[cid]['all_disc'][idx].lower()
        if name in user_data[cid]['excluded']: user_data[cid]['excluded'].remove(name)
        else: user_data[cid]['excluded'].add(name)
        save_user_settings(cid, user_data[cid])
        bot.edit_message_reply_markup(cid, c.message.message_id, reply_markup=markup_disc(cid, is_onb))

    elif c.data == "help_save": bot.edit_message_text("ОС:", cid, c.message.message_id, reply_markup=markup_os())
    elif c.data in ["help_android", "help_ios", "help_windows"]:
        texts = {"help_android": "Открой через Google Календарь.", "help_ios": "Для iOS в разработке.", "help_windows": "Импорт в веб-версии."}
        bot.edit_message_text(texts[c.data], cid, c.message.message_id, reply_markup=InlineKeyboardMarkup().add(InlineKeyboardButton("⬅️ Назад", callback_data="help_save")))
    elif c.data == "back_to_download": bot.edit_message_text("Готово!", cid, c.message.message_id, reply_markup=markup_download())
    
    elif c.data == "back_to_main": 
        user_state[cid] = None
        if c.message.content_type == 'document':
            try: bot.delete_message(cid, c.message.message_id)
            except: pass
            show_main_menu(cid)
        else:
            show_main_menu(cid, c.message.message_id)
            
    bot.answer_callback_query(c.id)

if __name__ == "__main__":
    init_db()
    load_databases()
    threading.Thread(target=auto_mailing_loop, daemon=True).start()
    threading.Thread(target=bot_alerts_loop, daemon=True).start()  
    bot.infinity_polling(timeout=10, long_polling_timeout=5)
