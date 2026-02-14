# -*- coding: utf-8 -*-
# Telegram Bot для поиска фильмов через Кинопоиск
# @K1p1k | Загружено с TG @KiTools

import asyncio
import re
import json
import sqlite3
import logging
from time import time
from random import randint
from datetime import datetime, timedelta

import requests
from bs4 import BeautifulSoup
from aiogram import Bot, Dispatcher, types, executor
from aiogram.contrib.fsm_storage.memory import MemoryStorage
from aiogram.dispatcher import FSMContext
from aiogram.dispatcher.filters import BoundFilter, CommandStart
from aiogram.dispatcher.filters.state import State, StatesGroup
from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup, ReplyKeyboardMarkup, KeyboardButton

# ==================== КОНФИГУРАЦИЯ ====================
token = '8189356827:AAFz5RM1NhYMf5ycn9STeSha2h1uqBRCC2E'  # Вставьте токен бота от @BotFather
admin_id = [5858391454]  # ID администраторов [12345, 67890]
rate_searsh = 1  # Задержка между поисками в секундах
bot_version = '1.9'

# ==================== ИНИЦИАЛИЗАЦИЯ БОТА ====================
storage = MemoryStorage()
bot = Bot(token)
dp = Dispatcher(bot, storage=storage)
logging.basicConfig(level=logging.INFO)

# ==================== СОСТОЯНИЯ ====================
class UserState(StatesGroup):
    search_film = State()  # Состояние поиска фильма

class AdminState(StatesGroup):
    myling_list_text = State()
    myling_list_add_ikb_text = State()
    myling_list_add_ikb_url = State()
    add_film_code = State()
    add_film_name = State()
    add_film_priew = State()
    delete_film_code = State()
    add_chennel_username = State()
    delete_chennel_username = State()
    chennger_kbname_player_text = State()
    chennger_wellcome_text = State()
    chennger_film_text = State()
    import_cfg_file = State()

# ==================== БАЗА ДАННЫХ ====================
class Database:
    def __init__(self):
        self.sql = sqlite3.connect('DataBase.db')
        self.cs = self.sql.cursor()
        self.create_tables()
    
    def create_tables(self):
        # Таблица пользователей
        self.cs.execute("""CREATE TABLE IF NOT EXISTS user_data(
            user_id INTEGER PRIMARY KEY,
            user_menotion TEXT,
            user_error_link_complaint_unix INTEGER,
            user_unix INTEGER
        )""")
        
        # Таблица фильмов
        self.cs.execute("""CREATE TABLE IF NOT EXISTS films_data(
            films_code TEXT PRIMARY KEY,
            films_name TEXT,
            films_priv TEXT,
            films_id INTEGER
        )""")
        
        # Таблица каналов
        self.cs.execute("""CREATE TABLE IF NOT EXISTS chennel_data(
            chennel_identifier TEXT PRIMARY KEY,
            chennel_name TEXT,
            chennel_link TEXT
        )""")
        
        # Таблица плееров
        self.cs.execute("""CREATE TABLE IF NOT EXISTS player_data(
            player_web TEXT,
            player_name TEXT PRIMARY KEY,
            switch BOOL,
            kb_name TEXT
        )""")
        
        # Таблица текстов
        self.cs.execute("""CREATE TABLE IF NOT EXISTS text_data(
            text_type TEXT PRIMARY KEY,
            text_text TEXT
        )""")
        
        # Таблица поиска
        self.cs.execute("""CREATE TABLE IF NOT EXISTS search_data(
            search_film TEXT PRIMARY KEY,
            search_count INTEGER
        )""")
        
        # Таблица франшиз
        self.cs.execute("""CREATE TABLE IF NOT EXISTS franchise_data(
            franchise_obj TEXT PRIMARY KEY,
            franchise_description TEXT
        )""")
        
        # Таблица избранного
        self.cs.execute("""CREATE TABLE IF NOT EXISTS favourites_data(
            favourites_uid INTEGER,
            favourites_id TEXT
        )""")
        
        # Таблица названий фильмов
        self.cs.execute("""CREATE TABLE IF NOT EXISTS films_names(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT
        )""")
        
        self.sql.commit()
        self.init_default_data()
    
    def init_default_data(self):
        # Добавление плееров по умолчанию
        try:
            self.cs.execute("INSERT INTO player_data VALUES(?, ?, ?, ?)", 
                           ['https://ww5.frkp.lol', 'frkp', 1, 'Смотреть #1▶️'])
            self.sql.commit()
        except:
            pass
        
        try:
            self.cs.execute("INSERT INTO player_data VALUES(?, ?, ?, ?)", 
                           ['www.ggkinopoisk.ru', 'vavada', 0, 'Смотреть #2▶️'])
            self.sql.commit()
        except:
            pass
        
        try:
            self.cs.execute("INSERT INTO player_data VALUES(?, ?, ?, ?)", 
                           ['https://www.freekinopoisk.ru', 'frkp2', 1, 'Смотреть #3▶️'])
            self.sql.commit()
        except:
            pass
        
        # Добавление текстов по умолчанию
        try:
            self.cs.execute("INSERT INTO text_data VALUES(?, ?)", 
                           ['wellcome', '<b>👋Привет <a href="tg://user?id={user_id}">{full_name}</a> \n🎬Ты попал в лучшего бота по просмотру фильмов и сериалов\n🎥Вводи название фильма или код и наслаждайся</b>'])
            self.sql.commit()
        except:
            pass
        
        try:
            self.cs.execute("INSERT INTO text_data VALUES(?, ?)", 
                           ['film', '<b>👤От: {username_bot}\n🎥Название: {film_name}</b>'])
            self.sql.commit()
        except:
            pass
        
        try:
            self.cs.execute("INSERT INTO text_data VALUES(?, ?)", 
                           ['franchise', '<b>👋Привет <a href="tg://user?id={user_id}">{full_name}</a>\nСпасибо что интересовался нашими проектами❤️</b>\n\n{chapter}'])
            self.sql.commit()
        except:
            pass
    
    async def only_list(self, kortage):
        return [i[0] for i in kortage]
    
    async def get_AllText(self, type='*'):
        self.cs.execute(f"SELECT {type} FROM text_data")
        return self.cs.fetchall()
    
    async def add_filmname(self, name):
        self.cs.execute("INSERT INTO films_names(name) VALUES(?)", [name])
        self.sql.commit()
        return self.cs.lastrowid
    
    async def get_filmname(self, id):
        self.cs.execute(f"SELECT * FROM films_names WHERE id = {id}")
        return self.cs.fetchone()
    
    async def add_user(self, user_id, user_menotion):
        try:
            self.cs.execute("INSERT INTO user_data VALUES(?, ?, ?, ?)", 
                           [user_id, user_menotion, None, time()])
            self.sql.commit()
        except:
            pass
    
    async def get_AllUser(self, type='*'):
        self.cs.execute(f"SELECT {type} FROM user_data")
        return self.cs.fetchall()
    
    async def add_film(self, code, name, priv, id):
        self.cs.execute("INSERT INTO films_data VALUES(?, ?, ?, ?)", [code, name, priv, id])
        self.sql.commit()
    
    async def get_AllFilms(self, type='*'):
        self.cs.execute(f"SELECT {type} FROM films_data")
        return self.cs.fetchall()
    
    async def get_films(self, code):
        self.cs.execute(f"SELECT * FROM films_data WHERE films_code = '{code}'")
        return self.cs.fetchall()
    
    async def delete_Film(self, code):
        self.cs.execute(f"SELECT films_code FROM films_data WHERE films_code = '{code}'")
        if not self.cs.fetchall():
            return False
        self.cs.execute(f"DELETE FROM films_data WHERE films_code = '{code}'")
        self.sql.commit()
        return True
    
    async def get_error_link_complaint_unix(self, user_id):
        self.cs.execute(f"SELECT user_error_link_complaint_unix FROM user_data WHERE user_id = {user_id}")
        result = self.cs.fetchone()
        return result[0] if result else None
    
    async def update_error_link_complaint_unix(self, user_id, time_ub):
        self.cs.execute(f"UPDATE user_data SET user_error_link_complaint_unix = {time_ub} WHERE user_id = {user_id}")
        self.sql.commit()
    
    async def add_Chennel(self, chennel_identifier, name, link):
        self.cs.execute("INSERT INTO chennel_data VALUES(?, ?, ?)", [chennel_identifier, name, link])
        self.sql.commit()
    
    async def get_AllChennel(self, type='*'):
        self.cs.execute(f"SELECT {type} FROM chennel_data")
        return self.cs.fetchall()
    
    async def update_nameChennel(self, chennel_identifier, name):
        self.cs.execute(f"UPDATE chennel_data SET chennel_name = '{name}' WHERE chennel_identifier = '{chennel_identifier}'")
        self.sql.commit()
    
    async def delete_Chennel(self, chennel_identifier):
        self.cs.execute(f"SELECT * FROM chennel_data WHERE chennel_identifier = '{chennel_identifier}'")
        if not self.cs.fetchall():
            return False
        self.cs.execute(f"DELETE FROM chennel_data WHERE chennel_identifier = '{chennel_identifier}'")
        self.sql.commit()
        return True
    
    async def get_Allplayer(self, type='*'):
        self.cs.execute(f"SELECT {type} FROM player_data")
        return self.cs.fetchall()
    
    async def swich_player(self, player_name):
        self.cs.execute(f"SELECT switch FROM player_data WHERE player_name = '{player_name}'")
        data_swich = self.cs.fetchone()
        if data_swich:
            edit = 0 if data_swich[0] == 1 else 1
            self.cs.execute(f"UPDATE player_data SET switch = {edit} WHERE player_name = '{player_name}'")
            self.sql.commit()
    
    async def update_kbname_player(self, player_name, kb):
        self.cs.execute(f"UPDATE player_data SET kb_name = '{kb}' WHERE player_name = '{player_name}'")
        self.sql.commit()
    
    async def get_text(self, type, text_type):
        self.cs.execute(f"SELECT {type} FROM text_data WHERE text_type = '{text_type}'")
        return self.cs.fetchall()
    
    async def update_wellcome_text(self, text, text_type):
        self.cs.execute(f"UPDATE text_data SET text_text = '{text}' WHERE text_type = '{text_type}'")
        self.sql.commit()
    
    async def add_historyInSearch(self, name):
        try:
            self.cs.execute("INSERT INTO search_data VALUES(?, ?)", [name, 1])
        except:
            self.cs.execute(f"UPDATE search_data SET search_count = search_count + 1 WHERE search_film = '{name}'")
        self.sql.commit()
    
    async def get_AllSearch(self, type='*'):
        self.cs.execute(f"SELECT {type} FROM search_data")
        return self.cs.fetchall()
    
    async def get_AllFranchise(self, type='*'):
        self.cs.execute(f"SELECT {type} FROM franchise_data")
        return self.cs.fetchall()
    
    async def add_favourite(self, user_id, name):
        self.cs.execute(f"SELECT * FROM favourites_data WHERE favourites_uid = {user_id} and favourites_id = {name}")
        if self.cs.fetchall():
            return False
        self.cs.execute("INSERT INTO favourites_data VALUES(?, ?)", [user_id, name])
        self.sql.commit()
        return True
    
    async def delete_favourite(self, user_id, name):
        self.cs.execute(f"DELETE FROM favourites_data WHERE favourites_uid = {user_id} and favourites_id = {name}")
        self.sql.commit()
    
    async def get_Allfavourite(self, type='*'):
        self.cs.execute(f"SELECT {type} FROM favourites_data")
        return self.cs.fetchall()
    
    async def get_UserAllFavourites(self, user_id):
        self.cs.execute(f"SELECT * FROM favourites_data WHERE favourites_uid = {user_id}")
        return self.cs.fetchall()
    
    async def get_UserFavouritesWfilm(self, user_id, name):
        self.cs.execute(f"SELECT * FROM favourites_data WHERE favourites_uid = {user_id} and favourites_id = {name}")
        return self.cs.fetchall()

# Инициализация БД
db = Database()

# ==================== ПАРСЕР КИНОПОИСКА ====================
class FilmParser:
    @staticmethod
    async def search(name_film):
        url = f'https://www.kinopoisk.ru/index.php?kp_query={name_film}'
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        request = requests.get(url, headers=headers)
        request.encoding = 'utf-8'
        soup = BeautifulSoup(request.text, "html.parser")
        
        element = soup.find(class_='element most_wanted')
        if not element:
            raise Exception("Фильм не найден")
        
        class FilmData:
            pass
        
        film_data = FilmData()
        film_data.id_ = element.find(class_='pic').find('a')['data-id']
        film_data.name_film_ = element.find(class_='pic').find('img')['alt']
        film_data.year_ = element.find(class_='year').get_text()
        film_data.type_kino_ = 'Фильм' if element.find(class_='pic').find('a')['data-type'] == 'film' else 'Сериал'
        film_data.photo_ = f'https://st.kp.yandex.net/images/film_iphone/iphone360_{film_data.id_}.jpg'
        
        info = element.find(class_='info')
        gray_elements = info.find_all(class_='gray')
        
        if len(gray_elements) > 1:
            film_data.genre_ = gray_elements[1].get_text().split('\n')[1] if '\n' in gray_elements[1].get_text() else gray_elements[1].get_text()
        
        if len(gray_elements) > 1:
            film_data.director_ = gray_elements[1].get_text().split('\n')[0] if '\n' in gray_elements[1].get_text() else gray_elements[1].get_text()
        
        if len(gray_elements) > 2:
            film_data.text_autor_ = gray_elements[2].get_text()
        else:
            film_data.text_autor_ = 'Режиссер'
        
        length_text = gray_elements[0].get_text() if gray_elements else ''
        length_match = re.search(r'\b\d+\b', length_text)
        film_data.length_ = length_match.group() if length_match else '0'
        
        return film_data

film_parser = FilmParser()

# ==================== КЛАВИАТУРЫ ====================
class Keyboards:
    def __init__(self):
        # Общие кнопки
        self.ikb_back_oikb = InlineKeyboardButton(text='Отмена❌', callback_data='cancellation_state')
        self.ikb_back = InlineKeyboardMarkup().row(self.ikb_back_oikb)
        
        self.ikb_close_oikb = InlineKeyboardButton(text='Закрыть❎', callback_data='close_text')
        self.ikb_close = InlineKeyboardMarkup().row(self.ikb_close_oikb)
        
        # Админ меню
        self.admin_menu_main = InlineKeyboardMarkup(row_width=2)
        self.admin_menu_main.row(InlineKeyboardButton(text='Сделать рассылку📬', callback_data='myling_list_start_admin'))
        self.admin_menu_main.row(InlineKeyboardButton(text='Списки🗒', callback_data='list_data_admin'))
        self.admin_menu_main.row(InlineKeyboardButton(text='Добавить фильм📌', callback_data='add_film_admin'))
        self.admin_menu_main.insert(InlineKeyboardButton(text='Удалить фильм🗑', callback_data='delete_film_admin'))
        self.admin_menu_main.row(InlineKeyboardButton(text='Добавить канал➕', callback_data='add_chennel_admin'))
        self.admin_menu_main.insert(InlineKeyboardButton(text='Удалить канал➖', callback_data='delete_chennel_admin'))
        self.admin_menu_main.row(InlineKeyboardButton(text='Проверка каналов⚛️', callback_data='check_chennel_admin'))
        self.admin_menu_main.row(InlineKeyboardButton(text='Плееры▶️', callback_data='player_settings_admin'))
        self.admin_menu_main.row(InlineKeyboardButton(text='Текста📝', callback_data='text_settings_admin'))
        self.admin_menu_main.row(self.ikb_close_oikb)
        
        # Меню списков
        self.admin_menu_list = InlineKeyboardMarkup(row_width=1)
        self.admin_menu_list.row(InlineKeyboardButton(text='Пользователи👥', callback_data='list_users_admin'))
        self.admin_menu_list.row(InlineKeyboardButton(text='Фильмы🎥', callback_data='list_films_admin'))
        self.admin_menu_list.row(InlineKeyboardButton(text='Каналы📢', callback_data='list_chennel_admin'))
        self.admin_menu_list.row(InlineKeyboardButton(text='Назад⬅️', callback_data='back_main_menu_admin'))
        
        # Меню текстов
        self.admin_menu_text = InlineKeyboardMarkup(row_width=1)
        self.admin_menu_text.row(InlineKeyboardButton(text='Приветствие', callback_data='chenneger_wellcome_text_settings_admin'))
        self.admin_menu_text.row(InlineKeyboardButton(text='Фильм', callback_data='chenneger_film_text_settings_admin'))
        self.admin_menu_text.row(InlineKeyboardButton(text='Назад⬅️', callback_data='back_main_menu_admin'))
    
    async def kb_user(self, user_id):
        kb = ReplyKeyboardMarkup(row_width=2, resize_keyboard=True)
        kb.row('Поиск🔍')
        kb.insert('Избранное🌟')
        if user_id in admin_id:
            kb.insert('Админ меню')
        return kb
    
    def kb_back(self):
        kb = ReplyKeyboardMarkup(resize_keyboard=True)
        kb.row('Отмена❌')
        return kb
    
    async def get_Favourites_kb(self, user_id):
        ikb = InlineKeyboardMarkup(row_width=1)
        data_Favourites_user = await db.get_UserAllFavourites(user_id)
        for i in data_Favourites_user:
            name = await db.get_filmname(i[1])
            if name:
                ikb.row(InlineKeyboardButton(name[1] + '🌟', callback_data='search_film_' + str(i[1])))
        ikb.row(self.ikb_close_oikb)
        return ikb
    
    async def sub_list(self):
        data_chennel = await db.get_AllChennel()
        sub_list = InlineKeyboardMarkup(row_width=1)
        for i in data_chennel:
            sub_list.add(InlineKeyboardButton(text=i[1], url=i[2]))
        sub_list.add(InlineKeyboardButton(text='Одна из ссылок не работает❓', callback_data='link_no_work'))
        return sub_list
    
    async def kb_films(self, name_films, user_id, type, id):
        ikb = InlineKeyboardMarkup(row_width=1)
        players = await db.get_Allplayer()
        for i in players:
            if i[2]:  # если плеер включен
                try:
                    url = f'{i[0]}/{"film" if type == "Фильм" else "series"}/{id}'
                    ikb.row(InlineKeyboardButton(text=i[3], url=url))
                except:
                    pass
        
        if not await db.get_UserFavouritesWfilm(user_id, name_films):
            ikb.row(InlineKeyboardButton('В избранное🌟', callback_data='in_favourites_' + str(name_films)))
        else:
            ikb.row(InlineKeyboardButton('Удалить из избранного🌟', callback_data='delete_favourites_' + str(name_films)))
        ikb.row(self.ikb_close_oikb)
        return ikb
    
    async def get_Player_menu(self):
        ikb = InlineKeyboardMarkup(row_width=4)
        ikb.insert(InlineKeyboardButton(text='Название', callback_data='player_exemple'))
        ikb.insert(InlineKeyboardButton(text='Сайт', callback_data='player_exemple'))
        ikb.insert(InlineKeyboardButton(text='Вкл./Выкл.', callback_data='player_exemple'))
        ikb.insert(InlineKeyboardButton(text='Название на кнопке', callback_data='player_exemple'))
        
        players = await db.get_Allplayer()
        for i in players:
            swich = '✅' if i[2] == 1 else '❌'
            ikb.insert(InlineKeyboardButton(text=i[1], callback_data='chenneger_name_player_admin'))
            ikb.insert(InlineKeyboardButton(text=i[0][:10] + '...', callback_data='chenneger_web_player_admin'))
            ikb.insert(InlineKeyboardButton(text=swich, callback_data='chenneger_swich_player_admin' + i[1]))
            ikb.insert(InlineKeyboardButton(text=i[3], callback_data='chenneger_kbname_player_admin' + i[1]))
        
        ikb.row(InlineKeyboardButton(text='Назад⬅️', callback_data='back_main_menu_admin'))
        return ikb

keyboards = Keyboards()

# ==================== ПРОВЕРКА ПОДПИСКИ ====================
async def check_subscription(user_id):
    data_chennel = await db.get_AllChennel()
    if not data_chennel:
        return False
    
    for channel in data_chennel:
        try:
            status = await bot.get_chat_member(chat_id=channel[0], user_id=user_id)
            if status.status == 'left':
                return True
        except:
            if admin_id:
                await bot.send_message(
                    chat_id=admin_id[0],
                    text=f'Ошибка с каналом: {channel[1]}\nID: {channel[0]}'
                )
    return False

# ==================== АНТИ-ФЛУД ====================
async def anti_flood(*args, **kwargs):
    m = args[0]
    await m.answer(f'Фильм можно найти раз в {rate_searsh} секунд😪', 
                   reply_markup=await keyboards.kb_user(m.from_user.id))

# ==================== ОБЩИЕ ОБРАБОТЧИКИ ====================
@dp.callback_query_handler(text='cancellation_state', state='*')
async def cancellation_state(call: types.CallbackQuery, state: FSMContext):
    await state.finish()
    await call.answer('Отмена❌')
    await call.message.delete()

@dp.callback_query_handler(text='close_text')
async def close_text(call: types.CallbackQuery):
    await call.message.delete()

# ==================== ОБРАБОТЧИКИ КНОПОК МЕНЮ ====================

@dp.message_handler(lambda message: message.text == 'Поиск🔍')
async def search_button_handler(message: types.Message):
    """Обработчик кнопки Поиск"""
    # Проверка подписки
    if await check_subscription(user_id=message.from_user.id):
        await bot.send_message(
            chat_id=message.from_user.id,
            text='Вы не подписаны на канал(ы)❌\nПосле подписки повторите попытку👌',
            reply_markup=await keyboards.sub_list()
        )
        return
    
    await message.answer('<b>Отправь мне название кино или его код🎫</b>', 
                        reply_markup=keyboards.kb_back(), 
                        parse_mode=types.ParseMode.HTML)
    await UserState.search_film.set()

@dp.message_handler(lambda message: message.text == 'Избранное🌟')
async def favorite_button_handler(message: types.Message):
    """Обработчик кнопки Избранное"""
    # Проверка подписки
    if await check_subscription(user_id=message.from_user.id):
        await bot.send_message(
            chat_id=message.from_user.id,
            text='Вы не подписаны на канал(ы)❌\nПосле подписки повторите попытку👌',
            reply_markup=await keyboards.sub_list()
        )
        return
    
    favourites_kb = await keyboards.get_Favourites_kb(user_id=message.from_user.id)
    await message.answer('Ваш список избранных кино🌟', reply_markup=favourites_kb)

@dp.message_handler(lambda message: message.text == 'Админ меню')
async def admin_button_handler(message: types.Message):
    """Обработчик кнопки Админ меню"""
    if message.from_user.id in admin_id:
        await cmd_admin(message)
    else:
        await message.answer('У вас нет прав администратора❌')

@dp.message_handler(lambda message: message.text == 'Отмена❌', state='*')
async def cancel_button_handler(message: types.Message, state: FSMContext):
    """Обработчик кнопки Отмена"""
    await state.finish()
    await message.answer('Отмена❌', reply_markup=await keyboards.kb_user(message.from_user.id))

# ==================== ОБРАБОТЧИКИ ПОЛЬЗОВАТЕЛЯ ====================

@dp.message_handler(CommandStart())
async def cmd_start(message: types.Message):
    if message.chat.type == types.ChatType.PRIVATE:
        try:
            await db.add_user(user_id=message.from_user.id, 
                            user_menotion=message.from_user.mention)
            if admin_id:
                await bot.send_message(
                    chat_id=admin_id[0],
                    text=f'<b>Новый пользователь <a href="tg://user?id={message.from_user.id}">{message.from_user.full_name}</a></b>',
                    parse_mode=types.ParseMode.HTML
                )
        except:
            pass
        
        text_start = await db.get_text(type='text_text', text_type='wellcome')
        if text_start:
            text_start = text_start[0][0]
            me = await bot.get_me()
            text_start = text_start.replace('{username_bot}', me.mention)
            text_start = text_start.replace('{bot_id}', str(me.id))
            text_start = text_start.replace('{username}', message.from_user.mention)
            text_start = text_start.replace('{full_name}', message.from_user.full_name)
            text_start = text_start.replace('{user_id}', str(message.from_user.id))
            
            await message.answer(
                text=text_start,
                parse_mode=types.ParseMode.HTML,
                reply_markup=await keyboards.kb_user(message.from_user.id)
            )

@dp.message_handler(state=UserState.search_film)
@dp.throttled(anti_flood, rate=rate_searsh)
async def search_kino_parser(message: types.Message, state: FSMContext):
    await state.finish()
    
    if message.text == 'Отмена❌':
        await message.answer('Отмена❌', reply_markup=await keyboards.kb_user(message.from_user.id))
        return
    
    # Проверка подписки
    if await check_subscription(user_id=message.from_user.id):
        await bot.send_message(
            chat_id=message.from_user.id,
            text='Вы не подписаны на канал(ы)❌\nПосле подписки повторите попытку👌',
            reply_markup=await keyboards.sub_list()
        )
        return
    
    # Проверка кода
    data_code = await db.only_list(await db.get_AllFilms(type='films_code'))
    if message.text in data_code:
        await message.answer('Фильм найден!', reply_markup=await keyboards.kb_user(message.from_user.id))
        film_data = await db.get_films(code=message.text)
        
        if film_data:
            text_film = await db.get_text(type='text_text', text_type='film')
            if text_film:
                text_film = text_film[0][0]
                me = await bot.get_me()
                text_film = text_film.replace('{username_bot}', me.mention)
                text_film = text_film.replace('{bot_id}', str(me.id))
                text_film = text_film.replace('{username}', message.from_user.mention)
                text_film = text_film.replace('{full_name}', message.from_user.full_name)
                text_film = text_film.replace('{user_id}', str(message.from_user.id))
                text_film = text_film.replace('{film_name}', film_data[0][1])
                
                try:
                    data_film = await film_parser.search(name_film=film_data[0][1])
                    ikb_films = await keyboards.kb_films(
                        name_films=film_data[0][3],
                        user_id=message.from_user.id,
                        type=data_film.type_kino_,
                        id=data_film.id_
                    )
                    await bot.send_photo(
                        chat_id=message.from_user.id,
                        photo=film_data[0][2],
                        caption=text_film,
                        reply_markup=ikb_films,
                        parse_mode=types.ParseMode.HTML
                    )
                except Exception as e:
                    print(f"Ошибка при поиске фильма: {e}")
                    await message.answer('Ошибка при получении данных о фильме😥')
    else:
        try:
            data_film = await film_parser.search(name_film=message.text)
            film_id = await db.add_filmname(message.text)
            
            await bot.send_photo(
                chat_id=message.from_user.id,
                photo=data_film.photo_,
                caption=f'<b>🎥 {data_film.type_kino_}:</b> <code>{data_film.name_film_}</code>\n\n'
                       f'🗓 Год производства: {data_film.year_}\n\n'
                       f'<b>👁 Жанры: {data_film.genre_}\n\n'
                       f'👥{data_film.text_autor_}: {data_film.director_}\n\n'
                       f'🔗 Длительность: {data_film.length_} мин</b>',
                reply_markup=await keyboards.kb_films(
                    name_films=film_id,
                    user_id=message.from_user.id,
                    type=data_film.type_kino_,
                    id=data_film.id_
                ),
                parse_mode=types.ParseMode.HTML
            )
            await db.add_historyInSearch(name=data_film.name_film_)
            await message.answer(f'Следующий запрос можно будет сделать через {rate_searsh}😴',
                               reply_markup=await keyboards.kb_user(message.from_user.id))
        except Exception as e:
            print(f"Ошибка при поиске фильма: {e}")
            await message.answer('Нам не удалось найти фильм😥')
            await message.answer(f'Следующий запрос можно будет сделать через {rate_searsh}😴',
                               reply_markup=await keyboards.kb_user(message.from_user.id))

@dp.callback_query_handler(lambda c: c.data and c.data.startswith('search_film_'))
async def search_film_with_call(call: types.CallbackQuery):
    film_id = call.data[12:]
    name = await db.get_filmname(film_id)
    
    if name:
        try:
            data_film = await film_parser.search(name_film=name[1])
            ikb = await keyboards.kb_films(
                name_films=int(film_id),
                user_id=call.from_user.id,
                type=data_film.type_kino_,
                id=data_film.id_
            )
            ikb.row(InlineKeyboardButton('Назад🔙', callback_data='back_to_favorites'))
            
            await bot.send_photo(
                chat_id=call.from_user.id,
                photo=data_film.photo_,
                caption=f'<b>🎥 {data_film.type_kino_}: </b><code>{data_film.name_film_}</code>\n\n'
                       f'🗓 Год производства: {data_film.year_}\n\n'
                       f'<b>👁 Жанры: {data_film.genre_}\n\n'
                       f'👥{data_film.text_autor_}: {data_film.director_}\n\n'
                       f'🔗 Длительность: {data_film.length_} мин</b>',
                reply_markup=ikb,
                parse_mode=types.ParseMode.HTML
            )
            await call.answer()
        except Exception as e:
            print(f"Ошибка: {e}")
            await call.answer('Ошибка при загрузке фильма', show_alert=True)

@dp.callback_query_handler(text='back_to_favorites')
async def back_to_favorites(call: types.CallbackQuery):
    await call.message.delete()
    await call.message.answer('Ваш список избранных кино', 
                            reply_markup=await keyboards.get_Favourites_kb(user_id=call.from_user.id))

@dp.callback_query_handler(lambda c: c.data and c.data.startswith('in_favourites_'))
async def add_Favourites(call: types.CallbackQuery):
    film_id = call.data[14:]
    
    await db.add_favourite(user_id=call.from_user.id, name=film_id)
    
    # Обновляем кнопку
    if call.message.reply_markup:
        new_kb = InlineKeyboardMarkup()
        for row in call.message.reply_markup.inline_keyboard:
            new_row = []
            for btn in row:
                if btn.text == 'В избранное🌟':
                    new_btn = InlineKeyboardButton(
                        text='Удалить из избранного🌟',
                        callback_data='delete_favourites_' + film_id
                    )
                    new_row.append(new_btn)
                else:
                    new_row.append(btn)
            new_kb.row(*new_row)
        
        await bot.edit_message_reply_markup(
            chat_id=call.from_user.id,
            message_id=call.message.message_id,
            reply_markup=new_kb
        )
    await call.answer('Добавлено в избранное🌟')

@dp.callback_query_handler(lambda c: c.data and c.data.startswith('delete_favourites_'))
async def delete_Favourites(call: types.CallbackQuery):
    film_id = call.data[18:]
    
    await db.delete_favourite(user_id=call.from_user.id, name=film_id)
    
    # Обновляем кнопку
    if call.message.reply_markup:
        new_kb = InlineKeyboardMarkup()
        for row in call.message.reply_markup.inline_keyboard:
            new_row = []
            for btn in row:
                if btn.text == 'Удалить из избранного🌟':
                    new_btn = InlineKeyboardButton(
                        text='В избранное🌟',
                        callback_data='in_favourites_' + film_id
                    )
                    new_row.append(new_btn)
                else:
                    new_row.append(btn)
            new_kb.row(*new_row)
        
        await bot.edit_message_reply_markup(
            chat_id=call.from_user.id,
            message_id=call.message.message_id,
            reply_markup=new_kb
        )
    await call.answer('Удалено из избранного🌟')

@dp.callback_query_handler(text='link_no_work')
async def link_complaint(call: types.CallbackQuery):
    last_complaint = await db.get_error_link_complaint_unix(user_id=call.from_user.id)
    
    if last_complaint is None or last_complaint <= time():
        await call.message.answer('Мы отправили администратору ошибку☑️', 
                                 reply_markup=keyboards.ikb_close)
        
        if admin_id:
            await bot.send_message(
                chat_id=admin_id[0],
                text=f'Пользователь <a href="tg://user?id={call.from_user.id}">{call.from_user.full_name}</a> '
                     f'пожаловался что одна из ссылок не работает❗️',
                parse_mode=types.ParseMode.HTML,
                reply_markup=keyboards.ikb_close
            )
        
        time_ub = (datetime.now() + timedelta(hours=3)).timestamp()
        await db.update_error_link_complaint_unix(user_id=call.from_user.id, time_ub=time_ub)
    else:
        await call.answer('Вы уже жаловались❌')

# ==================== ОБРАБОТЧИКИ АДМИНИСТРАТОРА ====================

@dp.message_handler(lambda message: message.from_user.id in admin_id and message.text in ['/admin', 'Админ меню'])
async def cmd_admin(message: types.Message):
    msg = await message.answer('<b>Загрузка...</b>', 
                              reply_markup=keyboards.admin_menu_main, 
                              parse_mode=types.ParseMode.HTML)
    
    # Статистика
    now = datetime.now()
    day_start = datetime(now.year, now.month, now.day, 0, 0, 0)
    day_end = datetime(now.year, now.month, now.day, 23, 59, 59)
    
    user_today = 0
    all_users = await db.get_AllUser()
    for user in all_users:
        if len(user) > 3 and user[3]:
            if day_start.timestamp() <= user[3] <= day_end.timestamp():
                user_today += 1
    
    all_search = await db.get_AllSearch()
    search_dict = {}
    for s in all_search:
        if len(s) > 1:
            search_dict[s[0]] = s[1]
    
    max_film = 'Нет'
    max_count = ''
    if search_dict:
        max_film = max(search_dict, key=search_dict.get)
        max_count = search_dict[max_film]
    
    text_menu = (f'<b>📊Статистика📊\n\n'
                 f'👥Всего пользователей: {len(all_users)}\n'
                 f'🍜Сегодняшние пользователи: {user_today}\n\n'
                 f'➖➖➖➖➖➖➖➖➖\n\n'
                 f'🎬Всего фильмов по коду: {len(await db.get_AllFilms())}\n'
                 f'🎞Макс по запросам: {max_film} ({max_count})</b>')
    
    await bot.edit_message_text(
        chat_id=message.from_user.id,
        message_id=msg.message_id,
        text=text_menu,
        reply_markup=keyboards.admin_menu_main,
        parse_mode=types.ParseMode.HTML
    )

# Рассылка
@dp.callback_query_handler(lambda c: c.from_user.id in admin_id and c.data == 'myling_list_start_admin')
async def mailing_start(call: types.CallbackQuery, state: FSMContext):
    ikb = InlineKeyboardMarkup(row_width=1)
    ikb.row(InlineKeyboardButton(text='Добавить кнопку▶️', callback_data='add_ikb_milling_admin'))
    ikb.row(keyboards.ikb_back_oikb)
    
    msg = await bot.send_message(
        chat_id=call.from_user.id,
        text='Хорошо, отправь текст для рассылки✒️\nМожно использовать стандартную разметку✂️',
        reply_markup=ikb
    )
    
    await state.update_data(ikb_list=[], message_id=msg.message_id)
    await AdminState.myling_list_text.set()

@dp.callback_query_handler(lambda c: c.from_user.id in admin_id and c.data == 'add_ikb_milling_admin', 
                          state=AdminState.myling_list_text)
async def add_ikb_mailing(call: types.CallbackQuery, state: FSMContext):
    ikb = InlineKeyboardMarkup(row_width=1)
    ikb.row(InlineKeyboardButton(text='Назад🔙', callback_data='back_to_text_milling'))
    
    data = await state.get_data()
    await bot.edit_message_text(
        chat_id=call.from_user.id,
        message_id=data['message_id'],
        text='Отправь мне надпись на кнопке🔖',
        reply_markup=ikb
    )
    
    await AdminState.myling_list_add_ikb_text.set()

@dp.message_handler(lambda message: message.from_user.id in admin_id, state=AdminState.myling_list_add_ikb_text)
async def add_ikb_mailing_text(message: types.Message, state: FSMContext):
    await message.delete()
    
    ikb = InlineKeyboardMarkup(row_width=1)
    ikb.row(InlineKeyboardButton(text='Назад🔙', callback_data='back_to_text_milling'))
    
    data = await state.get_data()
    await state.update_data(text_ikb=message.text)
    
    await bot.edit_message_text(
        chat_id=message.from_user.id,
        message_id=data['message_id'],
        text='Отправь мне ссылку на кнопке📝',
        reply_markup=ikb
    )
    
    await AdminState.myling_list_add_ikb_url.set()

@dp.message_handler(lambda message: message.from_user.id in admin_id, state=AdminState.myling_list_add_ikb_url)
async def add_ikb_mailing_url(message: types.Message, state: FSMContext):
    await message.delete()
    
    ikb = InlineKeyboardMarkup(row_width=1)
    ikb.row(InlineKeyboardButton(text='Добавить кнопку▶️', callback_data='add_ikb_milling_admin'))
    ikb.row(keyboards.ikb_back_oikb)
    
    data = await state.get_data()
    
    try:
        # Проверка кнопки
        test_ikb = InlineKeyboardMarkup().row(
            InlineKeyboardButton(text=data['text_ikb'], url=message.text)
        )
        test_msg = await message.answer('Тест кнопки...', reply_markup=test_ikb)
        await test_msg.delete()
        
        ikb_list = data.get('ikb_list', [])
        ikb_list.append({'text': data['text_ikb'], 'url': message.text})
        await state.update_data(ikb_list=ikb_list)
        
        await bot.edit_message_text(
            chat_id=message.from_user.id,
            message_id=data['message_id'],
            text='Кнопка добавлена➕\n\nХорошо, отправь текст для рассылки✒️\nМожно использовать стандартную разметку✂️',
            reply_markup=ikb
        )
    except:
        await bot.edit_message_text(
            chat_id=message.from_user.id,
            message_id=data['message_id'],
            text='Кнопка не добавлена (неправильная ссылка)❌\n\nХорошо, отправь текст для рассылки✒️\nМожно использовать стандартную разметку✂️',
            reply_markup=ikb
        )
    
    await AdminState.myling_list_text.set()

@dp.callback_query_handler(lambda c: c.from_user.id in admin_id and c.data == 'back_to_text_milling', 
                          state=[AdminState.myling_list_add_ikb_text, AdminState.myling_list_add_ikb_url])
async def back_to_mailing(call: types.CallbackQuery, state: FSMContext):
    ikb = InlineKeyboardMarkup(row_width=1)
    ikb.row(InlineKeyboardButton(text='Добавить кнопку▶️', callback_data='add_ikb_milling_admin'))
    ikb.row(keyboards.ikb_back_oikb)
    
    data = await state.get_data()
    await bot.edit_message_text(
        chat_id=call.from_user.id,
        message_id=data['message_id'],
        text='Хорошо, отправь текст для рассылки✒️\nМожно использовать стандартную разметку✂️',
        reply_markup=ikb
    )
    
    await AdminState.myling_list_text.set()

@dp.message_handler(lambda message: message.from_user.id in admin_id, state=AdminState.myling_list_text, 
                   content_types=types.ContentTypes.ANY)
async def mailing_send(message: types.Message, state: FSMContext):
    data = await state.get_data()
    users = await db.only_list(await db.get_AllUser(type='user_id'))
    count_accept = 0
    count_error = 0
    
    ikb = InlineKeyboardMarkup(row_width=1)
    for btn in data.get('ikb_list', []):
        ikb.row(InlineKeyboardButton(text=btn['text'], url=btn['url']))
    
    await bot.edit_message_text(
        chat_id=message.from_user.id,
        message_id=data['message_id'],
        text=f'<b>Данные о рассылке\n✅Успешно: {count_accept}\n❌Ошибки: {count_error}</b>',
        parse_mode=types.ParseMode.HTML
    )
    
    for user_id in users:
        try:
            await bot.copy_message(
                chat_id=user_id,
                from_chat_id=message.from_user.id,
                message_id=message.message_id,
                reply_markup=ikb if data.get('ikb_list') else None
            )
            count_accept += 1
        except:
            count_error += 1
        
        await bot.edit_message_text(
            chat_id=message.from_user.id,
            message_id=data['message_id'],
            text=f'<b>Данные о рассылке\n✅Успешно: {count_accept}\n❌Ошибки: {count_error}</b>',
            parse_mode=types.ParseMode.HTML
        )
    
    await bot.edit_message_text(
        chat_id=message.from_user.id,
        message_id=data['message_id'],
        text=f'<b>Данные о рассылке\n✅Успешно: {count_accept}\n❌Ошибки: {count_error}\nРассылка завершена🔔</b>',
        parse_mode=types.ParseMode.HTML,
        reply_markup=keyboards.ikb_close
    )
    
    await state.finish()

# Списки
@dp.callback_query_handler(lambda c: c.from_user.id in admin_id and c.data == 'list_data_admin')
async def list_data_menu(call: types.CallbackQuery):
    await call.message.edit_reply_markup(keyboards.admin_menu_list)

@dp.callback_query_handler(lambda c: c.from_user.id in admin_id and c.data == 'list_users_admin')
async def list_users(call: types.CallbackQuery):
    filename = 'users_data.txt'
    with open(filename, 'w', encoding='UTF-8') as f:
        for user in await db.get_AllUser():
            f.write(f'ID: {user[0]}, Username: {user[1]}\n')
    
    with open(filename, 'rb') as f:
        await bot.send_document(chat_id=call.from_user.id, document=f, reply_markup=keyboards.ikb_close)

@dp.callback_query_handler(lambda c: c.from_user.id in admin_id and c.data == 'list_films_admin')
async def list_films(call: types.CallbackQuery):
    filename = 'films_data.txt'
    with open(filename, 'w', encoding='UTF-8') as f:
        for film in await db.get_AllFilms():
            f.write(f'Код: {film[0]}, Название: {film[1]}\n')
    
    with open(filename, 'rb') as f:
        await bot.send_document(chat_id=call.from_user.id, document=f, reply_markup=keyboards.ikb_close)

@dp.callback_query_handler(lambda c: c.from_user.id in admin_id and c.data == 'list_chennel_admin')
async def list_channels(call: types.CallbackQuery):
    filename = 'channels_data.txt'
    with open(filename, 'w', encoding='UTF-8') as f:
        for channel in await db.get_AllChennel():
            f.write(f'ID: {channel[0]}, Название: {channel[1]}, Ссылка: {channel[2]}\n')
    
    with open(filename, 'rb') as f:
        await bot.send_document(chat_id=call.from_user.id, document=f, reply_markup=keyboards.ikb_close)

# Добавление фильма
@dp.callback_query_handler(lambda c: c.from_user.id in admin_id and c.data == 'add_film_admin')
async def add_film_start(call: types.CallbackQuery, state: FSMContext):
    msg = await bot.send_message(
        chat_id=call.from_user.id,
        text='Хорошо, отправь мне код🔑',
        reply_markup=InlineKeyboardMarkup(row_width=1).add(
            InlineKeyboardButton(text='Сгенерировать♻️', callback_data='generetion_fims_code_admin'),
            keyboards.ikb_back_oikb
        )
    )
    
    await state.update_data(message_id=msg.message_id)
    await AdminState.add_film_code.set()

@dp.callback_query_handler(lambda c: c.from_user.id in admin_id and c.data == 'generetion_fims_code_admin', 
                          state=AdminState.add_film_code)
async def generate_film_code(call: types.CallbackQuery, state: FSMContext):
    existing_codes = await db.only_list(await db.get_AllFilms(type='films_code'))
    
    while True:
        code = str(randint(0, 9999))
        if code not in existing_codes:
            break
    
    await state.update_data(code=code)
    
    await call.message.edit_text('Хорошо, теперь отправь мне название🎫')
    await call.message.edit_reply_markup(keyboards.ikb_back)
    await AdminState.add_film_name.set()

@dp.message_handler(lambda message: message.from_user.id in admin_id, state=AdminState.add_film_code)
async def add_film_code(message: types.Message, state: FSMContext):
    data = await state.get_data()
    await state.update_data(code=message.text)
    
    await bot.edit_message_text(
        chat_id=message.from_user.id,
        message_id=data['message_id'],
        text='Хорошо, теперь отправь мне название🎫',
        reply_markup=keyboards.ikb_back
    )
    
    await AdminState.add_film_name.set()

@dp.message_handler(lambda message: message.from_user.id in admin_id, state=AdminState.add_film_name)
async def add_film_name(message: types.Message, state: FSMContext):
    await message.delete()
    
    data = await state.get_data()
    await state.update_data(name=message.text)
    
    try:
        film_data = await film_parser.search(name_film=message.text)
        film_id = await db.add_filmname(data['name'])
        await db.add_film(code=data['code'], name=data['name'], priv=film_data.photo_, id=film_id)
        
        await message.answer_photo(
            photo=film_data.photo_,
            caption=f'📌Фильм добавлен\n🔑Код: <code>{data["code"]}</code>\n🎫Название: {data["name"]}',
            reply_markup=keyboards.ikb_close,
            parse_mode=types.ParseMode.HTML
        )
        await state.finish()
    except:
        await bot.edit_message_text(
            chat_id=message.from_user.id,
            message_id=data['message_id'],
            text='Не нашел на Кинопоиске. Отправь мне фотографию для обложки📌',
            reply_markup=keyboards.ikb_back
        )
        await AdminState.add_film_priew.set()

@dp.message_handler(lambda message: message.from_user.id in admin_id, state=AdminState.add_film_priew, 
                   content_types=['photo', 'text'])
async def add_film_photo(message: types.Message, state: FSMContext):
    await message.delete()
    
    data = await state.get_data()
    await bot.delete_message(chat_id=message.chat.id, message_id=data['message_id'])
    
    try:
        film_id = await db.add_filmname(data['name'])
        photo = message.text if message.text else message.photo[-1].file_id
        await db.add_film(code=data['code'], name=data['name'], priv=photo, id=film_id)
        
        await message.answer_photo(
            photo=photo,
            caption=f'📌Фильм добавлен\n🔑Код: <code>{data["code"]}</code>\n🎫Название: {data["name"]}',
            reply_markup=keyboards.ikb_close,
            parse_mode=types.ParseMode.HTML
        )
    except Exception as e:
        print(e)
        await message.answer('Скорее всего этот код уже добавлен\nОтмена❌',
                           reply_markup=keyboards.ikb_close)
    
    await state.finish()

# Удаление фильма
@dp.callback_query_handler(lambda c: c.from_user.id in admin_id and c.data == 'delete_film_admin')
async def delete_film_start(call: types.CallbackQuery, state: FSMContext):
    msg = await bot.send_message(
        chat_id=call.from_user.id,
        text='Хорошо, отправь мне код фильма, который хочешь удалить🗑',
        reply_markup=keyboards.ikb_back
    )
    
    await state.update_data(message_id=msg.message_id)
    await AdminState.delete_film_code.set()

@dp.message_handler(lambda message: message.from_user.id in admin_id, state=AdminState.delete_film_code)
async def delete_film_code(message: types.Message, state: FSMContext):
    await message.delete()
    
    data = await state.get_data()
    
    if await db.delete_Film(code=message.text):
        await bot.edit_message_text(
            chat_id=message.from_user.id,
            message_id=data['message_id'],
            text='Успешно удалено❎',
            reply_markup=keyboards.ikb_close
        )
        await state.finish()
    else:
        await bot.edit_message_text(
            chat_id=message.from_user.id,
            message_id=data['message_id'],
            text='Нет такого кода❌',
            reply_markup=keyboards.ikb_back
        )

# Добавление канала
@dp.callback_query_handler(lambda c: c.from_user.id in admin_id and c.data == 'add_chennel_admin')
async def add_channel_start(call: types.CallbackQuery, state: FSMContext):
    msg = await bot.send_message(
        chat_id=call.from_user.id,
        text='Хорошо, дайте в канале права "просматривать участников" и "пригласительные ссылки", '
             'после отправь мне @username или ID канала, который хотите добавить➕',
        reply_markup=keyboards.ikb_back
    )
    
    await state.update_data(message_id=msg.message_id)
    await AdminState.add_chennel_username.set()

@dp.message_handler(lambda message: message.from_user.id in admin_id, state=AdminState.add_chennel_username)
async def add_channel_username(message: types.Message, state: FSMContext):
    await message.delete()
    
    data = await state.get_data()
    
    try:
        channel_id = int(message.text) if message.text.lstrip('-').isdigit() else message.text
        
        try:
            chat = await bot.get_chat(chat_id=channel_id)
            me = await bot.get_me()
            link = await bot.create_chat_invite_link(chat_id=channel_id, name=f'Вход от {me.mention}')
            
            await db.add_Chennel(chennel_identifier=str(channel_id), name=chat.full_name, link=link.invite_link)
            
            await bot.edit_message_text(
                chat_id=message.from_user.id,
                message_id=data['message_id'],
                text='Канал успешно добавлен✅',
                reply_markup=keyboards.ikb_close
            )
            await state.finish()
        except Exception as e:
            print(e)
            await bot.edit_message_text(
                chat_id=message.from_user.id,
                message_id=data['message_id'],
                text='Ошибка при добавлении канала. Проверьте права бота.',
                reply_markup=keyboards.ikb_back
            )
    except:
        await bot.edit_message_text(
            chat_id=message.from_user.id,
            message_id=data['message_id'],
            text='Некорректный ID или username',
            reply_markup=keyboards.ikb_back
        )

# Удаление канала
@dp.callback_query_handler(lambda c: c.from_user.id in admin_id and c.data == 'delete_chennel_admin')
async def delete_channel_start(call: types.CallbackQuery, state: FSMContext):
    msg = await bot.send_message(
        chat_id=call.from_user.id,
        text='Хорошо, укажи ID или username канала, который удалить➖',
        reply_markup=keyboards.ikb_back
    )
    
    await state.update_data(message_id=msg.message_id)
    await AdminState.delete_chennel_username.set()

@dp.message_handler(lambda message: message.from_user.id in admin_id, state=AdminState.delete_chennel_username)
async def delete_channel_username(message: types.Message, state: FSMContext):
    await message.delete()
    
    data = await state.get_data()
    
    if await db.delete_Chennel(chennel_identifier=message.text):
        await bot.edit_message_text(
            chat_id=message.from_user.id,
            message_id=data['message_id'],
            text='Канал удален успешно✅',
            reply_markup=keyboards.ikb_close
        )
        await state.finish()
    else:
        await bot.edit_message_text(
            chat_id=message.from_user.id,
            message_id=data['message_id'],
            text='Извините, вы не добавляли такого канала❌',
            reply_markup=keyboards.ikb_back
        )

# Проверка каналов
@dp.callback_query_handler(lambda c: c.from_user.id in admin_id and c.data == 'check_chennel_admin')
async def check_channels(call: types.CallbackQuery):
    msg = await bot.send_message(
        chat_id=call.from_user.id,
        text='Хорошо, я проверяю, подождите♻️'
    )
    
    me = await bot.get_me()
    text = ''
    
    channels = await db.get_AllChennel()
    for channel in channels:
        try:
            admins = await bot.get_chat_administrators(chat_id=channel[0])
            chat = await bot.get_chat(chat_id=channel[0])
            await db.update_nameChennel(chennel_identifier=channel[0], name=chat.full_name)
            
            has_invite_permission = False
            for admin in admins:
                if admin.user.id == me.id:
                    has_invite_permission = admin.can_invite_users
                    break
            
            if has_invite_permission:
                text += f'✅ Канал: {channel[1]} - OK\n'
            else:
                text += f'⚠️ Канал: {channel[1]} - нет прав на ссылки\n'
        except:
            await db.delete_Chennel(chennel_identifier=channel[0])
            text += f'❌ Канал: {channel[1]} - удален (недоступен)\n'
        
        await bot.edit_message_text(
            chat_id=call.from_user.id,
            message_id=msg.message_id,
            text=f'Проверка...\n\n{text}'
        )
    
    await bot.edit_message_text(
        chat_id=call.from_user.id,
        message_id=msg.message_id,
        text=f'Проверка завершена❇️\n\n{text}',
        reply_markup=keyboards.ikb_close
    )

# Настройки плееров
@dp.callback_query_handler(lambda c: c.from_user.id in admin_id and c.data == 'player_settings_admin')
async def player_settings(call: types.CallbackQuery):
    await call.message.edit_reply_markup(await keyboards.get_Player_menu())

@dp.callback_query_handler(lambda c: c.from_user.id in admin_id and c.data and c.data.startswith('chenneger_swich_player_admin'))
async def switch_player(call: types.CallbackQuery):
    player_name = call.data[28:]
    await db.swich_player(player_name=player_name)
    await call.message.edit_reply_markup(await keyboards.get_Player_menu())

@dp.callback_query_handler(lambda c: c.from_user.id in admin_id and c.data and c.data.startswith('chenneger_kbname_player_admin'))
async def change_kbname_player(call: types.CallbackQuery, state: FSMContext):
    msg1 = await bot.send_message(
        chat_id=call.from_user.id,
        text='Хорошо, отправь мне новое название кнопки📌',
        reply_markup=keyboards.ikb_back
    )
    
    await state.update_data(
        message_id1=msg1.message_id,
        message_id2=call.message.message_id,
        name_kb=call.data[29:]
    )
    
    await AdminState.chennger_kbname_player_text.set()

@dp.message_handler(lambda message: message.from_user.id in admin_id, state=AdminState.chennger_kbname_player_text)
async def save_kbname_player(message: types.Message, state: FSMContext):
    await message.delete()
    
    data = await state.get_data()
    await db.update_kbname_player(player_name=data['name_kb'], kb=message.text)
    
    await bot.edit_message_text(
        chat_id=message.from_user.id,
        message_id=data['message_id1'],
        text='Кнопка изменена успешно✅',
        reply_markup=keyboards.ikb_close
    )
    await bot.edit_message_reply_markup(
        chat_id=message.from_user.id,
        message_id=data['message_id2'],
        reply_markup=await keyboards.get_Player_menu()
    )
    
    await state.finish()

# Настройки текстов
@dp.callback_query_handler(lambda c: c.from_user.id in admin_id and c.data == 'text_settings_admin')
async def text_settings(call: types.CallbackQuery):
    await call.message.edit_reply_markup(keyboards.admin_menu_text)

@dp.callback_query_handler(lambda c: c.from_user.id in admin_id and c.data == 'chenneger_wellcome_text_settings_admin')
async def change_welcome_text(call: types.CallbackQuery, state: FSMContext):
    msg = await bot.send_message(
        chat_id=call.from_user.id,
        text='<code>{username_bot}</code> - username бота\n'
             '<code>{bot_id}</code> - id бота\n'
             '<code>{username}</code> - username пользователя\n'
             '<code>{full_name}</code> - полное имя пользователя\n'
             '<code>{user_id}</code> - id пользователя\n\n'
             'Можно использовать разметку HTML✂️\n\n'
             'Хорошо, отправь мне новое приветствие🖊',
        reply_markup=keyboards.ikb_back,
        parse_mode=types.ParseMode.HTML
    )
    
    await state.update_data(message_id=msg.message_id)
    await AdminState.chennger_wellcome_text.set()

@dp.message_handler(lambda message: message.from_user.id in admin_id, state=AdminState.chennger_wellcome_text)
async def save_welcome_text(message: types.Message, state: FSMContext):
    await message.delete()
    
    data = await state.get_data()
    
    try:
        await bot.edit_message_text(
            chat_id=message.from_user.id,
            message_id=data['message_id'],
            text=message.text,
            parse_mode=types.ParseMode.HTML
        )
        await db.update_wellcome_text(text_type='wellcome', text=message.text)
        
        await bot.edit_message_text(
            chat_id=message.from_user.id,
            message_id=data['message_id'],
            text='Успешно изменил текст приветствия✅',
            reply_markup=keyboards.ikb_close
        )
        await state.finish()
    except:
        await bot.edit_message_text(
            chat_id=message.from_user.id,
            message_id=data['message_id'],
            text='Неправильная разметка HTML✂️',
            reply_markup=keyboards.ikb_back
        )

@dp.callback_query_handler(lambda c: c.from_user.id in admin_id and c.data == 'chenneger_film_text_settings_admin')
async def change_film_text(call: types.CallbackQuery, state: FSMContext):
    msg = await bot.send_message(
        chat_id=call.from_user.id,
        text='<code>{username_bot}</code> - username бота\n'
             '<code>{bot_id}</code> - id бота\n'
             '<code>{username}</code> - username пользователя\n'
             '<code>{full_name}</code> - полное имя пользователя\n'
             '<code>{user_id}</code> - id пользователя\n'
             '<code>{film_name}</code> - название фильма\n\n'
             'Можно использовать разметку HTML✂️\n\n'
             'Хорошо, отправь мне новый текст для фильмов🖊',
        reply_markup=keyboards.ikb_back,
        parse_mode=types.ParseMode.HTML
    )
    
    await state.update_data(message_id=msg.message_id)
    await AdminState.chennger_film_text.set()

@dp.message_handler(lambda message: message.from_user.id in admin_id, state=AdminState.chennger_film_text)
async def save_film_text(message: types.Message, state: FSMContext):
    await message.delete()
    
    data = await state.get_data()
    
    try:
        await bot.edit_message_text(
            chat_id=message.from_user.id,
            message_id=data['message_id'],
            text=message.text,
            parse_mode=types.ParseMode.HTML
        )
        await db.update_wellcome_text(text_type='film', text=message.text)
        
        await bot.edit_message_text(
            chat_id=message.from_user.id,
            message_id=data['message_id'],
            text='Успешно изменил текст фильма✅',
            reply_markup=keyboards.ikb_close
        )
        await state.finish()
    except:
        await bot.edit_message_text(
            chat_id=message.from_user.id,
            message_id=data['message_id'],
            text='Неправильная разметка HTML✂️',
            reply_markup=keyboards.ikb_back
        )

# Возврат в главное меню
@dp.callback_query_handler(lambda c: c.from_user.id in admin_id and c.data == 'back_main_menu_admin')
async def back_to_main_menu(call: types.CallbackQuery):
    await call.message.edit_reply_markup(keyboards.admin_menu_main)

# ==================== ОБРАБОТЧИК НЕИЗВЕСТНЫХ КОМАНД ====================

@dp.message_handler()
async def empty_command(message: types.Message):
    """Обработчик неизвестных команд"""
    # Игнорируем кнопки меню
    if message.text in ['Поиск🔍', 'Избранное🌟', 'Админ меню', 'Отмена❌']:
        return
    
    await message.delete()
    await message.answer('Такой команды нет🖍', 
                        reply_markup=await keyboards.kb_user(message.from_user.id))

# ==================== ЗАПУСК БОТА ====================
if __name__ == '__main__':
    print("Бот запущен!")
    print("Настройте токен и admin_id в начале файла!")
    executor.start_polling(dp, skip_updates=True)