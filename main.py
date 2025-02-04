import os
import logging
import asyncio
import aiohttp
from datetime import datetime, timedelta, time
import mysql.connector
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, CommandHandler, CallbackQueryHandler, MessageHandler, filters, ContextTypes
import telegram
import logging.handlers
from functools import lru_cache
import redis
from json import dumps, loads
import psutil
import matplotlib.pyplot as plt
import io
import signal

def setup_logging():
    # Создаем директорию для логов если её нет
    log_dir = 'logs'
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)

    # Определяем уровень логирования из переменной окружения
    debug_mode = os.getenv('DEBUG', 'False').lower() == 'true'
    log_level = logging.DEBUG if debug_mode else logging.INFO

    # Настраиваем ротацию логов
    log_file = os.path.join(log_dir, 'bot.log')
    file_handler = logging.handlers.TimedRotatingFileHandler(
        log_file,
        when='midnight',
        interval=1,
        backupCount=7,
        encoding='utf-8'
    )
    
    # Настраиваем вывод в консоль
    console_handler = logging.StreamHandler()
    
    # Формат логов
    log_format = logging.Formatter(
        '%(asctime)s - %(levelname)s - [%(name)s] - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    file_handler.setFormatter(log_format)
    console_handler.setFormatter(log_format)
    
    # Настройка корневого логгера
    root_logger = logging.getLogger()
    root_logger.setLevel(log_level)
    
    # Файловый обработчик всегда пишет все логи
    file_handler.setLevel(logging.INFO)
    root_logger.addHandler(file_handler)
    
    # Консольный обработчик учитывает режим отладки
    console_handler.setLevel(log_level)
    root_logger.addHandler(console_handler)
    
    # Отключаем лишние логи от библиотек
    logging.getLogger('telegram').setLevel(logging.WARNING)
    logging.getLogger('httpx').setLevel(logging.WARNING)
    logging.getLogger('aiohttp').setLevel(logging.WARNING)

    return debug_mode

# Инициализируем логирование и получаем режим отладки
DEBUG_MODE = setup_logging()

# Проверяем наличие переменных окружения
required_env_vars = [
    'BOT_TOKEN', 
    'ASU_API_TOKEN', 
    'ASU_API_URL', 
    'DB_HOST', 
    'DB_USER', 
    'DB_PASSWORD', 
    'DB_NAME', 
    'DB_PORT',
    'ADMIN_IDS'
]

# Проверяем и логируем значения переменных только в режиме отладки
if DEBUG_MODE:
    logging.debug("Environment variables:")
    for var in required_env_vars:
        value = os.getenv(var)
        logging.debug(f"{var}: {value if var != 'DB_PASSWORD' else '***'}")

# Проверяем наличие всех переменных
missing_vars = [var for var in required_env_vars if not os.getenv(var)]
if missing_vars:
    raise ValueError(f"Missing required environment variables: {', '.join(missing_vars)}")

# Константы из переменных окружения
BOT_TOKEN = os.getenv('BOT_TOKEN')
ASU_API_TOKEN = os.getenv('ASU_API_TOKEN')
ASU_API_URL = os.getenv('ASU_API_URL')
DB_CONFIG = {
    'host': os.getenv('DB_HOST'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
    'database': os.getenv('DB_NAME'),
    'port': int(os.getenv('DB_PORT'))
}
ADMIN_IDS = [int(id.strip()) for id in os.getenv('ADMIN_IDS', '').split(',') if id.strip()]

# Проверяем конфигурацию
if DEBUG_MODE:
    logging.debug("Configuration loaded successfully")

class Database:
    def __init__(self):
        self.config = DB_CONFIG
        self.connection_pool = None
        self._setup_connection_pool()

    def _setup_connection_pool(self):
        self.connection_pool = mysql.connector.pooling.MySQLConnectionPool(
            pool_name="mypool",
            pool_size=5,
            **self.config
        )

    def get_connection(self):
        try:
            return self.connection_pool.get_connection()
        except Exception as e:
            logging.error(f"Error getting connection from pool: {e}")
            self._setup_connection_pool()  # Пересоздаем пул при ошибке
            return self.connection_pool.get_connection()

    def test_connection(self):
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT 1")
            cursor.fetchone()

    def get_faculties(self):
        with self.get_connection() as conn:
            cursor = conn.cursor(dictionary=True)
            cursor.execute("SELECT * FROM faculty")
            return cursor.fetchall()

    def get_groups_by_faculty(self, faculty_id):
        with self.get_connection() as conn:
            cursor = conn.cursor(dictionary=True)
            cursor.execute("SELECT * FROM groups WHERE facultyId = %s", (faculty_id,))
            return cursor.fetchall()

    def search_groups(self, query):
        conn = self.get_connection()
        try:
            cursor = conn.cursor(dictionary=True)
            sql = """
                SELECT g.*, f.facultyTitle 
                FROM `groups` g 
                JOIN faculty f ON g.facultyId = f.facultyId 
                WHERE g.groupCode LIKE %s
            """
            cursor.execute(sql, (f"%{query}%",))
            return cursor.fetchall()
        finally:
            cursor.close()
            conn.close()

    def search_lecturers(self, query):
        with self.get_connection() as conn:
            cursor = conn.cursor(dictionary=True)
            sql = """
                SELECT l.*, f.facultyTitle 
                FROM lecturers l
                JOIN faculty f ON l.facultyId = f.facultyId 
                WHERE l.lecturerName LIKE %s
            """
            try:
                logging.info(f"Executing lecturer search query for: {query}")
                cursor.execute(sql, (f"%{query}%",))
                results = cursor.fetchall()
                logging.info(f"Found {len(results)} lecturers")
                return results
            except mysql.connector.Error as err:
                logging.error(f"Database error in search_lecturers: {err}")
                raise

    def ensure_user_exists(self, tg_id, username=None):
        with self.get_connection() as conn:
            cursor = conn.cursor()
            try:
                # Проверяем существует ли пользователь
                cursor.execute("SELECT id FROM users WHERE tg_id = %s", (tg_id,))
                result = cursor.fetchone()
                if result:
                    # Обновляем username, если он изменился
                    if username:
                        cursor.execute(
                            "UPDATE users SET username = %s WHERE tg_id = %s",
                            (username, tg_id)
                        )
                        conn.commit()
                    return result[0]
                
                # Если пользователь не существует, создаем его
                cursor.execute(
                    "INSERT INTO users (tg_id, username) VALUES (%s, %s)",
                    (tg_id, username)
                )
                conn.commit()
                
                # Логируем нового пользователя
                logging.info(f"New user registered: ID={tg_id}, username={username}")
                return cursor.lastrowid

            except mysql.connector.Error as err:
                logging.error(f"Database error in ensure_user_exists: {err}")
                raise

    def save_user_group(self, tg_id, group_id, username=None):
        with self.get_connection() as conn:
            cursor = conn.cursor()
            try:
                # Убеждаемся, что пользователь существует
                user_id = self.ensure_user_exists(tg_id, username)
                
                # Проверяем, существует ли уже такая запись
                cursor.execute(
                    "SELECT id FROM user_saved_groups WHERE user_id = %s AND group_id = %s",
                    (user_id, group_id)
                )
                if not cursor.fetchone():
                    cursor.execute(
                        "INSERT INTO user_saved_groups (user_id, group_id) VALUES (%s, %s)",
                        (user_id, group_id)
                    )
                    conn.commit()
                    return True
                return False
            except mysql.connector.Error as err:
                logging.error(f"Database error in save_user_group: {err}")
                raise

    def get_group_faculty(self, group_id):
        with self.get_connection() as conn:
            cursor = conn.cursor(dictionary=True)
            try:
                cursor.execute(
                    "SELECT facultyId FROM `groups` WHERE groupId = %s", 
                    (group_id,)
                )
                result = cursor.fetchone()
                if result:
                    logging.info(f"Found faculty ID {result['facultyId']} for group {group_id}")
                    return result['facultyId']
                logging.warning(f"No faculty found for group {group_id}")
                return None
            except mysql.connector.Error as err:
                logging.error(f"Database error in get_group_faculty: {err}")
                raise

    def save_user_lecturer(self, tg_id, lecturer_id, username=None):
        with self.get_connection() as conn:
            cursor = conn.cursor()
            try:
                user_id = self.ensure_user_exists(tg_id, username)
                cursor.execute(
                    "SELECT id FROM user_saved_lecturers WHERE user_id = %s AND lectureId = %s",
                    (user_id, lecturer_id)
                )
                if not cursor.fetchone():
                    cursor.execute(
                        "INSERT INTO user_saved_lecturers (user_id, lectureId) VALUES (%s, %s)",
                        (user_id, lecturer_id)
                    )
                    conn.commit()
                    return True
                return False
            except mysql.connector.Error as err:
                logging.error(f"Database error in save_user_lecturer: {err}")
                raise

    def get_saved_groups(self, tg_id):
        with self.get_connection() as conn:
            cursor = conn.cursor(dictionary=True)
            try:
                user_id = self.ensure_user_exists(tg_id)
                cursor.execute("""
                    SELECT g.* FROM user_saved_groups usg
                    JOIN `groups` g ON usg.group_id = g.groupId
                    WHERE usg.user_id = %s
                    ORDER BY usg.created_at DESC
                """, (user_id,))
                return cursor.fetchall()
            except mysql.connector.Error as err:
                logging.error(f"Database error in get_saved_groups: {err}")
                raise

    def get_saved_lecturers(self, tg_id):
        with self.get_connection() as conn:
            cursor = conn.cursor(dictionary=True)
            try:
                user_id = self.ensure_user_exists(tg_id)
                cursor.execute("""
                    SELECT l.* 
                    FROM lecturers l
                    JOIN user_saved_lecturers usl ON l.lectureId = usl.lectureId
                    WHERE usl.user_id = %s
                    ORDER BY usl.created_at DESC
                    LIMIT 5
                """, (user_id,))
                return cursor.fetchall()
            except mysql.connector.Error as err:
                logging.error(f"Database error in get_saved_lecturers: {err}")
                return []

    def get_lecturer_by_id(self, lecturer_id):
        with self.get_connection() as conn:
            cursor = conn.cursor(dictionary=True)
            try:
                cursor.execute("""
                    SELECT * FROM lecturers 
                    WHERE lectureId = %s
                """, (lecturer_id,))
                return cursor.fetchone()
            except mysql.connector.Error as err:
                logging.error(f"Database error in get_lecturer_by_id: {err}")
                return None

    def get_group_by_id(self, group_id):
        with self.get_connection() as conn:
            cursor = conn.cursor(dictionary=True)
            try:
                cursor.execute("""
                    SELECT g.*, f.facultyTitle 
                    FROM `groups` g 
                    JOIN faculty f ON g.facultyId = f.facultyId 
                    WHERE g.groupId = %s
                """, (group_id,))
                return cursor.fetchone()
            except mysql.connector.Error as err:
                logging.error(f"Database error in get_group_by_id: {err}")
                return None

    def is_group_saved(self, tg_id, group_id):
        with self.get_connection() as conn:
            cursor = conn.cursor()
            try:
                user_id = self.ensure_user_exists(tg_id)
                cursor.execute("""
                    SELECT id FROM user_saved_groups 
                    WHERE user_id = %s AND group_id = %s
                """, (user_id, group_id))
                return cursor.fetchone() is not None
            except mysql.connector.Error as err:
                logging.error(f"Database error in is_group_saved: {err}")
                return False

    def is_lecturer_saved(self, tg_id, lecturer_id):
        with self.get_connection() as conn:
            cursor = conn.cursor()
            try:
                user_id = self.ensure_user_exists(tg_id)
                cursor.execute("""
                    SELECT id FROM user_saved_lecturers 
                    WHERE user_id = %s AND lectureId = %s
                """, (user_id, lecturer_id))
                return cursor.fetchone() is not None
            except mysql.connector.Error as err:
                logging.error(f"Database error in is_lecturer_saved: {err}")
                return False

    def delete_saved_group(self, tg_id, group_id):
        with self.get_connection() as conn:
            cursor = conn.cursor()
            try:
                user_id = self.ensure_user_exists(tg_id)
                cursor.execute("""
                    DELETE FROM user_saved_groups 
                    WHERE user_id = %s AND group_id = %s
                """, (user_id, group_id))
                conn.commit()
                return cursor.rowcount > 0
            except mysql.connector.Error as err:
                logging.error(f"Database error in delete_saved_group: {err}")
                return False

    def delete_saved_lecturer(self, tg_id, lecturer_id):
        with self.get_connection() as conn:
            cursor = conn.cursor()
            try:
                user_id = self.ensure_user_exists(tg_id)
                cursor.execute("""
                    DELETE FROM user_saved_lecturers 
                    WHERE user_id = %s AND lectureId = %s
                """, (user_id, lecturer_id))
                conn.commit()
                return cursor.rowcount > 0
            except mysql.connector.Error as err:
                logging.error(f"Database error in delete_saved_lecturer: {err}")
                return False

    def get_user_by_tg_id(self, tg_id):
        with self.get_connection() as conn:
            cursor = conn.cursor(dictionary=True)
            try:
                cursor.execute("SELECT * FROM users WHERE tg_id = %s", (tg_id,))
                return cursor.fetchone()
            except mysql.connector.Error as err:
                logging.error(f"Database error in get_user_by_tg_id: {err}")
                return None

    def get_user_by_username(self, username):
        with self.get_connection() as conn:
            cursor = conn.cursor(dictionary=True)
            try:
                cursor.execute("SELECT * FROM users WHERE username = %s", (username,))
                return cursor.fetchone()
            except mysql.connector.Error as err:
                logging.error(f"Database error in get_user_by_username: {err}")
                return None

    def get_all_users(self):
        with self.get_connection() as conn:
            cursor = conn.cursor(dictionary=True)
            try:
                cursor.execute("SELECT * FROM users")
                return cursor.fetchall()
            except mysql.connector.Error as err:
                logging.error(f"Database error in get_all_users: {err}")
                return []

    def get_techcard_by_group(self, group_id):
        """Получение технологической карты для группы"""
        with self.get_connection() as conn:
            cursor = conn.cursor(dictionary=True)
            try:
                cursor.execute("""
                    SELECT t.*, g.groupCode 
                    FROM group_techcards t
                    JOIN `groups` g ON t.group_id = g.groupId
                    WHERE t.group_id = %s
                    ORDER BY t.created_at DESC
                    LIMIT 1
                """, (group_id,))
                return cursor.fetchone()
            except mysql.connector.Error as err:
                logging.error(f"Database error in get_techcard_by_group: {err}")
                return None

    def get_bot_stats(self):
        """Получение статистики бота"""
        with self.get_connection() as conn:
            cursor = conn.cursor(dictionary=True)
            try:
                stats = {}
                
                # Общее количество пользователей
                cursor.execute("SELECT COUNT(*) as total FROM users")
                stats['total_users'] = cursor.fetchone()['total']
                
                # Новые пользователи за последние 24 часа
                cursor.execute("""
                    SELECT COUNT(*) as new_users 
                    FROM users 
                    WHERE created_at >= NOW() - INTERVAL 24 HOUR
                """)
                stats['new_users_24h'] = cursor.fetchone()['new_users']
                
                # Количество сохраненных групп
                cursor.execute("SELECT COUNT(*) as total FROM user_saved_groups")
                stats['saved_groups'] = cursor.fetchone()['total']
                
                # Количество сохраненных преподавателей
                cursor.execute("SELECT COUNT(*) as total FROM user_saved_lecturers")
                stats['saved_lecturers'] = cursor.fetchone()['total']
                
                # Количество техкарт
                cursor.execute("SELECT COUNT(*) as total FROM group_techcards")
                stats['techcards'] = cursor.fetchone()['total']
                
                return stats
            except mysql.connector.Error as err:
                logging.error(f"Database error in get_bot_stats: {err}")
                raise

    def add_techcard(self, group_id, url):
        """Добавление технологической карты"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute("""
                    INSERT INTO group_techcards (group_id, techcard_url) 
                    VALUES (%s, %s)
                    ON DUPLICATE KEY UPDATE techcard_url = VALUES(techcard_url)
                """, (group_id, url))
                conn.commit()
                return True
            except mysql.connector.Error as err:
                logging.error(f"Database error in add_techcard: {err}")
                return False

    def delete_techcard(self, group_id):
        """Удаление технологической карты"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute("DELETE FROM group_techcards WHERE group_id = %s", (group_id,))
                conn.commit()
                return cursor.rowcount > 0
            except mysql.connector.Error as err:
                logging.error(f"Database error in delete_techcard: {err}")
                return False

    def ban_user(self, tg_id):
        """Бан пользователя"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute("""
                    UPDATE users 
                    SET is_banned = 1, banned_at = NOW() 
                    WHERE tg_id = %s
                """, (tg_id,))
                conn.commit()
                return cursor.rowcount > 0
            except mysql.connector.Error as err:
                logging.error(f"Database error in ban_user: {err}")
                return False

    def unban_user(self, tg_id):
        """Разбан пользователя"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute("""
                    UPDATE users 
                    SET is_banned = 0, banned_at = NULL 
                    WHERE tg_id = %s
                """, (tg_id,))
                conn.commit()
                return cursor.rowcount > 0
            except mysql.connector.Error as err:
                logging.error(f"Database error in unban_user: {err}")
                return False

    def get_user_info(self, tg_id):
        """Получение полной информации о пользователе"""
        with self.get_connection() as conn:
            cursor = conn.cursor(dictionary=True)
            try:
                # Получаем основную информацию
                cursor.execute("""
                    SELECT u.*, 
                        COUNT(DISTINCT usg.id) as groups_count,
                        COUNT(DISTINCT usl.id) as lecturers_count
                    FROM users u
                    LEFT JOIN user_saved_groups usg ON u.id = usg.user_id
                    LEFT JOIN user_saved_lecturers usl ON u.id = usl.user_id
                    WHERE u.tg_id = %s
                    GROUP BY u.id
                """, (tg_id,))
                user_info = cursor.fetchone()
                
                if not user_info:
                    return None
                
                # Получаем сохраненные группы
                cursor.execute("""
                    SELECT g.groupCode, g.groupId
                    FROM user_saved_groups usg
                    JOIN `groups` g ON usg.group_id = g.groupId
                    WHERE usg.user_id = %s
                """, (user_info['id'],))
                user_info['saved_groups'] = cursor.fetchall()
                
                # Получаем сохраненных преподавателей
                cursor.execute("""
                    SELECT l.lecturerName, l.lectureId
                    FROM user_saved_lecturers usl
                    JOIN lecturers l ON usl.lectureId = l.lectureId
                    WHERE usl.user_id = %s
                """, (user_info['id'],))
                user_info['saved_lecturers'] = cursor.fetchall()
                
                return user_info
            except mysql.connector.Error as err:
                logging.error(f"Database error in get_user_info: {err}")
                return None

    def delete_user_group(self, user_id, group_code):
        """Удаление группы у пользователя"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            try:
                # Сначала находим ID группы по коду
                cursor.execute("SELECT groupId FROM `groups` WHERE groupCode = %s", (group_code,))
                group = cursor.fetchone()
                if not group:
                    return False, "Группа не найдена"
                
                # Удаляем связь пользователь-группа
                cursor.execute("""
                    DELETE usg FROM user_saved_groups usg
                    JOIN users u ON u.id = usg.user_id
                    WHERE u.tg_id = %s AND usg.group_id = %s
                """, (user_id, group[0]))
                conn.commit()
                return True, "Группа успешно удалена"
            except mysql.connector.Error as err:
                logging.error(f"Database error in delete_user_group: {err}")
                return False, "Ошибка базы данных"

    def delete_user_lecturer(self, user_id, lecturer_name):
        """Удаление преподавателя у пользователя"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            try:
                # Сначала находим ID преподавателя по имени
                cursor.execute("SELECT lectureId FROM lecturers WHERE lecturerName LIKE %s", (f"%{lecturer_name}%",))
                lecturer = cursor.fetchone()
                if not lecturer:
                    return False, "Преподаватель не найден"
                
                # Удаляем связь пользователь-преподаватель
                cursor.execute("""
                    DELETE usl FROM user_saved_lecturers usl
                    JOIN users u ON u.id = usl.user_id
                    WHERE u.tg_id = %s AND usl.lectureId = %s
                """, (user_id, lecturer[0]))
                conn.commit()
                return True, "Преподаватель успешно удален"
            except mysql.connector.Error as err:
                logging.error(f"Database error in delete_user_lecturer: {err}")
                return False, "Ошибка базы данных"

    def get_user_by_username(self, username):
        """Получение пользователя по username"""
        with self.get_connection() as conn:
            cursor = conn.cursor(dictionary=True)
            try:
                cursor.execute("SELECT * FROM users WHERE username = %s", (username,))
                return cursor.fetchone()
            except mysql.connector.Error as err:
                logging.error(f"Database error in get_user_by_username: {err}")
                return None

    def get_user_by_tg_id(self, tg_id):
        """Получение пользователя по Telegram ID"""
        with self.get_connection() as conn:
            cursor = conn.cursor(dictionary=True)
            try:
                cursor.execute("SELECT * FROM users WHERE tg_id = %s", (tg_id,))
                return cursor.fetchone()
            except mysql.connector.Error as err:
                logging.error(f"Database error in get_user_by_tg_id: {err}")
                return None

    def add_command_stat(self, command_name):
        """Добавление статистики использования команды"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute("""
                    INSERT INTO command_stats (command_name, usage_count, last_used) 
                    VALUES (%s, 1, NOW())
                    ON DUPLICATE KEY UPDATE 
                        usage_count = usage_count + 1,
                        last_used = NOW()
                """, (command_name,))
                conn.commit()
            except mysql.connector.Error as err:
                logging.error(f"Database error in add_command_stat: {err}")

    def get_command_stats(self):
        """Получение статистики использования команд"""
        with self.get_connection() as conn:
            cursor = conn.cursor(dictionary=True)
            try:
                cursor.execute("""
                    SELECT command_name, usage_count, last_used
                    FROM command_stats
                    ORDER BY usage_count DESC
                """)
                return cursor.fetchall()
            except mysql.connector.Error as err:
                logging.error(f"Database error in get_command_stats: {err}")
                return []

    def cleanup_old_data(self):
        """Очистка старых данных"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            try:
                # Удаляем старые записи логов старше 30 дней
                cursor.execute("""
                    DELETE FROM command_stats 
                    WHERE last_used < NOW() - INTERVAL 30 DAY
                """)
                
                # Удаляем неактивных пользователей
                cursor.execute("""
                    DELETE FROM users 
                    WHERE last_activity < NOW() - INTERVAL 180 DAY
                    AND is_banned = 0
                """)
                
                conn.commit()
            except mysql.connector.Error as err:
                logging.error(f"Database error in cleanup_old_data: {err}")

    def search_rooms(self, query):
        """Поиск по аудиториям"""
        with self.get_connection() as conn:
            cursor = conn.cursor(dictionary=True)
            try:
                cursor.execute("""
                    SELECT DISTINCT room 
                    FROM schedule 
                    WHERE room LIKE %s
                    ORDER BY room
                """, (f"%{query}%",))
                return cursor.fetchall()
            except mysql.connector.Error as err:
                logging.error(f"Database error in search_rooms: {err}")
                return []

    def set_notification(self, tg_id, group_id, notification_time):
        """Установка времени уведомлений"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            try:
                user_id = self.ensure_user_exists(tg_id)
                cursor.execute("""
                    INSERT INTO user_notifications (user_id, group_id, notification_time)
                    VALUES (%s, %s, %s)
                    ON DUPLICATE KEY UPDATE 
                        notification_time = VALUES(notification_time),
                        is_active = TRUE
                """, (user_id, group_id, notification_time))
                conn.commit()
                return True
            except mysql.connector.Error as err:
                logging.error(f"Database error in set_notification: {err}")
                return False

    def disable_notification(self, tg_id, group_id):
        """Отключение уведомлений"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            try:
                user_id = self.ensure_user_exists(tg_id)
                cursor.execute("""
                    UPDATE user_notifications 
                    SET is_active = FALSE 
                    WHERE user_id = %s AND group_id = %s
                """, (user_id, group_id))
                conn.commit()
                return True
            except mysql.connector.Error as err:
                logging.error(f"Database error in disable_notification: {err}")
                return False

    def get_user_notifications(self, tg_id):
        """Получение настроек уведомлений пользователя"""
        with self.get_connection() as conn:
            cursor = conn.cursor(dictionary=True)
            try:
                user_id = self.ensure_user_exists(tg_id)
                cursor.execute("""
                    SELECT un.*, g.groupCode
                    FROM user_notifications un
                    JOIN `groups` g ON un.group_id = g.groupId
                    WHERE un.user_id = %s AND un.is_active = TRUE
                """, (user_id,))
                return cursor.fetchall()
            except mysql.connector.Error as err:
                logging.error(f"Database error in get_user_notifications: {err}")
                return []

    def get_users_for_notification(self, notification_time):
        """Получение пользователей для отправки уведомлений"""
        with self.get_connection() as conn:
            cursor = conn.cursor(dictionary=True)
            try:
                # Оптимизированный запрос с индексами
                cursor.execute("""
                    SELECT 
                        u.tg_id,
                        g.groupId,
                        g.groupCode,
                        un.notification_time,
                        GROUP_CONCAT(DISTINCT usg.group_id) as saved_groups,
                        GROUP_CONCAT(DISTINCT usl.lectureId) as saved_lecturers
                    FROM user_notifications un
                    JOIN users u ON un.user_id = u.id
                    JOIN `groups` g ON un.group_id = g.groupId
                    LEFT JOIN user_saved_groups usg ON u.id = usg.user_id
                    LEFT JOIN user_saved_lecturers usl ON u.id = usl.user_id
                    WHERE un.is_active = TRUE 
                    AND TIME(un.notification_time) = %s
                    AND u.is_banned = FALSE
                    GROUP BY u.id, g.groupId
                    ORDER BY u.id
                """, (notification_time,))
                return cursor.fetchall()
            except mysql.connector.Error as err:
                logging.error(f"Database error in get_users_for_notification: {err}")
                return []

    def bulk_insert_stats(self, stats_data):
        """Пакетная вставка статистики"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.executemany("""
                    INSERT INTO bot_stats 
                    (stat_type, value, created_at) 
                    VALUES (%s, %s, NOW())
                """, stats_data)
                conn.commit()
            except mysql.connector.Error as err:
                logging.error(f"Database error in bulk_insert_stats: {err}")

    def get_active_users_last_24h(self):
        """Получение активных пользователей за последние 24 часа"""
        with self.get_connection() as conn:
            cursor = conn.cursor(dictionary=True)
            try:
                cursor.execute("""
                    SELECT DISTINCT u.* 
                    FROM users u
                    WHERE u.last_activity >= NOW() - INTERVAL 24 HOUR
                """)
                return cursor.fetchall()
            except mysql.connector.Error as err:
                logging.error(f"Database error in get_active_users_last_24h: {err}")
                return []

    def get_usage_stats_last_week(self):
        """Получение статистики использования за последнюю неделю"""
        with self.get_connection() as conn:
            cursor = conn.cursor(dictionary=True)
            try:
                cursor.execute("""
                    SELECT 
                        DATE(created_at) as date,
                        COUNT(DISTINCT user_id) as users,
                        COUNT(*) as queries
                    FROM user_actions
                    WHERE created_at >= DATE_SUB(CURDATE(), INTERVAL 7 DAY)
                    GROUP BY DATE(created_at)
                    ORDER BY date
                """)
                results = cursor.fetchall()
                
                # Форматируем данные для графика
                dates = []
                users = []
                queries = []
                
                current = datetime.now().date() - timedelta(days=7)
                end_date = datetime.now().date()
                
                while current <= end_date:
                    dates.append(current)
                    stat = next((r for r in results if r['date'].date() == current), None)
                    users.append(stat['users'] if stat else 0)
                    queries.append(stat['queries'] if stat else 0)
                    current += timedelta(days=1)
                
                return {
                    'dates': dates,
                    'users': users,
                    'queries': queries
                }
            except mysql.connector.Error as err:
                logging.error(f"Database error in get_usage_stats_last_week: {err}")
                return {'dates': [], 'users': [], 'queries': []}

    def get_techcards_stats(self):
        """Получение статистики техкарт"""
        with self.get_connection() as conn:
            cursor = conn.cursor(dictionary=True)
            try:
                stats = {}
                
                # Общее количество
                cursor.execute("SELECT COUNT(*) as total FROM group_techcards")
                stats['total'] = cursor.fetchone()['total']
                
                # Добавлено за неделю
                cursor.execute("""
                    SELECT COUNT(*) as count 
                    FROM group_techcards 
                    WHERE created_at >= DATE_SUB(CURDATE(), INTERVAL 7 DAY)
                """)
                stats['added_week'] = cursor.fetchone()['count']
                
                # Обновлено за неделю
                cursor.execute("""
                    SELECT COUNT(*) as count 
                    FROM group_techcards 
                    WHERE updated_at >= DATE_SUB(CURDATE(), INTERVAL 7 DAY)
                """)
                stats['updated_week'] = cursor.fetchone()['count']
                
                return stats
            except mysql.connector.Error as err:
                logging.error(f"Database error in get_techcards_stats: {err}")
                return {'total': 0, 'added_week': 0, 'updated_week': 0}

    def get_all_techcards(self):
        """Получение всех техкарт"""
        with self.get_connection() as conn:
            cursor = conn.cursor(dictionary=True)
            try:
                cursor.execute("""
                    SELECT gt.*, g.groupCode
                    FROM group_techcards gt
                    JOIN `groups` g ON gt.group_id = g.groupId
                    ORDER BY gt.updated_at DESC
                """)
                return cursor.fetchall()
            except mysql.connector.Error as err:
                logging.error(f"Database error in get_all_techcards: {err}")
                return []

    def get_table_count(self, table_name):
        """Получение количества записей в таблице"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            try:
                # Экранируем имя таблицы
                safe_table_name = f"`{table_name.strip('`')}`"
                cursor.execute(f"SELECT COUNT(*) FROM {safe_table_name}")
                return cursor.fetchone()[0]
            except mysql.connector.Error as err:
                logging.error(f"Database error in get_table_count: {err}")
                return 0

    def get_detailed_techcard_stats(self):
        """Получение детальной статистики техкарт"""
        with self.get_connection() as conn:
            cursor = conn.cursor(dictionary=True)
            try:
                # Статистика по факультетам
                cursor.execute("""
                    SELECT 
                        f.facultyTitle as name,
                        COUNT(gt.id) as count
                    FROM faculty f
                    LEFT JOIN `groups` g ON f.facultyId = g.facultyId
                    LEFT JOIN group_techcards gt ON g.groupId = gt.group_id
                    GROUP BY f.facultyId
                    ORDER BY count DESC
                """)
                faculty_stats = cursor.fetchall()
                
                return {
                    'by_faculty': faculty_stats
                }
            except mysql.connector.Error as err:
                logging.error(f"Database error in get_detailed_techcard_stats: {err}")
                return {'by_faculty': []}

    def search_user(self, query):
        """Поиск пользователя по ID или username"""
        with self.get_connection() as conn:
            cursor = conn.cursor(dictionary=True)
            try:
                # Если это ID
                if query.isdigit():
                    cursor.execute("""
                        SELECT * FROM users 
                        WHERE tg_id = %s
                    """, (int(query),))
                # Если это username
                else:
                    username = query.replace('@', '')
                    cursor.execute("""
                        SELECT * FROM users 
                        WHERE username = %s
                    """, (username,))
                
                return cursor.fetchall()
            except mysql.connector.Error as err:
                logging.error(f"Database error in search_user: {err}")
                return []

    def get_banned_users(self):
        """Получение списка забаненных пользователей"""
        with self.get_connection() as conn:
            cursor = conn.cursor(dictionary=True)
            try:
                cursor.execute("""
                    SELECT tg_id, username, banned_at, created_at, last_activity
                    FROM users 
                    WHERE is_banned = 1 
                    ORDER BY banned_at DESC
                """)
                return cursor.fetchall()
            except mysql.connector.Error as err:
                logging.error(f"Database error in get_banned_users: {err}")
                return []

class ASUApi:
    def __init__(self, base_url, api_token):
        self.base_url = base_url
        self.api_token = api_token
        self.session = None

    async def init_session(self):
        if not self.session:
            self.session = aiohttp.ClientSession()

    async def close_session(self):
        if self.session:
            await self.session.close()
            self.session = None

    async def get_schedule(self, path, date=None):
        await self.init_session()
        params = {
            'file': 'list.json',
            'api_token': self.api_token
        }
        if date:
            params['date'] = date

        # Формируем URL в зависимости от типа запроса
        if path.startswith('lecturers/'):
            # Извлекаем ID преподавателя
            lecturer_id = path.split('/')[-1]
            url = f"{self.base_url.rstrip('/')}/timetable/lecturers/15/65/{lecturer_id}/"
        else:
            # Для групп используем фиксированный факультет и ID
            url = f"{self.base_url.rstrip('/')}/timetable/students/15/{path}"

        try:
            logging.info(f"Fetching schedule from: {url} with params: {params}")
            async with self.session.get(url, params=params) as response:
                text = await response.text()
                logging.info(f"Raw response: {text}")
                
                if response.status == 200:
                    try:
                        data = await response.json()
                        if 'schedule' in data and data['schedule'].get('records', []):
                            lessons_count = len(data['schedule']['records'])
                            logging.info(f"Successfully received schedule with {lessons_count} lessons")
                            # Добавляем URL в ответ
                            data['url'] = url
                            return data
                        else:
                            # Если нет записей, пробуем получить расписание без даты
                            if date and '-' in date:  # Если это запрос на неделю
                                logging.info("Trying to fetch schedule without date range")
                                params.pop('date')  # Удаляем параметр date
                                async with self.session.get(url, params=params) as retry_response:
                                    retry_data = await retry_response.json()
                                    if 'schedule' in retry_data and retry_data['schedule'].get('records', []):
                                        lessons_count = len(retry_data['schedule']['records'])
                                        logging.info(f"Successfully received schedule with {lessons_count} lessons")
                                        # Добавляем URL в ответ
                                        retry_data['url'] = url
                                        return retry_data
                            logging.warning(f"No schedule records found")
                            return None
                    except Exception as e:
                        logging.error(f"Error parsing JSON: {e}")
                        return None
                logging.error(f"Failed to fetch schedule. Status: {response.status}")
                return None
        except Exception as e:
            logging.error(f"Error fetching schedule: {e}")
            return None

class RedisCache:
    def __init__(self):
        self.redis = redis.Redis(
            host='localhost',
            port=6379,
            db=0,
            decode_responses=True,
            socket_timeout=5,
            socket_connect_timeout=5,
            retry_on_timeout=True
        )
        self.default_ttl = 300  # 5 минут

    def set_many(self, mapping, ttl=None):
        """Пакетная установка значений в кэш"""
        pipeline = self.redis.pipeline()
        try:
            for key, value in mapping.items():
                pipeline.set(key, dumps(value), ex=ttl or self.default_ttl)
            pipeline.execute()
        except Exception as e:
            logging.error(f"Redis error in set_many: {e}")

    def get_many(self, keys):
        """Пакетное получение значений из кэша"""
        try:
            pipeline = self.redis.pipeline()
            for key in keys:
                pipeline.get(key)
            values = pipeline.execute()
            return [loads(v) if v else None for v in values]
        except Exception as e:
            logging.error(f"Redis error in get_many: {e}")
            return [None] * len(keys)

class TelegramBot:
    def __init__(self):
        """Инициализация бота"""
        self.db = Database()
        self.api = ASUApi(ASU_API_URL, ASU_API_TOKEN)
        self.start_time = datetime.now()  # Добавляем время старта
        
        # Создаем приложение с поддержкой job queue
        self.application = (
            Application.builder()
            .token(BOT_TOKEN)
            .concurrent_updates(True)
            .build()
        )
        
        # Инициализируем очередь сообщений
        self.message_queue = asyncio.Queue()
        
        # Регистрируем обработчики
        self.application.add_handler(CommandHandler("start", self.start))
        self.application.add_handler(CommandHandler("week", self.get_current_week))
        self.application.add_handler(CommandHandler("help", self.help_command))
        self.application.add_handler(CommandHandler("send", self.send_command))
        self.application.add_handler(CommandHandler("broadcast", self.broadcast_command))
        self.application.add_handler(CommandHandler("ban", self.ban_command))
        self.application.add_handler(CommandHandler("unban", self.unban_command))
        self.application.add_handler(CallbackQueryHandler(self.button_handler))
        self.application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, self.message_handler))
        
        # Регистрируем обработчик ошибок
        self.application.add_error_handler(self.error_handler)
        
        # Кэширование
        self.cache_timeout = timedelta(minutes=5)
        self.cache_last_update = {}
        self.schedule_cache = {}
        
        # Запускаем обработчик очереди сообщений как корутину
        self.queue_task = None

        # Добавляем обработчик команды админа
        self.application.add_handler(CommandHandler("admin", self.admin_panel))

        # Настраиваем задания для уведомлений
        self.setup_notification_jobs()
        
        # Добавляем задачу очистки кэша каждые 6 часов
        self.application.job_queue.run_repeating(
            self.cleanup_task,
            interval=timedelta(hours=6),
            first=0
        )

        # Инициализируем Redis кэш
        try:
            self.redis_cache = RedisCache()
        except Exception as e:
            logging.error(f"Failed to initialize Redis cache: {e}")
            self.redis_cache = None
        
        # Логируем информацию о настроенных заданиях
        if hasattr(self.application.job_queue, 'jobs'):
            jobs = self.application.job_queue.jobs()
            logging.info(f"Scheduled jobs: {[job.name for job in jobs]}")

    async def _process_message_queue(self):
        """Обработка очереди сообщений"""
        while True:
            try:
                message, chat_id = await self.message_queue.get()
                try:
                    await self.application.bot.send_message(
                        chat_id=chat_id,
                        text=message,
                        parse_mode='HTML'
                    )
                    await asyncio.sleep(0.05)
                finally:
                    self.message_queue.task_done()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logging.error(f"Error processing message queue: {e}")

    async def run(self):
        """Асинхронный метод запуска бота"""
        try:
            logging.info("Starting bot...")
            print("Бот запущен...")
            
            # Запускаем обработчик очереди сообщений
            self.queue_task = asyncio.create_task(self._process_message_queue())
            
            # Запускаем бота
            await self.application.initialize()
            await self.application.start()
            await self.application.updater.start_polling()
            
            # Ждем завершения
            stop_signal = asyncio.Future()
            await stop_signal
            
        except Exception as e:
            logging.error(f"Error occurred: {e}")
            print(f"Произошла ошибка: {e}")
        finally:
            await self.shutdown()

    async def shutdown(self):
        """Корректное завершение работы бота"""
        try:
            logging.info("Starting shutdown process...")
            
            # Отменяем задачу очереди сообщений
            if self.queue_task and not self.queue_task.done():
                self.queue_task.cancel()
                try:
                    await self.queue_task
                except asyncio.CancelledError:
                    pass

            # Останавливаем планировщик
            if hasattr(self.application, 'job_queue'):
                await self.application.job_queue.stop()

            # Останавливаем updater если он запущен
            if self.application.updater.running:
                await self.application.updater.stop()

            # Останавливаем бота
            try:
                await self.application.stop()
                await self.application.shutdown()
            except Exception as e:
                logging.error(f"Error stopping application: {e}")
                
            # Закрываем API сессию
            if hasattr(self.api, 'session') and self.api.session:
                await self.api.close_session()
                
            logging.info("Shutdown completed successfully")
        except Exception as e:
            logging.error(f"Error during shutdown: {e}")

    async def error_handler(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Улучшенный обработчик ошибок"""
        try:
            # Формируем базовую информацию об ошибке
            error_info = [
                "❌ <b>Произошла ошибка:</b>",
                f"Ошибка: {context.error}",
            ]

            # Добавляем информацию о пользователе, если доступна
            if update.effective_user:
                error_info.extend([
                    f"Пользователь ID: {update.effective_user.id}",
                    f"Username: @{update.effective_user.username or 'нет'}"
                ])

            # Добавляем информацию о чате, если доступна
            if update.effective_chat:
                error_info.append(f"Чат ID: {update.effective_chat.id}")

            # Добавляем информацию о действии
            if update.callback_query:
                error_info.append(f"Действие: callback_query = {update.callback_query.data}")
            elif update.message:
                error_info.append(f"Действие: message = {update.message.text}")

            # Формируем сообщение для админов
            admin_error_text = "\n".join(error_info)

            # Отправляем уведомление админам
            for admin_id in ADMIN_IDS:
                try:
                    await context.bot.send_message(
                        chat_id=admin_id,
                        text=admin_error_text,
                        parse_mode='HTML'
                    )
                except Exception as e:
                    logging.error(f"Failed to notify admin {admin_id}: {e}")

            # Отправляем сообщение пользователю
            if update and update.effective_message:
                user_error_text = "Произошла ошибка при обработке запроса. Попробуйте позже."
                
                if isinstance(context.error, telegram.error.TimedOut):
                    user_error_text = "Превышено время ожидания ответа. Попробуйте позже."
                elif isinstance(context.error, telegram.error.NetworkError):
                    user_error_text = "Проблемы с сетью. Попробуйте позже."
                
                await update.effective_message.reply_text(
                    user_error_text,
                    reply_markup=InlineKeyboardMarkup([[
                        InlineKeyboardButton("🔙 В главное меню", callback_data='start')
                    ]])
                )
            
            # Логируем ошибку
            if DEBUG_MODE:
                logging.error(f"Update {update} caused error {context.error}", exc_info=context.error)
            else:
                logging.error(f"Update {update} caused error {context.error}")
            
        except Exception as e:
            logging.error(f"Error in error handler: {e}")

    async def start(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработчик команды /start"""
        user = update.effective_user
        try:
            # Регистрируем пользователя
            self.db.ensure_user_exists(user.id, user.username)
            
            # Получаем сохраненные группы и преподавателей
            saved_groups = self.db.get_saved_groups(user.id)
            saved_lecturers = self.db.get_saved_lecturers(user.id)
            
            # Формируем приветственное сообщение
            message = (
                f"👋 Привет, {user.first_name}!\n\n"
                "🎓 Я помогу тебе узнать расписание занятий в АлтГУ.\n\n"
                "Что ты хочешь сделать?"
            )
            
            # Формируем клавиатуру
            keyboard = [
                [InlineKeyboardButton("🔍 Найти группу", callback_data='search_group'),
                 InlineKeyboardButton("🔍 Найти преподавателя", callback_data='search_lecturer')],
                [InlineKeyboardButton("📋 Технологические карты", callback_data='tech_cards')],
                [InlineKeyboardButton("⏰ Настройка уведомлений", callback_data='notifications')]
            ]
            
            # Добавляем кнопки сохраненных групп и преподавателей
            if saved_groups:
                keyboard.append([InlineKeyboardButton("📋 Сохранённые группы", callback_data='saved_groups')])
            if saved_lecturers:
                keyboard.append([InlineKeyboardButton("👨‍🏫 Сохранённые преподаватели", callback_data='saved_lecturers')])
            
            reply_markup = InlineKeyboardMarkup(keyboard)
            
            # Отправляем сообщение
            if update.callback_query:
                await update.callback_query.message.edit_text(
                    text=message,
                    reply_markup=reply_markup,
                    parse_mode='HTML'
                )
            else:
                await update.message.reply_text(
                    text=message,
                    reply_markup=reply_markup,
                    parse_mode='HTML'
                )
            
        except Exception as e:
            logging.error(f"Error in start command: {e}")
            error_message = "Произошла ошибка при запуске бота. Пожалуйста, попробуйте позже."
            if update.callback_query:
                await update.callback_query.message.reply_text(error_message)
            else:
                await update.message.reply_text(error_message)

    async def button_handler(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        query = update.callback_query
        user = query.from_user
        logging.info(f"User {user.id} pressed button with data: {query.data}")
        
        try:
            await query.answer()
        except Exception as e:
            logging.error(f"Error answering callback query: {e}")
            if "Query is too old" in str(e):
                if update.callback_query.message:
                    await self.start(update, context)
                return

        if query.data == 'start':
            if not update.callback_query.message:
                return
            
            # Получаем сохраненные группы и преподавателей
            saved_groups = self.db.get_saved_groups(user.id)
            saved_lecturers = self.db.get_saved_lecturers(user.id)
            
            # Формируем приветственное сообщение
            message = (
                f"👋 Привет, {user.first_name}!\n\n"
                "🎓 Я помогу тебе узнать расписание занятий в АлтГУ.\n\n"
                "Что ты хочешь сделать?"
            )
            
            # Формируем клавиатуру
            keyboard = [
                [InlineKeyboardButton("🔍 Найти группу", callback_data='search_group'),
                 InlineKeyboardButton("🔍 Найти преподавателя", callback_data='search_lecturer')],
                [InlineKeyboardButton("📋 Технологические карты", callback_data='tech_cards')],
                [InlineKeyboardButton("⏰ Настройка уведомлений", callback_data='notifications')]
            ]
            
            # Добавляем кнопки сохраненных групп и преподавателей
            if saved_groups:
                keyboard.append([InlineKeyboardButton("📋 Сохранённые группы", callback_data='saved_groups')])
            if saved_lecturers:
                keyboard.append([InlineKeyboardButton("👨‍🏫 Сохранённые преподаватели", callback_data='saved_lecturers')])
            
            reply_markup = InlineKeyboardMarkup(keyboard)
            await query.message.edit_text(
                text=message,
                reply_markup=reply_markup,
                parse_mode='HTML'
            )
            return

        elif query.data == 'search_group':
            await query.message.edit_text(
                "Введите номер группы для поиска:\n"
                "Например: 305с11-4"
            )
            context.user_data['state'] = 'waiting_for_group'

        elif query.data == 'search_lecturer':
            await query.message.edit_text(
                "Введите фамилию преподавателя для поиска:\n"
                "Например: Иванов"
            )
            context.user_data['state'] = 'waiting_for_lecturer'

        elif query.data == 'saved_groups':
            saved_groups = self.db.get_saved_groups(user.id)
            if saved_groups:
                keyboard = []
                for group in saved_groups:
                    keyboard.append([InlineKeyboardButton(
                        f"📚 {group['groupCode']}", 
                        callback_data=f"group_{group['groupId']}"
                    )])
                keyboard.append([InlineKeyboardButton("🔙 Вернуться в главное меню", callback_data='start')])
                reply_markup = InlineKeyboardMarkup(keyboard)
                await query.message.edit_text(
                    "Ваши сохранённые группы:",
                    reply_markup=reply_markup
                )
            else:
                keyboard = [[InlineKeyboardButton("🔙 Вернуться в главное меню", callback_data='start')]]
                reply_markup = InlineKeyboardMarkup(keyboard)
                await query.message.edit_text(
                    "У вас нет сохранённых групп.\n"
                    "Чтобы сохранить группу, найдите её через поиск и нажмите '⭐️ Сохранить группу'",
                    reply_markup=reply_markup
                )

        elif query.data == 'saved_lecturers':
            saved_lecturers = self.db.get_saved_lecturers(user.id)
            if saved_lecturers:
                keyboard = []
                for lecturer in saved_lecturers:
                    keyboard.append([InlineKeyboardButton(
                        f"👨‍🏫 {lecturer['lecturerName']}", 
                        callback_data=f"lecturer_{lecturer['lectureId']}"
                    )])
                keyboard.append([InlineKeyboardButton("🔙 Вернуться в главное меню", callback_data='start')])
                reply_markup = InlineKeyboardMarkup(keyboard)
                await query.message.edit_text(
                    "Ваши сохранённые преподаватели:",
                    reply_markup=reply_markup
                )
            else:
                keyboard = [[InlineKeyboardButton("🔙 Вернуться в главное меню", callback_data='start')]]
                reply_markup = InlineKeyboardMarkup(keyboard)
                await query.message.edit_text(
                    "У вас нет сохранённых преподавателей.\n"
                    "Чтобы сохранить преподавателя, найдите его через поиск и нажмите '⭐️ Сохранить преподавателя'",
                    reply_markup=reply_markup
                )

        elif query.data.startswith('lecturer_'):
            parts = query.data.split('_')
            lecturer_id = parts[1]
            request_type = parts[2] if len(parts) > 2 else 'today'

            try:
                if request_type == 'today':
                    date = datetime.now().strftime('%Y%m%d')
                elif request_type == 'tomorrow':
                    date = (datetime.now() + timedelta(days=1)).strftime('%Y%m%d')
                elif request_type == 'week':
                    today = datetime.now()
                    week_end = today + timedelta(days=6)
                    date = f"{today.strftime('%Y%m%d')}-{week_end.strftime('%Y%m%d')}"
                else:
                    date = datetime.now().strftime('%Y%m%d')

                schedule = await self.api.get_schedule(f"lecturers/15/65/{lecturer_id}", date=date)
                
                # Формируем клавиатуру
                keyboard = [
                    [InlineKeyboardButton("📅 На сегодня", callback_data=f"lecturer_{lecturer_id}_today"),
                     InlineKeyboardButton("📅 На завтра", callback_data=f"lecturer_{lecturer_id}_tomorrow")],
                    [InlineKeyboardButton("📆 На неделю", callback_data=f"lecturer_{lecturer_id}_week")],
                    [InlineKeyboardButton("🔄 Найти другого преподавателя", callback_data='search_lecturer')]
                ]
                
                # Проверяем, сохранен ли преподаватель
                if self.db.is_lecturer_saved(user.id, lecturer_id):
                    keyboard.append([InlineKeyboardButton("❌ Удалить из сохраненных", callback_data=f"delete_lecturer_{lecturer_id}")])
                else:
                    keyboard.append([InlineKeyboardButton("⭐️ Сохранить преподавателя", callback_data=f"save_lecturer_{lecturer_id}")])
                
                keyboard.append([InlineKeyboardButton("🔙 Вернуться в главное меню", callback_data='start')])
                reply_markup = InlineKeyboardMarkup(keyboard)

                message = self.format_schedule(schedule) if schedule and 'schedule' in schedule else "Расписание на сегодня отсутствует."
                
                try:
                    await query.message.edit_text(
                        text=message,
                        reply_markup=reply_markup,
                        parse_mode='HTML'
                    )
                except telegram.error.BadRequest as e:
                    if "Message is not modified" in str(e):
                        await query.answer()
                    else:
                        logging.error(f"Error editing message: {e}")
                        await query.message.reply_text(message, parse_mode='HTML', reply_markup=reply_markup)
            except Exception as e:
                logging.error(f"Error fetching lecturer schedule: {e}")
                keyboard = [[InlineKeyboardButton("🔙 Вернуться в главное меню", callback_data='start')]]
                await query.message.edit_text(
                    "Произошла ошибка при получении расписания преподавателя.",
                    reply_markup=InlineKeyboardMarkup(keyboard)
                )

        elif query.data.startswith('group_'):
            group_id = query.data.split('_')[1]
            try:
                today = datetime.now().strftime('%Y%m%d')
                schedule = await self.api.get_schedule(f"{group_id}", date=today)
                
                # Формируем клавиатуру
                keyboard = [
                    [InlineKeyboardButton("📅 На сегодня", callback_data=f"group_{group_id}"),
                     InlineKeyboardButton("📅 На завтра", callback_data=f"tomorrow_{group_id}")],
                    [InlineKeyboardButton("📆 На неделю", callback_data=f"week_{group_id}")],
                    [InlineKeyboardButton("🔄 Искать другую группу", callback_data='search_group')]
                ]
                
                # Проверяем, сохранена ли группа
                if self.db.is_group_saved(user.id, group_id):
                    keyboard.append([InlineKeyboardButton("❌ Удалить из сохраненных", callback_data=f"delete_group_{group_id}")])
                else:
                    keyboard.append([InlineKeyboardButton("⭐️ Сохранить группу", callback_data=f"save_group_{group_id}")])
                
                keyboard.append([InlineKeyboardButton("🔙 Вернуться в главное меню", callback_data='start')])
                reply_markup = InlineKeyboardMarkup(keyboard)

                message = self.format_schedule(schedule) if schedule else "Расписание на сегодня отсутствует."
                
                try:
                    await query.message.edit_text(
                        text=message,
                        reply_markup=reply_markup,
                        parse_mode='HTML'
                    )
                except telegram.error.BadRequest as e:
                    if "Message is not modified" in str(e):
                        await query.answer()
                    else:
                        logging.error(f"Error editing message: {e}")
                        await query.message.reply_text(message, parse_mode='HTML', reply_markup=reply_markup)
            except Exception as e:
                logging.error(f"Error fetching schedule: {e}")
                keyboard = [[InlineKeyboardButton("🔙 Вернуться в главное меню", callback_data='start')]]
                await query.message.edit_text(
                    "Произошла ошибка при получении расписания.",
                    reply_markup=InlineKeyboardMarkup(keyboard)
                )

        elif query.data.startswith('tomorrow_'):
            group_id = query.data.split('_')[1]
            try:
                tomorrow = (datetime.now() + timedelta(days=1)).strftime('%Y%m%d')
                schedule = await self.api.get_schedule(f"{group_id}", date=tomorrow)
                
                # Получаем информацию о группе из базы данных
                group = self.db.get_group_by_id(group_id)
                if group and schedule:
                    schedule['requested_group'] = group  # Добавляем информацию о запрошенной группе
                
                if schedule and 'schedule' in schedule:
                    message = self.format_schedule(schedule)
                    keyboard = [
                        [InlineKeyboardButton("📅 На сегодня", callback_data=f"group_{group_id}"),
                         InlineKeyboardButton("📆 На неделю", callback_data=f"week_{group_id}")],
                        [InlineKeyboardButton("⭐️ Сохранить группу", callback_data=f"save_group_{group_id}")],
                        [InlineKeyboardButton("🔄 Искать другую группу", callback_data='search_group')],
                        [InlineKeyboardButton("🔙 Вернуться в главное меню", callback_data='start')]
                    ]
                    reply_markup = InlineKeyboardMarkup(keyboard)
                    await query.message.edit_text(message, parse_mode='HTML', reply_markup=reply_markup)
                else:
                    keyboard = [
                        [InlineKeyboardButton("📅 На сегодня", callback_data=f"group_{group_id}"),
                         InlineKeyboardButton("📆 На неделю", callback_data=f"week_{group_id}")],
                        [InlineKeyboardButton("⭐️ Сохранить группу", callback_data=f"save_group_{group_id}")],
                        [InlineKeyboardButton("🔄 Искать другую группу", callback_data='search_group')],
                        [InlineKeyboardButton("🔙 Вернуться в главное меню", callback_data='start')]
                    ]
                    await query.message.edit_text(
                        "Расписание на завтра отсутствует.",
                        reply_markup=InlineKeyboardMarkup(keyboard)
                    )
            except Exception as e:
                logging.error(f"Error fetching schedule: {e}")
                await query.message.edit_text("Произошла ошибка при получении расписания.")

        elif query.data.startswith('week_'):
            group_id = query.data.split('_')[1]
            try:
                today = datetime.now()
                week_end = today + timedelta(days=6)
                date_range = f"{today.strftime('%Y%m%d')}-{week_end.strftime('%Y%m%d')}"
                schedule = await self.api.get_schedule(f"{group_id}", date=date_range)
                
                if schedule and 'schedule' in schedule and schedule['schedule'].get('records', []):
                    message = self.format_schedule(schedule)
                    keyboard = [
                        [InlineKeyboardButton("📅 На сегодня", callback_data=f"group_{group_id}"),
                         InlineKeyboardButton("📅 На завтра", callback_data=f"tomorrow_{group_id}")],
                        [InlineKeyboardButton("⭐️ Сохранить группу", callback_data=f"save_group_{group_id}")],
                        [InlineKeyboardButton("🔄 Искать другую группу", callback_data='search_group')],
                        [InlineKeyboardButton("🔙 Вернуться в главное меню", callback_data='start')]
                    ]
                    reply_markup = InlineKeyboardMarkup(keyboard)
                    try:
                        await query.message.edit_text(message, parse_mode='HTML', reply_markup=reply_markup)
                    except telegram.error.BadRequest as e:
                        if "Message is not modified" in str(e):
                            # Игнорируем эту ошибку, так как сообщение не изменилось
                            await query.answer()
                        else:
                            # Для других ошибок пытаемся отправить новое сообщение
                            logging.error(f"Error editing message: {e}")
                            await query.message.reply_text(message, parse_mode='HTML', reply_markup=reply_markup)
                    except Exception as e:
                        logging.error(f"Error editing message: {e}")
                        await query.message.reply_text(message, parse_mode='HTML', reply_markup=reply_markup)
                else:
                    keyboard = [
                        [InlineKeyboardButton("📅 На сегодня", callback_data=f"group_{group_id}"),
                         InlineKeyboardButton("📅 На завтра", callback_data=f"tomorrow_{group_id}")],
                        [InlineKeyboardButton("⭐️ Сохранить группу", callback_data=f"save_group_{group_id}")],
                        [InlineKeyboardButton("🔄 Искать другую группу", callback_data='search_group')],
                        [InlineKeyboardButton("🔙 Вернуться в главное меню", callback_data='start')]
                    ]
                    await query.message.edit_text(
                        "Расписание на неделю отсутствует.",
                        reply_markup=InlineKeyboardMarkup(keyboard)
                    )
            except Exception as e:
                logging.error(f"Error fetching schedule: {e}")
                keyboard = [[InlineKeyboardButton("🔙 Вернуться в главное меню", callback_data='start')]]
                await query.message.edit_text(
                    "Произошла ошибка при получении расписания.",
                    reply_markup=InlineKeyboardMarkup(keyboard)
                )

        elif query.data.startswith('save_group_'):
            group_id = query.data.split('_')[2]
            try:
                if self.db.save_user_group(user.id, group_id, user.username):
                    await query.message.reply_text(
                        "✅ Группа успешно сохранена!\n"
                        "Теперь вы можете быстро получать расписание этой группы через меню"
                    )
                else:
                    await query.message.reply_text("ℹ️ Эта группа уже сохранена")
            except Exception as e:
                logging.error(f"Error saving group: {e}")
                await query.message.reply_text(
                    "❌ Произошла ошибка при сохранении группы\n"
                    "Пожалуйста, попробуйте позже"
                )

        elif query.data.startswith('save_lecturer_'):
            lecturer_id = query.data.split('_')[2]
            try:
                if self.db.save_user_lecturer(user.id, lecturer_id, user.username):
                    await query.message.reply_text(
                        "✅ Преподаватель успешно сохранен!\n"
                        "Теперь вы можете быстро получать его расписание через меню"
                    )
                else:
                    await query.message.reply_text("ℹ️ Этот преподаватель уже сохранен")
            except Exception as e:
                logging.error(f"Error saving lecturer: {e}")
                await query.message.reply_text(
                    "❌ Произошла ошибка при сохранении преподавателя\n"
                    "Пожалуйста, попробуйте позже"
                )

        elif query.data.startswith('delete_group_'):
            group_id = query.data.split('_')[2]
            try:
                if self.db.delete_saved_group(user.id, group_id):
                    await query.answer("✅ Группа удалена из сохраненных")
                    # Обновляем сообщение с новой клавиатурой
                    await self.start(update, context)
                else:
                    await query.answer("❌ Не удалось удалить группу")
            except Exception as e:
                logging.error(f"Error deleting group: {e}")
                await query.answer("Произошла ошибка при удалении группы")

        elif query.data.startswith('delete_lecturer_'):
            lecturer_id = query.data.split('_')[2]
            try:
                if self.db.delete_saved_lecturer(user.id, lecturer_id):
                    await query.answer("✅ Преподаватель удален из сохраненных")
                else:
                    await query.answer("❌ Не удалось удалить преподавателя")
            except Exception as e:
                logging.error(f"Error deleting lecturer: {e}")
                await query.answer("Произошла ошибка при удалении преподавателя")

        elif query.data == 'tech_cards':
            # Получаем сохраненные группы пользователя
            saved_groups = self.db.get_saved_groups(user.id)
            
            if not saved_groups:
                message = (
                    "📚 <b>Технологические карты</b>\n\n"
                    "У вас нет сохраненных групп. Сначала добавьте группу в избранное, "
                    "чтобы получить доступ к технологическим картам."
                )
                keyboard = [[InlineKeyboardButton("🔙 Вернуться в главное меню", callback_data='start')]]
            else:
                message = (
                    "📚 <b>Технологические карты</b>\n\n"
                    "Выберите группу для просмотра технологической карты:"
                )
                
                # Формируем клавиатуру из сохраненных групп
                keyboard = []
                for group in saved_groups:
                    # Проверяем наличие техкарты для группы
                    techcard = self.db.get_techcard_by_group(group['groupId'])
                    if techcard:  # Показываем только группы с техкартами
                        keyboard.append([
                            InlineKeyboardButton(
                                f"📋 {group['groupCode']}", 
                                callback_data=f"techcard_{group['groupId']}"
                            )
                        ])
                
                if not keyboard:  # Если нет групп с техкартами
                    message = (
                        "📚 <b>Технологические карты</b>\n\n"
                        "Для ваших сохраненных групп пока нет доступных технологических карт."
                    )
                    keyboard = [[InlineKeyboardButton("🔙 Вернуться в главное меню", callback_data='start')]]
                else:
                    keyboard.append([InlineKeyboardButton("🔙 Вернуться в главное меню", callback_data='start')])
            
            reply_markup = InlineKeyboardMarkup(keyboard)
            await query.message.edit_text(
                message,
                reply_markup=reply_markup,
                parse_mode='HTML'
            )

        elif query.data.startswith('techcard_'):
            group_id = query.data.split('_')[1]
            try:
                techcard = self.db.get_techcard_by_group(group_id)
                group = self.db.get_group_by_id(group_id)
                
                if techcard and group:
                    message = (
                        f"📚 <b>Технологическая карта группы {group['groupCode']}</b>\n\n"
                        f"🔗 <a href='{techcard['techcard_url']}'>Открыть технологическую карту</a>"
                    )
                else:
                    message = "❌ Технологическая карта не найдена"
                
                keyboard = [[InlineKeyboardButton("🔙 Вернуться к выбору группы", callback_data='tech_cards')]]
                reply_markup = InlineKeyboardMarkup(keyboard)
                
                await query.message.edit_text(
                    message,
                    reply_markup=reply_markup,
                    parse_mode='HTML'
                )
            except Exception as e:
                logging.error(f"Error getting techcard: {e}")
                await query.message.edit_text(
                    "❌ Произошла ошибка при получении технологической карты",
                    reply_markup=InlineKeyboardMarkup([[
                        InlineKeyboardButton("🔙 Вернуться в главное меню", callback_data='start')
                    ]])
                )

        elif query.data == 'admin':
            await self.admin_panel(update, context)

        elif query.data == 'admin_techcards':
            await self.admin_techcards_handler(update, context)

        elif query.data == 'admin_detailed_stats':
            if query.from_user.id not in ADMIN_IDS:
                await query.answer("⛔️ У вас нет доступа к этой функции")
                return
            
            # Здесь можно добавить более подробную статистику
            stats = self.db.get_bot_stats()
            message = (
                "📊 <b>Подробная статистика</b>\n\n"
                f"👥 Всего пользователей: {stats['total_users']}\n"
                f"📈 Новых за 24 часа: {stats['new_users_24h']}\n"
                f"👥 Сохранённых групп: {stats['saved_groups']}\n"
                f"👨‍🏫 Сохранённых преподавателей: {stats['saved_lecturers']}\n"
                f"📚 Технологических карт: {stats['techcards']}\n"
            )
            
            keyboard = [[InlineKeyboardButton("🔙 Вернуться в админ-панель", callback_data='admin')]]
            reply_markup = InlineKeyboardMarkup(keyboard)
            
            await query.message.edit_text(message, reply_markup=reply_markup, parse_mode='HTML')

        elif query.data == 'admin_users':
            await self.admin_users_handler(update, context)

        elif query.data == 'admin_users_active':
            await self.admin_users_active_handler(update, context)

        elif query.data == 'admin_users_banned':
            await self.admin_users_banned_handler(update, context)

        elif query.data == 'admin_users_search':
            await self.admin_users_search_handler(update, context)

        elif query.data == 'admin_users_mass':
            await self.admin_users_mass_handler(update, context)

        elif query.data == 'admin_broadcast':
            await self.admin_broadcast_handler(update, context)

        elif query.data == 'admin_system':
            await self.admin_system_handler(update, context)

        elif query.data == 'admin_graphs':
            await self.admin_graphs_handler(update, context)

        elif query.data.startswith('admin_users_active_'):
            action = query.data.split('_')[-1]
            page = int(context.user_data.get('active_page', 1))
            if action == 'next':
                context.user_data['active_page'] = page + 1
            elif action == 'prev':
                context.user_data['active_page'] = max(1, page - 1)
            await self.admin_users_active_handler(update, context)

        elif query.data == 'notify_morning' or query.data == 'notify_evening' or query.data == 'notify_disable':
            await self.schedule_notification_handler(update, context)

        elif query.data == 'export_google' or query.data == 'export_apple' or query.data == 'export_ical':
            await self.export_schedule(update, context)

        elif query.data == 'notifications':
            await self.notification_settings(update, context)

        elif query.data.startswith('notify_setup_'):
            await self.setup_notification_time(update, context)

        elif query.data.startswith('set_notify_'):
            _, _, group_id, time = query.data.split('_')
            if self.db.set_notification(user.id, group_id, time):
                await query.message.edit_text(
                    f"✅ Уведомления настроены на {time}",
                    reply_markup=InlineKeyboardMarkup([[
                        InlineKeyboardButton("🔙 Вернуться в главное меню", callback_data='start')
                    ]])
                )
            else:
                await query.message.edit_text(
                    "❌ Ошибка при настройке уведомлений",
                    reply_markup=InlineKeyboardMarkup([[
                        InlineKeyboardButton("🔙 Вернуться в главное меню", callback_data='start')
                    ]])
                )

        elif query.data.startswith('notify_disable_'):
            group_id = query.data.split('_')[2]
            if self.db.disable_notification(user.id, group_id):
                await query.message.edit_text(
                    "✅ Уведомления отключены",
                    reply_markup=InlineKeyboardMarkup([[
                        InlineKeyboardButton("🔙 Вернуться в главное меню", callback_data='start')
                    ]])
                )
            else:
                await query.message.edit_text(
                    "❌ Ошибка при отключении уведомлений",
                    reply_markup=InlineKeyboardMarkup([[
                        InlineKeyboardButton("🔙 Вернуться в главное меню", callback_data='start')
                    ]])
                )

        elif query.data.startswith(('next_page_', 'prev_page_')):
            # Получаем параметры из callback_data
            parts = query.data.split('_')
            direction = parts[0]  # next или prev
            search_query = parts[2]  # текст поиска
            page = int(parts[3])  # номер страницы
            
            # Получаем результаты поиска
            groups = self.db.search_groups(search_query)
            
            # Настройки пагинации
            GROUPS_PER_PAGE = 10
            total_pages = (len(groups) + GROUPS_PER_PAGE - 1) // GROUPS_PER_PAGE
            
            # Проверяем валидность страницы
            if page < 1:
                page = 1
            elif page > total_pages:
                page = total_pages
            
            # Получаем группы для текущей страницы
            start_idx = (page - 1) * GROUPS_PER_PAGE
            end_idx = start_idx + GROUPS_PER_PAGE
            current_groups = groups[start_idx:end_idx]
            
            # Формируем клавиатуру
            keyboard = []
            for group in current_groups:
                keyboard.append([
                    InlineKeyboardButton(
                        f"📋 {group['groupCode']} ({group['facultyTitle']})",
                        callback_data=f"group_{group['groupId']}"
                    )
                ])
            
            # Добавляем навигационные кнопки
            nav_buttons = []
            if page > 1:
                nav_buttons.append(
                    InlineKeyboardButton(
                        "⬅️ Предыдущая",
                        callback_data=f"prev_page_{search_query}_{page - 1}"
                    )
                )
            
            nav_buttons.append(
                InlineKeyboardButton(
                    f"📄 Стр. {page}/{total_pages}",
                    callback_data="current_page"
                )
            )
            
            if page < total_pages:
                nav_buttons.append(
                    InlineKeyboardButton(
                        "➡️ Следующая",
                        callback_data=f"next_page_{search_query}_{page + 1}"
                    )
                )
            
            keyboard.append(nav_buttons)
            keyboard.append([InlineKeyboardButton("🔍 Новый поиск", callback_data='search_group')])
            keyboard.append([InlineKeyboardButton("🔙 Вернуться в главное меню", callback_data='start')])
            
            reply_markup = InlineKeyboardMarkup(keyboard)
            try:
                await query.message.edit_text(
                    f"🔍 Результаты поиска группы ({len(groups)} найдено):\n"
                    "Выберите группу из списка:",
                    reply_markup=reply_markup
                )
            except telegram.error.BadRequest as e:
                if "Message is not modified" in str(e):
                    await query.answer("Вы уже на этой странице")
                else:
                    raise

        elif query.data.startswith(('next_lecturer_', 'prev_lecturer_')):
            parts = query.data.split('_')
            direction = parts[0]  # next или prev
            search_query = parts[2]  # текст поиска
            page = int(parts[3])  # номер страницы
            
            lecturers = self.db.search_lecturers(search_query)
            LECTURERS_PER_PAGE = 10
            total_pages = (len(lecturers) + LECTURERS_PER_PAGE - 1) // LECTURERS_PER_PAGE
            
            if page < 1:
                page = 1
            elif page > total_pages:
                page = total_pages
            
            start_idx = (page - 1) * LECTURERS_PER_PAGE
            end_idx = start_idx + LECTURERS_PER_PAGE
            current_lecturers = lecturers[start_idx:end_idx]
            
            keyboard = []
            for lecturer in current_lecturers:
                keyboard.append([
                    InlineKeyboardButton(
                        f"👨‍🏫 {lecturer['lecturerName']} ({lecturer['facultyTitle']})",
                        callback_data=f"lecturer_{lecturer['lectureId']}"
                    )
                ])
            
            nav_buttons = []
            if page > 1:
                nav_buttons.append(
                    InlineKeyboardButton(
                        "⬅️ Предыдущая",
                        callback_data=f"prev_lecturer_{search_query}_{page - 1}"
                    )
                )
            
            nav_buttons.append(
                InlineKeyboardButton(
                    f"📄 Стр. {page}/{total_pages}",
                    callback_data="current_page"
                )
            )
            
            if page < total_pages:
                nav_buttons.append(
                    InlineKeyboardButton(
                        "➡️ Следующая",
                        callback_data=f"next_lecturer_{search_query}_{page + 1}"
                    )
                )
            
            keyboard.append(nav_buttons)
            keyboard.append([InlineKeyboardButton("🔍 Новый поиск", callback_data='search_lecturer')])
            keyboard.append([InlineKeyboardButton("🔙 Вернуться в главное меню", callback_data='start')])
            
            reply_markup = InlineKeyboardMarkup(keyboard)
            try:
                await query.message.edit_text(
                    f"🔍 Результаты поиска преподавателя ({len(lecturers)} найдено):\n"
                    "Выберите преподавателя из списка:",
                    reply_markup=reply_markup
                )
            except telegram.error.BadRequest as e:
                if "Message is not modified" in str(e):
                    await query.answer("Вы уже на этой странице")
                else:
                    raise

        elif query.data == 'admin_techcard_list':
            await self.admin_techcard_list_handler(update, context)

        elif query.data == 'admin_techcard_add':
            await self.admin_techcard_add_handler(update, context)

        elif query.data.startswith('admin_techcard_'):
            action = query.data.split('_')[-1]
            if action in ['next', 'prev']:
                page = int(context.user_data.get('techcard_page', 1))
                if action == 'next':
                    context.user_data['techcard_page'] = page + 1
                else:
                    context.user_data['techcard_page'] = max(1, page - 1)
                await self.admin_techcard_list_handler(update, context)

        elif query.data == 'admin_clear_cache':
            await self.admin_clear_cache_handler(update, context)
        
        elif query.data == 'admin_check_db':
            await self.admin_check_db_handler(update, context)
        
        elif query.data == 'admin_techcard_stats':
            await self.admin_techcard_stats_handler(update, context)
        
        elif query.data == 'admin_techcard_search':
            await self.admin_techcard_search_handler(update, context)

        # Обработка состояния ожидания текста для рассылки
        if context.user_data.get('state') == 'waiting_for_broadcast':
            if update.effective_user.id not in ADMIN_IDS:
                return
            
            await self.process_broadcast(update, context)
            return

    async def admin_panel(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Админ-панель"""
        user_id = update.effective_user.id
        if user_id not in ADMIN_IDS:
            await update.message.reply_text("⛔️ У вас нет доступа к админ-панели")
            return

        try:
            stats = self.db.get_bot_stats()
            
            message = (
                "👨‍💼 <b>Админ-панель</b>\n\n"
                "<b>📊 Статистика:</b>\n"
                f"• Всего пользователей: {stats['total_users']}\n"
                f"• Новых за 24 часа: {stats['new_users_24h']}\n"
                f"• Сохранённых групп: {stats['saved_groups']}\n"
                f"• Сохранённых преподавателей: {stats['saved_lecturers']}\n"
                f"• Технологических карт: {stats['techcards']}\n\n"
                "Выберите действие:"
            )
            
            keyboard = [
                [InlineKeyboardButton("👥 Управление пользователями", callback_data='admin_users')],
                [InlineKeyboardButton("📝 Управление техкартами", callback_data='admin_techcards')],
                [InlineKeyboardButton("📊 Подробная статистика", callback_data='admin_detailed_stats')],
                [InlineKeyboardButton("📨 Рассылка всем", callback_data='admin_broadcast')],
                [InlineKeyboardButton("🔄 Система", callback_data='admin_system')],
                [InlineKeyboardButton("📈 Графики", callback_data='admin_graphs')],
                [InlineKeyboardButton("🔙 Вернуться в главное меню", callback_data='start')]
            ]
            
            reply_markup = InlineKeyboardMarkup(keyboard)
            if update.callback_query:
                await update.callback_query.message.edit_text(message, reply_markup=reply_markup, parse_mode='HTML')
            else:
                await update.message.reply_text(message, reply_markup=reply_markup, parse_mode='HTML')
        
        except Exception as e:
            logging.error(f"Error in admin panel: {e}")
            await update.message.reply_text("❌ Произошла ошибка при загрузке админ-панели")

    async def admin_techcards_handler(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработчик управления техкартами"""
        query = update.callback_query
        if query.from_user.id not in ADMIN_IDS:
            await query.answer("⛔️ Нет доступа")
            return

        # Получаем статистику техкарт
        stats = self.db.get_techcards_stats()

        message = (
            "📚 <b>Управление технологическими картами</b>\n\n"
            f"Всего техкарт: {stats['total']}\n"
            f"Добавлено за неделю: {stats['added_week']}\n"
            f"Обновлено за неделю: {stats['updated_week']}\n\n"
            "<b>Действия:</b>"
        )
        
        keyboard = [
            [InlineKeyboardButton("➕ Добавить техкарту", callback_data='admin_techcard_add')],
            [InlineKeyboardButton("📋 Список техкарт", callback_data='admin_techcard_list')],
            [InlineKeyboardButton("🔍 Поиск техкарты", callback_data='admin_techcard_search')],
            [InlineKeyboardButton("📊 Статистика", callback_data='admin_techcard_stats')],
            [InlineKeyboardButton("🔙 Назад", callback_data='admin')]
        ]
        
        reply_markup = InlineKeyboardMarkup(keyboard)
        await query.message.edit_text(message, reply_markup=reply_markup, parse_mode='HTML')

    async def admin_techcard_list_handler(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Список техкарт"""
        query = update.callback_query
        page = int(context.user_data.get('techcard_page', 1))
        
        techcards = self.db.get_all_techcards()
        ITEMS_PER_PAGE = 10
        total_pages = (len(techcards) + ITEMS_PER_PAGE - 1) // ITEMS_PER_PAGE
        
        start_idx = (page - 1) * ITEMS_PER_PAGE
        end_idx = start_idx + ITEMS_PER_PAGE
        current_items = techcards[start_idx:end_idx]
        
        message = "📚 <b>Список технологических карт</b>\n\n"
        for tc in current_items:
            message += (
                f"• Группа: {tc['groupCode']}\n"
                f"  Добавлено: {tc['created_at'].strftime('%d.%m.%Y')}\n"
                f"  Обновлено: {tc['updated_at'].strftime('%d.%m.%Y')}\n"
                f"  URL: {tc['techcard_url'][:50]}...\n\n"
            )
        
        keyboard = []
        nav_buttons = []
        
        if page > 1:
            nav_buttons.append(InlineKeyboardButton("⬅️", callback_data='admin_techcard_prev'))
        nav_buttons.append(InlineKeyboardButton(f"{page}/{total_pages}", callback_data='current_page'))
        if page < total_pages:
            nav_buttons.append(InlineKeyboardButton("➡️", callback_data='admin_techcard_next'))
        
        if nav_buttons:
            keyboard.append(nav_buttons)
        keyboard.append([InlineKeyboardButton("🔙 Назад", callback_data='admin_techcards')])
        
        reply_markup = InlineKeyboardMarkup(keyboard)
        await query.message.edit_text(message, reply_markup=reply_markup, parse_mode='HTML')

    async def admin_techcard_add_handler(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Добавление техкарты"""
        query = update.callback_query
        message = (
            "📚 <b>Добавление технологической карты</b>\n\n"
            "Отправьте данные в формате:\n"
            "<code>тк_добавить ГРУППА URL</code>\n\n"
            "Например:\n"
            "<code>тк_добавить К402с9-1 https://example.com/techcard.pdf</code>\n\n"
            "Для отмены нажмите кнопку «Отмена»"
        )
        
        keyboard = [[InlineKeyboardButton("🔙 Отмена", callback_data='admin_techcards')]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await query.message.edit_text(message, reply_markup=reply_markup, parse_mode='HTML')
        context.user_data['state'] = 'waiting_for_techcard_add'

    async def admin_users_handler(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработчик управления пользователями"""
        query = update.callback_query
        user_id = query.from_user.id
        
        if user_id not in ADMIN_IDS:
            await query.answer("⛔️ Нет доступа")
            return

        # Получаем статистику пользователей
        total_users = len(self.db.get_all_users())
        active_users = len(self.db.get_active_users_last_24h())
        banned_users = len([u for u in self.db.get_all_users() if u.get('is_banned')])

        message = (
            "👥 <b>Управление пользователями</b>\n\n"
            f"Всего пользователей: {total_users}\n"
            f"Активных за 24ч: {active_users}\n"
            f"Заблокированных: {banned_users}\n\n"
            "Выберите действие:"
        )
        
        keyboard = [
            [InlineKeyboardButton("📊 Список активных", callback_data='admin_users_active')],
            [InlineKeyboardButton("🚫 Список забаненных", callback_data='admin_users_banned')],
            [InlineKeyboardButton("🔍 Поиск пользователя", callback_data='admin_users_search')],
            [InlineKeyboardButton("📝 Массовые действия", callback_data='admin_users_mass')],
            [InlineKeyboardButton("🔙 Назад", callback_data='admin')]
        ]
        
        reply_markup = InlineKeyboardMarkup(keyboard)
        await query.message.edit_text(message, reply_markup=reply_markup, parse_mode='HTML')

    async def admin_users_active_handler(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Список активных пользователей"""
        query = update.callback_query
        page = int(context.user_data.get('active_page', 1))
        
        active_users = self.db.get_active_users_last_24h()
        USERS_PER_PAGE = 10
        total_pages = (len(active_users) + USERS_PER_PAGE - 1) // USERS_PER_PAGE
        
        start_idx = (page - 1) * USERS_PER_PAGE
        end_idx = start_idx + USERS_PER_PAGE
        current_users = active_users[start_idx:end_idx]
        
        message = "👥 <b>Активные пользователи (24ч)</b>\n\n"
        for user in current_users:
            message += (
                f"• ID: {user['tg_id']}\n"
                f"  Username: @{user['username'] or 'нет'}\n"
                f"  Последняя активность: {user['last_activity']}\n\n"
            )
        
        keyboard = []
        nav_buttons = []
        
        if page > 1:
            nav_buttons.append(InlineKeyboardButton("⬅️", callback_data='admin_users_active_prev'))
        nav_buttons.append(InlineKeyboardButton(f"{page}/{total_pages}", callback_data='current_page'))
        if page < total_pages:
            nav_buttons.append(InlineKeyboardButton("➡️", callback_data='admin_users_active_next'))
        
        if nav_buttons:
            keyboard.append(nav_buttons)
        keyboard.append([InlineKeyboardButton("🔙 Назад", callback_data='admin_users')])
        
        reply_markup = InlineKeyboardMarkup(keyboard)
        await query.message.edit_text(message, reply_markup=reply_markup, parse_mode='HTML')

    async def admin_users_search_handler(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Поиск пользователей"""
        query = update.callback_query
        message = (
            "🔍 <b>Поиск пользователя</b>\n\n"
            "Отправьте ID или username пользователя.\n"
            "Поддерживаемые форматы:\n"
            "• ID: 123456789\n"
            "• Username: @username\n\n"
            "Для отмены нажмите кнопку «Назад»"
        )
        
        keyboard = [[InlineKeyboardButton("🔙 Назад", callback_data='admin_users')]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await query.message.edit_text(message, reply_markup=reply_markup, parse_mode='HTML')
        context.user_data['state'] = 'waiting_for_user_search'

    async def check_ban(self, user_id):
        """Проверка на бан пользователя"""
        try:
            user = self.db.get_user_by_tg_id(user_id)
            if user and user.get('is_banned'):
                return True
            return False
        except Exception as e:
            logging.error(f"Error checking ban status: {e}")
            return False

    async def message_handler(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        try:
            # Проверяем наличие сообщения
            if not update.message:
                return
            
            # Получаем текст сообщения
            message_text = update.message.text
            if not message_text:
                return
            
            # Проверяем тип чата
            chat_type = update.message.chat.type
            is_group_chat = chat_type in ['group', 'supergroup']
            
            # В групповых чатах обрабатываем только команды и ответы на сообщения бота
            if is_group_chat:
                # Проверяем, является ли сообщение командой
                if not message_text.startswith('/'):
                    # Проверяем, является ли сообщение ответом на сообщение бота
                    if not (update.message.reply_to_message and 
                           update.message.reply_to_message.from_user.id == context.bot.id):
                        return
                    
            # Получаем информацию о пользователе
            user = update.effective_user
            if not user:
                await update.message.reply_text("❌ Не удалось определить пользователя")
                return
            
            # Проверяем, является ли сообщение командой
            if message_text.startswith('/'):
                command = message_text.split('@')[0]  # Убираем username бота из команды
                if command == '/start':
                    await self.send_welcome_message(update.message)
                    return
                # Добавьте другие команды по необходимости
                
            # Существующий код обработки сообщений...
            
        except Exception as e:
            error_msg = f"❌ Произошла ошибка:\n{str(e)}\n"
            if update.effective_user:
                error_msg += f"Пользователь ID: {update.effective_user.id}\n"
                error_msg += f"Username: @{update.effective_user.username}\n"
            if update.message and update.message.chat:
                error_msg += f"Чат ID: {update.message.chat.id}"
            
            await update.message.reply_text(error_msg)
            logging.error(f"Error in message handler: {str(e)}", exc_info=True)

    async def get_current_week(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Команда для получения информации о текущей неделе"""
        week_num = datetime.now().isocalendar()[1]
        week_type = "🔴 Красная" if week_num % 2 == 1 else "🔵 Синяя"
        await update.message.reply_text(f"Сейчас идёт {week_type} неделя")

    def format_schedule(self, schedule_data):
        if not schedule_data or 'schedule' not in schedule_data:
            return "Расписание отсутствует"

        records = schedule_data['schedule'].get('records', [])
        if not records:
            return "Расписание отсутствует"

        # Получаем информацию о группе/преподавателе
        schedule_type = ""
        is_group_schedule = False
        target_group = None

        # Проверяем сначала requested_group
        if 'requested_group' in schedule_data:
            group = schedule_data['requested_group']
            schedule_type = f"группы {group['groupCode']}"
            target_group = group['groupCode']
            is_group_schedule = True
        elif 'url' in schedule_data:
            url = schedule_data['url']
            if 'lecturers' in url:
                # Это расписание преподавателя
                lecturer_id = url.split('/')[-2]  # Получаем ID преподавателя из URL
                lecturer = self.db.get_lecturer_by_id(lecturer_id)
                if lecturer:
                    schedule_type = f"преподавателя {lecturer['lecturerName']}"
            else:
                # Это расписание группы
                group_id = url.split('/')[-1].rstrip('/')  # Получаем ID группы из URL
                # Ищем группу в первой записи
                for lesson in records:
                    for group_info in lesson['lessonGroups']:
                        if str(group_info['lessonGroup']['groupId']) == group_id:
                            group = group_info['lessonGroup']
                            schedule_type = f"группы {group['groupCode']}"
                            target_group = group['groupCode']
                            is_group_schedule = True
                            break
                    if is_group_schedule:
                        break

        formatted_text = f"<b>📅 РАСПИСАНИЕ {schedule_type}</b>\n\n"

        # Группируем занятия по датам
        lessons_by_date = {}
        for lesson in records:
            # Фильтруем занятия только для запрошенной группы
            if is_group_schedule:
                lesson_groups = [g['lessonGroup']['groupCode'] for g in lesson['lessonGroups']]
                if target_group not in lesson_groups:
                    continue

            date = lesson['lessonDate']
            if date not in lessons_by_date:
                lessons_by_date[date] = []
            lessons_by_date[date].append(lesson)

        weekdays = {
            'Monday': 'Понедельник',
            'Tuesday': 'Вторник',
            'Wednesday': 'Среда',
            'Thursday': 'Четверг',
            'Friday': 'Пятница',
            'Saturday': 'Суббота',
            'Sunday': 'Воскресенье'
        }

        # Словарь для перевода типов занятий
        lesson_types = {
            'лек.': 'Лекция',
            'пр.з.': 'Практика',
            'лаб.': 'Лабораторная'
        }

        for date, lessons in sorted(lessons_by_date.items()):
            date_obj = datetime.strptime(date, '%Y%m%d')
            weekday = weekdays.get(date_obj.strftime('%A'), date_obj.strftime('%A'))
            formatted_text += f"<b>📆 {date_obj.strftime('%d.%m.%Y')} ({weekday})</b>\n"
            formatted_text += "━━━━━━━━━━━━━━━━━━━━━\n"

            lessons.sort(key=lambda x: x['lessonTimeStart'])
            for lesson in lessons:
                # Номер пары
                lesson_num = lesson.get('lessonNum', '')
                if lesson_num:
                    formatted_text += f"<b>{lesson_num} пара</b>\n"

                # Основная информация
                time = f"{lesson['lessonTimeStart']}-{lesson['lessonTimeEnd']}"
                subject = lesson['lessonSubject']['subjectTitle']
                lesson_type = lesson_types.get(lesson['lessonSubjectType'], lesson['lessonSubjectType'] or 'Занятие')
                week_type = lesson.get('lessonWeek', '').replace('Красная', '🔴').replace('Синяя', '🔵')

                formatted_text += f"🕒 <b>{time}</b> | {subject}\n"
                formatted_text += f"📝 {lesson_type}"
                if week_type:
                    formatted_text += f" | {week_type}"
                formatted_text += "\n"

                # Добавляем информацию о преподавателях
                if lesson['lessonLecturers']:
                    teachers = [l['lecturerName'] for l in lesson['lessonLecturers']]
                    formatted_text += f"👨‍🏫 {', '.join(teachers)}\n"

                # Группы показываем только для расписания преподавателя
                if not is_group_schedule:
                    groups = [g['lessonGroup']['groupCode'] for g in lesson['lessonGroups']]
                    formatted_text += f"Группы: {', '.join(groups)}\n"

                # Местоположение
                room = lesson['lessonRoom'].get('roomTitle', '')
                building = lesson['lessonBuilding'].get('buildingCode', '')
                building_address = lesson['lessonBuilding'].get('buildingAddress', '')
                
                if room and building:
                    location = f"ауд. {room}, корпус {building}"
                    if building_address:
                        location += f" ({building_address})"
                elif building_address:
                    location = building_address
                else:
                    location = "Место не указано"

                formatted_text += f"📍 {location}\n"

                # Комментарий
                comment = lesson.get('lessonCommentary', '')
                if comment:
                    formatted_text += f"💬 {comment}\n"

                formatted_text += "\n"

        # Добавляем информацию о неделе
        current_week = "🔴 Красная" if datetime.now().isocalendar()[1] % 2 == 1 else "🔵 Синяя"
        formatted_text += f"\n<i>Текущая неделя: {current_week}</i>"

        return formatted_text

    async def is_admin(self, user_id: int) -> bool:
        """Проверяет, является ли пользователь администратором"""
        return user_id in self.admin_ids

    async def send_to_user(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Отправляет сообщение конкретному пользователю"""
        user = update.effective_user
        
        if not await self.is_admin(user.id):
            await update.message.reply_text("У вас нет прав для выполнения этой команды.")
            return

        try:
            # Проверяем аргументы команды
            args = context.args
            if len(args) < 2:
                await update.message.reply_text(
                    "Использование: /send <id/username> <сообщение>\n"
                    "Например: /send 123456789 Привет!"
                )
                return

            target = args[0]
            message = ' '.join(args[1:])

            # Получаем информацию о целевом пользователе из базы данных
            target_user = None
            if target.isdigit():
                # Если указан ID
                target_user = self.db.get_user_by_tg_id(int(target))
            else:
                # Если указан username
                target_user = self.db.get_user_by_username(target.lstrip('@'))

            if not target_user:
                await update.message.reply_text("Пользователь не найден.")
                return

            # Отправляем сообщение
            try:
                await context.bot.send_message(
                    chat_id=target_user['tg_id'],
                    text=f"📩 Сообщение от администратора:\n\n{message}"
                )
                await update.message.reply_text(f"✅ Сообщение успешно отправлено пользователю {target}")
            except Exception as e:
                logging.error(f"Error sending message to user {target}: {e}")
                await update.message.reply_text("❌ Не удалось отправить сообщение.")

        except Exception as e:
            logging.error(f"Error in send_to_user: {e}")
            await update.message.reply_text("Произошла ошибка при отправке сообщения.")

    async def broadcast(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Отправляет сообщение всем пользователям бота"""
        user = update.effective_user
        
        if not await self.is_admin(user.id):
            await update.message.reply_text("У вас нет прав для выполнения этой команды.")
            return

        try:
            # Проверяем наличие сообщения
            if not context.args:
                await update.message.reply_text(
                    "Использование: /broadcast <сообщение>\n"
                    "Например: /broadcast Важное объявление!"
                )
                return

            message = ' '.join(context.args)
            users = self.db.get_all_users()
            
            if not users:
                await update.message.reply_text("Нет пользователей для рассылки.")
                return

            # Отправляем сообщение всем пользователям
            success = 0
            failed = 0
            status_message = await update.message.reply_text("Начинаю рассылку...")

            for user in users:
                try:
                    await context.bot.send_message(
                        chat_id=user['tg_id'],
                        text=f"📢 Объявление:\n\n{message}"
                    )
                    success += 1
                    if (success + failed) % 10 == 0:  # Обновляем статус каждые 10 сообщений
                        await status_message.edit_text(
                            f"Отправлено: {success}\n"
                            f"Ошибок: {failed}\n"
                            f"Всего: {len(users)}"
                        )
                except Exception as e:
                    logging.error(f"Error broadcasting to user {user['tg_id']}: {e}")
                    failed += 1

            # Отправляем итоговый отчет
            await status_message.edit_text(
                f"✅ Рассылка завершена\n\n"
                f"Успешно: {success}\n"
                f"Ошибок: {failed}\n"
                f"Всего пользователей: {len(users)}"
            )

        except Exception as e:
            logging.error(f"Error in broadcast: {e}")
            await update.message.reply_text("Произошла ошибка при выполнении рассылки.")

    async def delete_group_handler(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        query = update.callback_query
        user = query.from_user
        group_id = query.data.split('_')[2]

        try:
            if self.db.delete_saved_group(user.id, group_id):
                await query.answer("✅ Группа удалена из сохраненных")
                # Обновляем сообщение с новой клавиатурой
                await self.start(update, context)
            else:
                await query.answer("❌ Не удалось удалить группу")
        except Exception as e:
            logging.error(f"Error deleting group: {e}")
            await query.answer("Произошла ошибка при удалении группы")

    @lru_cache(maxsize=100)
    def get_cached_schedule(self, group_id, date):
        # Проверяем актуальность кэша
        cache_key = f"{group_id}_{date}"
        if (cache_key in self.cache_last_update and 
            datetime.now() - self.cache_last_update[cache_key] < self.cache_timeout):
            return self.schedule_cache.get(cache_key)
        
        # Получаем новые данные
        schedule = self.api.get_schedule(group_id, date)
        if schedule:
            self.cache_last_update[cache_key] = datetime.now()
        return schedule

    def clear_expired_cache(self):
        """Очистка устаревшего кэша"""
        current_time = datetime.now()
        expired_keys = [
            key for key, update_time in self.cache_last_update.items()
            if current_time - update_time > self.cache_timeout
        ]
        for key in expired_keys:
            self.cache_last_update.pop(key, None)
            self.get_cached_schedule.cache_clear()

    async def send_message(self, chat_id, message):
        """Добавление сообщения в очередь"""
        await self.message_queue.put((message, chat_id))

    async def schedule_notification_handler(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Настройка уведомлений о расписании"""
        query = update.callback_query
        user_id = query.from_user.id
        
        message = (
            "⏰ <b>Настройка уведомлений</b>\n\n"
            "Выберите время, когда вы хотите получать расписание:"
        )
        
        keyboard = [
            [InlineKeyboardButton("🌅 Утром (8:00)", callback_data='notify_morning')],
            [InlineKeyboardButton("🌆 Вечером (20:00)", callback_data='notify_evening')],
            [InlineKeyboardButton("❌ Отключить уведомления", callback_data='notify_disable')],
            [InlineKeyboardButton("🔙 Назад", callback_data='start')]
        ]
        
        reply_markup = InlineKeyboardMarkup(keyboard)
        await query.message.edit_text(message, reply_markup=reply_markup, parse_mode='HTML')

    async def export_schedule(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Экспорт расписания в календарь"""
        query = update.callback_query
        user_id = query.from_user.id
        
        message = (
            "📅 <b>Экспорт расписания</b>\n\n"
            "Выберите формат экспорта:"
        )
        
        keyboard = [
            [InlineKeyboardButton("📱 Google Calendar", callback_data='export_google')],
            [InlineKeyboardButton("📱 Apple Calendar", callback_data='export_apple')],
            [InlineKeyboardButton("📄 iCal файл", callback_data='export_ical')],
            [InlineKeyboardButton("🔙 Назад", callback_data='start')]
        ]
        
        reply_markup = InlineKeyboardMarkup(keyboard)
        await query.message.edit_text(message, reply_markup=reply_markup, parse_mode='HTML')

    async def notification_settings(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Настройка уведомлений"""
        query = update.callback_query
        user_id = query.from_user.id
        
        # Получаем сохраненные группы пользователя
        saved_groups = self.db.get_saved_groups(user_id)
        
        if not saved_groups:
            await query.message.edit_text(
                "❌ У вас нет сохраненных групп. Сначала добавьте группу в избранное.",
                reply_markup=InlineKeyboardMarkup([[
                    InlineKeyboardButton("🔙 Вернуться в главное меню", callback_data='start')
                ]])
            )
            return
        
        message = (
            "⏰ <b>Настройка уведомлений о парах</b>\n\n"
            "Выберите группу для настройки уведомлений:"
        )
        
        keyboard = []
        for group in saved_groups:
            keyboard.append([
                InlineKeyboardButton(
                    f"📋 {group['groupCode']}", 
                    callback_data=f"notify_setup_{group['groupId']}"
                )
            ])
        
        keyboard.append([InlineKeyboardButton("🔙 Вернуться в главное меню", callback_data='start')])
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await query.message.edit_text(message, reply_markup=reply_markup, parse_mode='HTML')

    async def setup_notification_time(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Настройка времени уведомлений"""
        query = update.callback_query
        group_id = query.data.split('_')[2]
        
        try:
            # Получаем текущие настройки уведомлений
            notifications = self.db.get_user_notifications(query.from_user.id)
            current_settings = ""
            if notifications:
                current_settings = "\n\nТекущие настройки:\n"
                for notif in notifications:
                    time_str = notif['notification_time'].strftime('%H:%M') if isinstance(notif['notification_time'], datetime) else str(notif['notification_time'])
                    current_settings += f"• Группа {notif['groupCode']}: {time_str}\n"
            
            message = (
                "⏰ <b>Выберите время уведомлений</b>\n\n"
                "В выбранное время вы будете получать расписание на следующий день"
                f"{current_settings}"
            )
            
            keyboard = [
                [
                    InlineKeyboardButton("08:00", callback_data=f"set_notify_{group_id}_08:00"),
                    InlineKeyboardButton("12:00", callback_data=f"set_notify_{group_id}_12:00")
                ],
                [
                    InlineKeyboardButton("16:00", callback_data=f"set_notify_{group_id}_16:00"),
                    InlineKeyboardButton("20:00", callback_data=f"set_notify_{group_id}_20:00")
                ],
                [InlineKeyboardButton("22:00", callback_data=f"set_notify_{group_id}_22:00")],
                [InlineKeyboardButton("❌ Отключить уведомления", callback_data=f"notify_disable_{group_id}")],
                [InlineKeyboardButton("🔙 Назад", callback_data='notifications')]
            ]
            
            reply_markup = InlineKeyboardMarkup(keyboard)
            await query.message.edit_text(message, reply_markup=reply_markup, parse_mode='HTML')
                
        except Exception as e:
            logging.error(f"Error in setup_notification_time: {e}")
            await query.message.edit_text(
                "❌ Произошла ошибка при настройке уведомлений",
                reply_markup=InlineKeyboardMarkup([[
                    InlineKeyboardButton("🔙 Назад", callback_data='notifications')
                ]])
            )

    async def check_notifications(self, context: ContextTypes.DEFAULT_TYPE):
        """Проверка и отправка уведомлений"""
        try:
            # Получаем текущее время в формате HH:MM
            current_time = datetime.now().strftime('%H:%M')
            logging.info(f"Checking notifications for time: {current_time}")
            
            # Получаем пользователей для текущего времени
            users = self.db.get_users_for_notification(current_time)
            logging.info(f"Found {len(users)} users for notifications")
            
            for user in users:
                try:
                    # Получаем расписание на следующий день
                    tomorrow = (datetime.now() + timedelta(days=1)).strftime('%Y%m%d')
                    schedule = await self.api.get_schedule(str(user['groupId']), date=tomorrow)
                    
                    if schedule and 'schedule' in schedule:
                        # Форматируем сообщение
                        message = self.format_schedule(schedule)
                        message = f"📅 Расписание на завтра:\n\n{message}"
                    else:
                        message = f"📅 На завтра ({tomorrow}) занятий нет"
                    
                    # Отправляем уведомление
                    await context.bot.send_message(
                        chat_id=user['tg_id'],
                        text=message,
                        parse_mode='HTML'
                    )
                    logging.info(f"Notification sent to user {user['tg_id']}")
                    
                except Exception as e:
                    logging.error(f"Error sending notification to user {user['tg_id']}: {e}")
                    continue
                    
        except Exception as e:
            logging.error(f"Error in check_notifications: {e}")

    async def cleanup_task(self, context: ContextTypes.DEFAULT_TYPE):
        """Периодическая очистка устаревших данных"""
        try:
            # Очищаем кэш
            self.clear_expired_cache()
            # Очищаем старые данные из БД
            self.db.cleanup_old_data()
            logging.info("Cleanup task completed successfully")
        except Exception as e:
            logging.error(f"Error in cleanup task: {e}")

    async def admin_system_handler(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Системная информация"""
        query = update.callback_query
        if query.from_user.id not in ADMIN_IDS:
            await query.answer("⛔️ Нет доступа")
            return

        # Получаем системную информацию
        memory_usage = psutil.Process().memory_info().rss / 1024 / 1024  # MB
        cpu_percent = psutil.Process().cpu_percent()
        uptime = datetime.now() - self.start_time
        active_users = len(self.db.get_active_users_last_24h())
        cached_items = len(self.schedule_cache)

        message = (
            "🖥 <b>Системная информация</b>\n\n"
            f"💾 Использование памяти: {memory_usage:.1f} MB\n"
            f"⚡️ Загрузка CPU: {cpu_percent}%\n"
            f"⏱ Время работы: {str(uptime).split('.')[0]}\n"
            f"👥 Активных пользователей (24ч): {active_users}\n"
            f"📦 Кэшированных элементов: {cached_items}\n\n"
            "<b>Действия:</b>"
        )

        keyboard = [
            [InlineKeyboardButton("🔄 Очистить кэш", callback_data='admin_clear_cache')],
            [InlineKeyboardButton("📊 Проверить БД", callback_data='admin_check_db')],
            [InlineKeyboardButton("🔙 Назад", callback_data='admin')]
        ]
        
        reply_markup = InlineKeyboardMarkup(keyboard)
        await query.message.edit_text(message, reply_markup=reply_markup, parse_mode='HTML')

    async def admin_graphs_handler(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Графики статистики"""
        query = update.callback_query
        if query.from_user.id not in ADMIN_IDS:
            await query.answer("⛔️ Нет доступа")
            return

        # Генерируем графики
        stats = self.db.get_usage_stats_last_week()
        
        # Создаем график с помощью matplotlib
        plt.figure(figsize=(10, 6))
        plt.plot(stats['dates'], stats['users'], label='Пользователи')
        plt.plot(stats['dates'], stats['queries'], label='Запросы')
        plt.title('Статистика использования бота')
        plt.xlabel('Дата')
        plt.ylabel('Количество')
        plt.legend()
        
        # Сохраняем график во временный файл
        buf = io.BytesIO()
        plt.savefig(buf, format='png')
        buf.seek(0)
        
        # Отправляем график
        await query.message.reply_photo(
            photo=buf,
            caption="📈 График использования бота за последнюю неделю"
        )
        
        keyboard = [[InlineKeyboardButton("🔙 Назад", callback_data='admin')]]
        await query.message.edit_reply_markup(reply_markup=InlineKeyboardMarkup(keyboard))

    async def admin_users_banned_handler(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Список забаненных пользователей"""
        query = update.callback_query
        if query.from_user.id not in ADMIN_IDS:
            await query.answer("⛔️ Нет доступа")
            return
        
        banned_users = self.db.get_banned_users()
        
        message = "🚫 <b>Заблокированные пользователи</b>\n\n"
        if banned_users:
            for user in banned_users:
                ban_date = user['banned_at'].strftime('%d.%m.%Y %H:%M') if user['banned_at'] else 'неизвестно'
                last_activity = user['last_activity'].strftime('%d.%m.%Y %H:%M') if user['last_activity'] else 'никогда'
                message += (
                    f"• ID: {user['tg_id']}\n"
                    f"  Username: @{user['username'] or 'нет'}\n"
                    f"  Дата блокировки: {ban_date}\n"
                    f"  Последняя активность: {last_activity}\n\n"
                )
                
            # Добавляем кнопки действий
            keyboard = [
                [InlineKeyboardButton("🔄 Разблокировать всех", callback_data='admin_unban_all')],
                [InlineKeyboardButton("🔙 Назад", callback_data='admin_users')]
            ]
        else:
            message += "Нет заблокированных пользователей"
        keyboard = [[InlineKeyboardButton("🔙 Назад", callback_data='admin_users')]]
        
        reply_markup = InlineKeyboardMarkup(keyboard)
        await query.message.edit_text(message, reply_markup=reply_markup, parse_mode='HTML')

    async def admin_users_mass_handler(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Массовые действия с пользователями"""
        query = update.callback_query
        if query.from_user.id not in ADMIN_IDS:
            await query.answer("⛔️ Нет доступа")
            return
        
        message = (
            "📝 <b>Массовые действия</b>\n\n"
            "Выберите действие:"
        )
        
        keyboard = [
            [InlineKeyboardButton("📨 Рассылка всем", callback_data='admin_broadcast')],
            [InlineKeyboardButton("🔄 Сброс настроек", callback_data='admin_mass_reset')],
            [InlineKeyboardButton("❌ Удалить неактивных", callback_data='admin_mass_delete_inactive')],
            [InlineKeyboardButton("🔙 Назад", callback_data='admin_users')]
        ]
        
        reply_markup = InlineKeyboardMarkup(keyboard)
        await query.message.edit_text(message, reply_markup=reply_markup, parse_mode='HTML')

    async def admin_broadcast_handler(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Рассылка сообщений"""
        query = update.callback_query
        if query.from_user.id not in ADMIN_IDS:
            await query.answer("⛔️ Нет доступа")
            return
        
        try:
            message = (
                "📨 <b>Рассылка сообщений</b>\n\n"
                "Отправьте текст сообщения для рассылки.\n"
                "Поддерживается HTML-разметка.\n\n"
                "<i>Примеры использования HTML:</i>\n"
                "<code>&lt;b&gt;жирный текст&lt;/b&gt;</code>\n"
                "<code>&lt;i&gt;курсив&lt;/i&gt;</code>\n"
                "<code>&lt;code&gt;моноширинный&lt;/code&gt;</code>\n"
                "<code>&lt;a href='URL'&gt;ссылка&lt;/a&gt;</code>\n\n"
                "Для отмены нажмите кнопку «Отмена»"
            )
            
            keyboard = [[InlineKeyboardButton("🔙 Отмена", callback_data='admin')]]
            reply_markup = InlineKeyboardMarkup(keyboard)
            
            await query.message.edit_text(message, reply_markup=reply_markup, parse_mode='HTML')
            context.user_data['state'] = 'waiting_for_broadcast'
        except Exception as e:
            logging.error(f"Error in admin_broadcast_handler: {e}")
            await query.answer("❌ Произошла ошибка")

    async def get_all_users(self):
        """Получение всех пользователей"""
        with self.get_connection() as conn:
            cursor = conn.cursor(dictionary=True)
            try:
                cursor.execute("""
                    SELECT u.*, 
                           MAX(ul.login_time) as last_login,
                           COUNT(DISTINCT usg.group_id) as saved_groups_count,
                           COUNT(DISTINCT usl.lectureId) as saved_lecturers_count
                    FROM users u
                    LEFT JOIN user_logins ul ON u.id = ul.user_id
                    LEFT JOIN user_saved_groups usg ON u.id = usg.user_id
                    LEFT JOIN user_saved_lecturers usl ON u.id = usl.user_id
                    GROUP BY u.id
                """)
                return cursor.fetchall()
            except mysql.connector.Error as err:
                logging.error(f"Database error in get_all_users: {err}")
                return []

    async def admin_check_db_handler(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Проверка базы данных"""
        query = update.callback_query
        if query.from_user.id not in ADMIN_IDS:
            await query.answer("⛔️ Нет доступа")
            return

        message = "🔍 <b>Проверка базы данных</b>\n\n"
        try:
            # Проверяем соединение
            self.db.test_connection()
            message += "✅ Соединение с БД: успешно\n"
            
            # Проверяем основные таблицы
            tables = {
                'users': 'Пользователи',
                '`groups`': 'Группы',  # Добавляем кавычки для зарезервированных слов
                'lecturers': 'Преподаватели',
                'user_saved_groups': 'Сохраненные группы',
                'user_notifications': 'Уведомления',
                'group_techcards': 'Техкарты'
            }
            
            for table, name in tables.items():
                count = self.db.get_table_count(table)
                message += f"📊 {name}: {count} записей\n"
            
        except Exception as e:
            message += f"❌ Ошибка: {str(e)}"
        
        keyboard = [[InlineKeyboardButton("🔙 Назад", callback_data='admin_system')]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        await query.message.edit_text(message, reply_markup=reply_markup, parse_mode='HTML')

    async def admin_clear_cache_handler(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Очистка кэша"""
        query = update.callback_query
        if query.from_user.id not in ADMIN_IDS:
            await query.answer("⛔️ Нет доступа")
            return
        
        try:
            # Очищаем кэш расписания
            self.schedule_cache.clear()
            self.cache_last_update.clear()
            
            # Очищаем Redis кэш если он инициализирован
            if hasattr(self, 'redis_cache') and self.redis_cache:
                try:
                    self.redis_cache.redis.flushdb()
                except Exception as e:
                    logging.warning(f"Error clearing Redis cache: {e}")
            
            await query.answer("✅ Кэш успешно очищен")
            await self.admin_system_handler(update, context)
        except Exception as e:
            logging.error(f"Error clearing cache: {e}")
            await query.answer("❌ Ошибка при очистке кэша")

    async def admin_techcard_stats_handler(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Статистика техкарт"""
        query = update.callback_query
        if query.from_user.id not in ADMIN_IDS:
            await query.answer("⛔️ Нет доступа")
            return
        
        stats = self.db.get_techcards_stats()
        
        # Получаем дополнительную статистику
        detailed_stats = self.db.get_detailed_techcard_stats()
        
        message = (
            "📊 <b>Статистика технологических карт</b>\n\n"
            f"📚 Всего техкарт: {stats['total']}\n"
            f"➕ Добавлено за неделю: {stats['added_week']}\n"
            f"🔄 Обновлено за неделю: {stats['updated_week']}\n\n"
            "<b>По факультетам:</b>\n"
        )
        
        for faculty in detailed_stats['by_faculty']:
            message += f"• {faculty['name']}: {faculty['count']}\n"
        
        keyboard = [[InlineKeyboardButton("🔙 Назад", callback_data='admin_techcards')]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        await query.message.edit_text(message, reply_markup=reply_markup, parse_mode='HTML')

    async def admin_techcard_search_handler(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Поиск техкарт"""
        query = update.callback_query
        if query.from_user.id not in ADMIN_IDS:
            await query.answer("⛔️ Нет доступа")
            return
        
        message = (
            "🔍 <b>Поиск технологических карт</b>\n\n"
            "Отправьте код группы или часть названия для поиска\n"
            "Например: К402 или ПИ-01\n\n"
            "Для отмены нажмите кнопку «Назад»"
        )
        
        keyboard = [[InlineKeyboardButton("🔙 Назад", callback_data='admin_techcards')]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await query.message.edit_text(message, reply_markup=reply_markup, parse_mode='HTML')
        context.user_data['state'] = 'waiting_for_techcard_search'

    async def process_broadcast(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработка рассылки сообщений"""
        if not update.message or not update.message.text:
            return
        
        message_text = update.message.text
        
        # Получаем всех активных пользователей
        users = [u for u in self.db.get_all_users() if not u.get('is_banned')]
        total_users = len(users)
        success_count = 0
        fail_count = 0
        
        # Отправляем статус начала рассылки
        status_message = await update.message.reply_text(
            "📨 Начинаю рассылку...\n"
            f"Всего получателей: {total_users}"
        )
        
        # Выполняем рассылку
        for user in users:
            try:
                await context.bot.send_message(
                    chat_id=user['tg_id'],
                    text=message_text,
                    parse_mode='HTML'
                )
                success_count += 1
                
                # Обновляем статус каждые 10 отправленных сообщений
                if success_count % 10 == 0:
                    await status_message.edit_text(
                        f"📨 Отправлено: {success_count}/{total_users}\n"
                        f"❌ Ошибок: {fail_count}"
                    )
                
                # Небольшая задержка чтобы не превысить лимиты Telegram
                await asyncio.sleep(0.05)
                
            except Exception as e:
                fail_count += 1
                logging.error(f"Error sending broadcast to user {user['tg_id']}: {e}")
        
        # Отправляем итоговый отчет
        final_message = (
            "📨 <b>Рассылка завершена</b>\n\n"
            f"✅ Успешно отправлено: {success_count}\n"
            f"❌ Ошибок: {fail_count}\n"
            f"📊 Всего получателей: {total_users}"
        )
        
        keyboard = [[InlineKeyboardButton("🔙 В админ-панель", callback_data='admin')]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await status_message.edit_text(
            final_message,
            reply_markup=reply_markup,
            parse_mode='HTML'
        )
        
        # Сбрасываем состояние
        context.user_data['state'] = None

    async def help_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Показать справку по командам"""
        user_id = update.effective_user.id
        is_admin = user_id in ADMIN_IDS
        
        help_text = (
            "🤖 <b>Доступные команды:</b>\n\n"
            "/start - Запустить бота\n"
            "/week - Показать текущую неделю\n"
            "/help - Показать эту справку\n"
        )
        
        if is_admin:
            admin_help = (
                "\n<b>Команды администратора:</b>\n"
                "/send [ID/username] [текст] - Отправить сообщение пользователю\n"
                "/broadcast [текст] - Отправить сообщение всем пользователям\n"
                "/ban [ID/username] - Заблокировать пользователя\n"
                "/unban [ID/username] - Разблокировать пользователя\n\n"
                "<b>Также доступны команды:</b>\n"
                "инфо @username - Информация о пользователе\n"
                "бан @username - Заблокировать пользователя\n"
                "разбан @username - Разблокировать пользователя\n"
                "удалить_группу ID ГРУППА - Удалить группу у пользователя\n"
                "удалить_препод ID ИМЯ - Удалить преподавателя у пользователя"
            )
            help_text += admin_help
        
        await update.message.reply_text(help_text, parse_mode='HTML')

    async def send_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Отправка сообщения конкретному пользователю"""
        if update.effective_user.id not in ADMIN_IDS:
            await update.message.reply_text("⛔️ У вас нет доступа к этой команде")
            return
        
        if not context.args or len(context.args) < 2:
            await update.message.reply_text(
                "❌ Неверный формат команды\n"
                "Использование: /send [ID/username] [текст]"
            )
            return
        
        target = context.args[0]
        message_text = ' '.join(context.args[1:])
        
        try:
            # Определяем ID пользователя
            if target.startswith('@'):
                user = self.db.get_user_by_username(target[1:])
            else:
                user = self.db.get_user_by_tg_id(int(target))
            
            if not user:
                await update.message.reply_text("❌ Пользователь не найден")
                return
            
            # Отправляем сообщение
            await context.bot.send_message(
                chat_id=user['tg_id'],
                text=message_text,
                parse_mode='HTML'
            )
            await update.message.reply_text("✅ Сообщение отправлено")
            
        except Exception as e:
            logging.error(f"Error in send_command: {e}")
            await update.message.reply_text("❌ Ошибка при отправке сообщения")

    async def broadcast_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Отправка сообщения всем пользователям"""
        if update.effective_user.id not in ADMIN_IDS:
            await update.message.reply_text("⛔️ У вас нет доступа к этой команде")
            return
        
        if not context.args:
            await update.message.reply_text(
                "❌ Неверный формат команды\n"
                "Использование: /broadcast [текст]"
            )
            return
        
        message_text = ' '.join(context.args)
        
        # Запускаем рассылку
        status_message = await update.message.reply_text("📨 Начинаю рассылку...")
        context.user_data['broadcast_message'] = message_text
        context.user_data['status_message'] = status_message
        await self.process_broadcast(update, context)

    async def ban_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Блокировка пользователя"""
        if update.effective_user.id not in ADMIN_IDS:
            await update.message.reply_text("⛔️ У вас нет доступа к этой команде")
            return
        
        if not context.args:
            await update.message.reply_text(
                "❌ Неверный формат команды\n"
                "Использование: /ban [ID/username]"
            )
            return
        
        target = context.args[0]
        try:
            if target.startswith('@'):
                user = self.db.get_user_by_username(target[1:])
            else:
                user = self.db.get_user_by_tg_id(int(target))
            
            if not user:
                await update.message.reply_text("❌ Пользователь не найден")
                return
            
            if self.db.ban_user(user['tg_id']):
                await update.message.reply_text(f"✅ Пользователь {target} заблокирован")
            else:
                await update.message.reply_text("❌ Ошибка при блокировке пользователя")
                
        except Exception as e:
            logging.error(f"Error in ban_command: {e}")
            await update.message.reply_text("❌ Ошибка при блокировке пользователя")

    async def unban_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Разблокировка пользователя"""
        if update.effective_user.id not in ADMIN_IDS:
            await update.message.reply_text("⛔️ У вас нет доступа к этой команде")
            return
        
        if not context.args:
            await update.message.reply_text(
                "❌ Неверный формат команды\n"
                "Использование: /unban [ID/username]"
            )
            return
        
        target = context.args[0]
        try:
            if target.startswith('@'):
                user = self.db.get_user_by_username(target[1:])
            else:
                user = self.db.get_user_by_tg_id(int(target))
            
            if not user:
                await update.message.reply_text("❌ Пользователь не найден")
                return
            
            if self.db.unban_user(user['tg_id']):
                await update.message.reply_text(f"✅ Пользователь {target} разблокирован")
            else:
                await update.message.reply_text("❌ Ошибка при разблокировке пользователя")
                
        except Exception as e:
            logging.error(f"Error in unban_command: {e}")
            await update.message.reply_text("❌ Ошибка при разблокировке пользователя")

    def setup_notification_jobs(self):
        """Настройка заданий для уведомлений"""
        try:
            # Удаляем все существующие задания
            if hasattr(self.application.job_queue, 'jobs'):
                for job in self.application.job_queue.jobs():
                    job.schedule_removal()
            
            # Задаем времена для уведомлений
            notification_times = ['08:00', '12:00', '16:00', '20:00', '22:00']
            
            for time_str in notification_times:
                hour, minute = map(int, time_str.split(':'))
                # Создаем время для задания
                target_time = time(hour=hour, minute=minute)
                
                # Регистрируем задание
                self.application.job_queue.run_daily(
                    self.check_notifications,
                    time=target_time,
                    name=f"notify_{time_str}"
                )
                logging.info(f"Scheduled notification job for {time_str}")
                
        except Exception as e:
            logging.error(f"Error setting up notification jobs: {e}")

if __name__ == '__main__':
    # Настраиваем обработку сигналов для корректного завершения
    async def shutdown_bot(bot, loop):
        """Корректное завершение работы бота"""
        try:
            logging.info("Starting shutdown process from signal handler...")
            await bot.shutdown()
            
            # Отменяем все оставшиеся задачи
            tasks = [t for t in asyncio.all_tasks(loop) if t is not asyncio.current_task()]
            if tasks:
                logging.info(f"Cancelling {len(tasks)} pending tasks...")
                for task in tasks:
                    task.cancel()
                await asyncio.gather(*tasks, return_exceptions=True)
            
            # Закрываем асинхронные генераторы
            await loop.shutdown_asyncgens()
            logging.info("Shutdown from signal handler completed")
        except Exception as e:
            logging.error(f"Error during shutdown from signal handler: {e}")

    def signal_handler(sig, frame):
        print('Получен сигнал завершения, закрываем бот...')
        try:
            loop = asyncio.get_event_loop()
            if loop.is_running():
                loop.create_task(shutdown_bot(bot, loop))
                loop.call_soon_threadsafe(loop.stop)
        except Exception as e:
            print(f"Ошибка при остановке loop: {e}")
    
    # Регистрируем обработчики сигналов
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    # Запускаем бота
    bot = TelegramBot()
    
    # Запускаем в asyncio.run()
    try:
        asyncio.run(bot.run())
    except KeyboardInterrupt:
        print("Получен сигнал прерывания...")
    except Exception as e:
        print(f"Ошибка при запуске бота: {e}")
