#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Генератор игровых событий для системы сбора статистики
Симулирует события онлайн-игры и сохраняет их в PostgreSQL
"""

import psycopg2
import time
import random
import os
from datetime import datetime
from faker import Faker

# Инициализация генератора случайных данных
fake = Faker()

# ========================================
# КОНФИГУРАЦИЯ
# ========================================

# Параметры подключения к БД из переменных окружения
DB_CONFIG = {
    'host': os.getenv('DB_HOST', 'localhost'),
    'port': os.getenv('DB_PORT', '5432'),
    'database': os.getenv('DB_NAME', 'game_stats'),
    'user': os.getenv('DB_USER', 'gameuser'),
    'password': os.getenv('DB_PASSWORD', 'gamepass123')
}

# Интервал генерации событий (в секундах)
EVENT_INTERVAL = 1

# ========================================
# ИГРОВЫЕ ДАННЫЕ
# ========================================

# Список оружия (как в CS:GO/Valorant)
WEAPONS = [
    'AK-47', 'M4A4', 'M4A1-S', 'AWP', 'Desert Eagle',
    'P90', 'UMP-45', 'MP7', 'Glock-18', 'USP-S',
    'FAMAS', 'Galil AR', 'SSG 08', 'Knife', 'Grenade'
]

# Игровые карты
MAPS = [
    'de_dust2',      # Классика
    'de_inferno',    # Огненная
    'de_mirage',     # Мираж
    'de_nuke',       # Ядерная
    'de_train',      # Поезда
    'de_overpass',   # Эстакада
    'de_vertigo',    # Небоскрёб
    'de_ancient',    # Древние руины
]

# Типы действий с весами вероятности и диапазоном очков
ACTION_TYPES = {
    'kill': {
        'weight': 35,
        'points_range': (10, 30),
        'description': 'Убийство противника'
    },
    'death': {
        'weight': 35,
        'points_range': (-25, -10),
        'description': 'Смерть игрока'
    },
    'headshot': {
        'weight': 10,
        'points_range': (40, 60),
        'description': 'Убийство в голову'
    },
    'assist': {
        'weight': 8,
        'points_range': (5, 15),
        'description': 'Помощь в убийстве'
    },
    'achievement': {
        'weight': 7,
        'points_range': (50, 150),
        'description': 'Достижение разблокировано'
    },
    'level_up': {
        'weight': 3,
        'points_range': (100, 250),
        'description': 'Повышение уровня'
    },
    'defuse_bomb': {
        'weight': 2,
        'points_range': (80, 120),
        'description': 'Бомба обезврежена'
    }
}

# Пул активных игроков
PLAYER_POOL = []

# ========================================
# ФУНКЦИИ
# ========================================

def print_header():
    """Вывод красивого заголовка"""
    print("\n" + "=" * 70)
    print("🎮  ГЕНЕРАТОР ИГРОВОЙ СТАТИСТИКИ  🎮")
    print("=" * 70)
    print(f"📅 Время запуска: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"🔧 БД: {DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}")
    print("=" * 70 + "\n")

def init_player_pool(size=50):
    """
    Инициализация пула игроков
    
    Args:
        size: количество игроков в пуле
    """
    global PLAYER_POOL
    
    print(f"👥 Создание пула игроков ({size} игроков)...")
    
    for i in range(size):
        player = {
            'player_id': f'PLAYER_{i:04d}',
            'player_name': fake.user_name(),
            'level': random.randint(1, 50),
            'total_points': 0,
            'kills': 0,
            'deaths': 0
        }
        PLAYER_POOL.append(player)
    
    print(f"✓ Пул игроков создан\n")

def connect_to_db(max_retries=10, retry_delay=5):
    """
    Подключение к PostgreSQL с повторными попытками
    
    Args:
        max_retries: максимальное количество попыток
        retry_delay: задержка между попытками (секунды)
    
    Returns:
        connection: объект подключения к БД
    """
    print("🔌 Подключение к базе данных...")
    
    for attempt in range(1, max_retries + 1):
        try:
            conn = psycopg2.connect(**DB_CONFIG)
            print(f"✓ Подключение установлено (попытка {attempt}/{max_retries})\n")
            return conn
        except psycopg2.OperationalError as e:
            if attempt < max_retries:
                print(f"⚠ Попытка {attempt}/{max_retries} не удалась")
                print(f"  Повторная попытка через {retry_delay} секунд...")
                time.sleep(retry_delay)
            else:
                print(f"\n✗ ОШИБКА: Не удалось подключиться после {max_retries} попыток")
                print(f"  Детали: {e}")
                raise

def generate_event():
    """
    Генерация одного игрового события
    
    Returns:
        dict: словарь с данными события
    """
    # Выбираем случайного игрока
    player = random.choice(PLAYER_POOL)
    
    # Выбираем тип действия (с учётом весов)
    action_types = list(ACTION_TYPES.keys())
    weights = [ACTION_TYPES[a]['weight'] for a in action_types]
    action_type = random.choices(action_types, weights=weights)[0]
    
    # Генерируем очки
    points_min, points_max = ACTION_TYPES[action_type]['points_range']
    points = random.randint(points_min, points_max)
    
    # Обновляем статистику игрока
    player['total_points'] += points
    
    if action_type == 'kill' or action_type == 'headshot':
        player['kills'] += 1
    elif action_type == 'death':
        player['deaths'] += 1
    
    # Повышение уровня (5% вероятность или при level_up событии)
    if action_type == 'level_up' or random.random() < 0.05:
        if player['level'] < 100:
            player['level'] += 1
    
    # Оружие (только для боевых действий)
    weapon = None
    if action_type in ['kill', 'headshot', 'death']:
        weapon = random.choice(WEAPONS)
    
    # Карта
    map_name = random.choice(MAPS)
    
    # Формируем событие
    event = {
        'player_id': player['player_id'],
        'player_name': player['player_name'],
        'action_type': action_type,
        'points': points,
        'level': player['level'],
        'weapon': weapon,
        'map_name': map_name
    }
    
    return event

def insert_event(conn, event):
    """
    Вставка события в базу данных
    
    Args:
        conn: подключение к БД
        event: словарь с данными события
    
    Returns:
        bool: успешность операции
    """
    query = """
        INSERT INTO game_events 
        (player_id, player_name, action_type, points, level, weapon, map_name)
        VALUES (%(player_id)s, %(player_name)s, %(action_type)s, 
                %(points)s, %(level)s, %(weapon)s, %(map_name)s)
    """
    
    try:
        with conn.cursor() as cursor:
            cursor.execute(query, event)
        conn.commit()
        return True
    except Exception as e:
        print(f"\n✗ Ошибка вставки: {e}")
        conn.rollback()
        return False

def format_event_log(event, count):
    """
    Форматирование события для красивого вывода
    
    Args:
        event: словарь события
        count: номер события
    
    Returns:
        str: форматированная строка
    """
    action_emoji = {
        'kill': '💀',
        'death': '☠️',
        'headshot': '🎯',
        'assist': '🤝',
        'achievement': '🏆',
        'level_up': '⬆️',
        'defuse_bomb': '💣'
    }
    
    emoji = action_emoji.get(event['action_type'], '📊')
    weapon_str = f" ({event['weapon']})" if event['weapon'] else ""
    
    return (f"#{count:05d} | {emoji} {event['action_type']:12} | "
            f"{event['player_name']:15} | Lvl {event['level']:2} | "
            f"{event['points']:+4} pts{weapon_str}")

def print_statistics(event_count, start_time):
    """Вывод статистики работы генератора"""
    elapsed = time.time() - start_time
    rate = event_count / elapsed if elapsed > 0 else 0
    
    # Топ-3 игрока по очкам
    top_players = sorted(PLAYER_POOL, key=lambda p: p['total_points'], reverse=True)[:3]
    
    print("\n" + "─" * 70)
    print("📊 СТАТИСТИКА")
    print("─" * 70)
    print(f"Всего событий:     {event_count}")
    print(f"Время работы:      {elapsed:.1f} сек")
    print(f"Скорость:          {rate:.2f} событий/сек")
    print("\n🏆 Топ-3 игрока:")
    for i, p in enumerate(top_players, 1):
        kd_ratio = p['kills'] / p['deaths'] if p['deaths'] > 0 else p['kills']
        print(f"  {i}. {p['player_name']:15} | Lvl {p['level']:2} | "
              f"{p['total_points']:6} pts | K/D: {kd_ratio:.2f}")
    print("─" * 70 + "\n")

# ========================================
# ОСНОВНАЯ ПРОГРАММА
# ========================================

def main():
    """Главная функция генератора"""
    
    # Вывод заголовка
    print_header()
    
    # Инициализация
    init_player_pool(50)
    conn = connect_to_db()
    
    # Счётчики
    event_count = 0
    start_time = time.time()
    last_stat_time = start_time
    
    print("🚀 Генератор запущен!")
    print(f"⏱  Интервал: {EVENT_INTERVAL} сек между событиями")
    print("📝 Нажмите Ctrl+C для остановки\n")
    print("─" * 70)
    
    try:
        while True:
            # Генерируем событие
            event = generate_event()
            
            # Сохраняем в БД
            if insert_event(conn, event):
                event_count += 1
                
                # Выводим информацию о событии
                print(format_event_log(event, event_count))
                
                # Статистика каждые 50 событий
                if event_count % 50 == 0:
                    print_statistics(event_count, start_time)
            
            # Пауза между событиями
            time.sleep(EVENT_INTERVAL)
            
    except KeyboardInterrupt:
        print("\n\n⏹  Получен сигнал остановки...")
    except Exception as e:
        print(f"\n\n✗ Критическая ошибка: {e}")
    finally:
        # Финальная статистика
        print_statistics(event_count, start_time)
        
        # Закрываем соединение
        conn.close()
        print("✓ Соединение с БД закрыто")
        print("✓ Генератор остановлен")
        print("=" * 70 + "\n")

if __name__ == "__main__":
    main()
