-- =========================================
-- ИНИЦИАЛИЗАЦИЯ БАЗЫ ДАННЫХ ИГРОВОЙ СТАТИСТИКИ
-- =========================================

-- Создание основной таблицы событий
CREATE TABLE IF NOT EXISTS game_events (
    id SERIAL PRIMARY KEY,
    player_id VARCHAR(50) NOT NULL,
    player_name VARCHAR(100) NOT NULL,
    action_type VARCHAR(50) NOT NULL,
    points INTEGER NOT NULL,
    level INTEGER NOT NULL,
    weapon VARCHAR(50),
    map_name VARCHAR(50),
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Создание индексов для быстрых запросов
CREATE INDEX idx_player_id ON game_events(player_id);
CREATE INDEX idx_action_type ON game_events(action_type);
CREATE INDEX idx_timestamp ON game_events(timestamp);
CREATE INDEX idx_level ON game_events(level);
CREATE INDEX idx_map_name ON game_events(map_name);

-- Создание таблицы агрегированной статистики
CREATE TABLE IF NOT EXISTS player_stats (
    player_id VARCHAR(50) PRIMARY KEY,
    player_name VARCHAR(100) NOT NULL,
    total_kills INTEGER DEFAULT 0,
    total_deaths INTEGER DEFAULT 0,
    total_points INTEGER DEFAULT 0,
    current_level INTEGER DEFAULT 1,
    games_played INTEGER DEFAULT 0,
    last_activity TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Комментарии для документирования
COMMENT ON TABLE game_events IS 'Таблица всех игровых событий';
COMMENT ON COLUMN game_events.action_type IS 'Типы: kill, death, achievement, level_up, assist';
COMMENT ON COLUMN game_events.points IS 'Очки (могут быть отрицательными при death)';
COMMENT ON COLUMN game_events.weapon IS 'Оружие (NULL для неигровых событий)';
COMMENT ON COLUMN game_events.map_name IS 'Игровая карта';

COMMENT ON TABLE player_stats IS 'Агрегированная статистика игроков';

-- Вывод информации об успешной инициализации
DO $$
BEGIN
    RAISE NOTICE '✓ База данных успешно инициализирована';
    RAISE NOTICE '✓ Таблица game_events создана';
    RAISE NOTICE '✓ Таблица player_stats создана';
    RAISE NOTICE '✓ Индексы созданы';
END $$;