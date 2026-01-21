FROM python:3.11-slim

# Устанавливаем рабочую директорию
WORKDIR /app

# Копируем зависимости
COPY requirements.txt .

# Устанавливаем Python пакеты
RUN pip install --no-cache-dir -r requirements.txt

# Копируем код генератора
COPY generator.py .

# Запускаем генератор при старте контейнера
CMD ["python", "-u", "generator.py"]