# Инструкция по запуску MVP "AI-финансовый аналитик"

Этот проект представляет собой MVP систему для финансовой аналитики, использующую FastAPI на бэкенде и Streamlit на фронтенде.

## Структура проекта
- `backend/`: API на FastAPI, логика обработки данных, аналитика и БД.
- `frontend/`: Интерфейс на Streamlit.
- `config/`: Конфигурационные файлы для маппинга колонок и категорий.

## Требования
- Python 3.10+
- SQLite (используется по умолчанию для MVP)

## Быстрый запуск

### 1. Подготовка окружения
```bash
# Клонирование репозитория (если еще не сделано)
git clone https://github.com/VasiliyTop/AI_finanalitik.git
cd AI_finanalitik

# Создание виртуального окружения
python3 -m venv venv
source venv/bin/activate  # для Windows: venv\Scripts\activate

# Установка зависимостей
pip install -r backend/requirements.txt
pip install -r frontend/requirements.txt
pip install python-multipart  # необходимо для загрузки файлов
```

### 2. Настройка базы данных
```bash
cd backend
export DATABASE_URL=sqlite:///./financial_analytics.db
python3 -c "from app.database import engine, Base; from app.models.database_models import *; Base.metadata.create_all(bind=engine)"
```

### 3. Запуск бэкенда
```bash
cd backend
uvicorn app.main:app --host 0.0.0.0 --port 8000
```

### 4. Запуск фронтенда
В новом окне терминала:
```bash
cd frontend
streamlit run app.py --server.port 8501
```

## Основные функции
1. **Import**: Загрузка файлов Adesk (XLS) и 1С.
2. **Dashboard**: Визуализация остатков, ДДС и структуры расходов.
3. **Forecast**: Прогноз денежного потока и выявление кассовых разрывов.
4. **Recommendations**: Автоматические советы по управлению финансами.
5. **Risks**: Оценка рисков ликвидности и контрагентов.
6. **Export**: Выгрузка отчетов в Excel.

## Исправления, внесенные в ходе разработки
- Исправлена ошибка `SyntaxError` из-за использования зарезервированного слова `import` в именах модулей.
- Исправлены ошибки типизации `Decimal` vs `float` в модулях аналитики.
- Добавлена автоматическая инициализация таблиц БД.
- Настроена корректная обработка дат в Pandas для графиков.
