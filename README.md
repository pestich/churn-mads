# Прогнозирование оттока клиентов в сети ресторанов быстрого питания

Этот проект представляет собой модель машинного обучения для предсказания оттока клиентов из сети ресторанов быстрого питания. Для достижения этой цели мы используем две модели CatBoost: одну для классификации и другую для регрессии.

## Инструкции по установке и запуску

1. Установите все необходимые библиотеки, указанные в файле `requirements.txt`, выполнив следующую команду:
   ```bash
   pip install -r requirements.txt
   
   python main.py --path PATH_TO_GZIP_FILE

Запустите скрипт, указав путь к файлу данных в формате gzip.


2. Структура проекта

    data/: В этом каталоге содержатся данные, необходимые для модели.

    models/: Здесь находятся сохраненные модели CatBoost для классификации и регрессии.

    main.py: Основной скрипт проекта с интерфейсом командной строки. Он загружает данные, обучает модели и предсказывает отток клиентов.

    src: Вспомогательный скрипт с функциями обработки данных и вспомогательными функциями.
    
    notebooks: Jupyter-тетрадки обучением модели. 
