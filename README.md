# Lakehouse и бенчмарки TPC-H Q3

Вояга Даниил Максимович, БИВТ-24-СП-3. Вариант 1. Основные Bronze, Silver и Gold используют `sf1`; `sf3` выполнен отдельно для контроля масштаба.

Итоговый документ: [отчет.md](отчет.md).

## Что выполнено

- Docker Compose: PostgreSQL, Silo (совместимый форк MinIO), Lakekeeper, Trino, контейнер инструментов с DuckDB / PyIceberg.
- Bronze: полные проекции customer, orders и lineitem. Silver: очищенный, нормализованный и обогащённый целевой факт Q3. Gold: 11 620 агрегированных заказов и топ-10.
- Две сессии `main` и `repeat1`: по 72 CTAS и 80 измеряемых запросов. Восемь сочетаний Parquet / ORC с NONE / SNAPPY / GZIP / ZSTD, три записи и пять запросов каждой нагрузки после прогрева.
- Дополнительный CTAS из сохранённого Bronze: 72 записи без генератора TPC-H в измеряемом плане.
- Контрольные checksum-запросы, оценка повторяемости и объёма физического ввода.
- Универсальный Silver с 6 001 215 строками: сравнение без партиций и с `month(orderdate)`. Проверены границы дат в реальных Parquet-файлах.
- Сравнение Bronze / универсального Silver / целевого Silver / Gold в одной перемешанной серии; отдельный контроль sf1 / sf3.
- Изменение схемы на копии Silver, контроль файлов и снапшотов; восстановление тестовой таблицы.
- DuckDB как SQL-движок над Arrow после чтения Iceberg через PyIceberg. Нативный Iceberg-сканер DuckDB не используется.

## Состав проекта

| Файл / каталог | Назначение |
|---|---|
| `отчет.md` | Итоговый отчёт для сдачи |
| `docker-compose.yml`, `config/trino`, `tools/Dockerfile` | Развёртывание, каталоги и зависимости |
| `sql/medallion.sql`, `q3.sql`, `q3_bronze.sql` | ELT и запрос варианта 1 |
| `sql/restore_snapshot.sql` | Снапшот, фактически проверенный на исходном стенде |
| `scripts/benchmark.py`, `trino_client.py` | Бенчмарк и журнал Trino HTTP API |
| `scripts/pipeline.py`, `demos.py`, `duckdb_demo.py` | Слои, демонстрации и дополнительный движок |
| `scripts/review_experiments.py` | Дополнительные измерения записи, партиций, слоёв и масштаба |
| `scripts/review_summary.py` | Сравнение main / repeat1 и извлечение physicalInputBytes |
| `scripts/review_finalize.py`, `partition_pruning_evidence.py` | Проверка Gold, S3-файлов и границ дат |
| `scripts/render_standalone_report.py` | Текущая версия генератора компактного отчёта |
| `scripts/render_report.py` | Совместимый вход в текущий генератор без параметров |
| `scripts/verify_results.py`, `verify_review.py` | Проверки сохранённых измерений |
| `results/benchmark_main.json`, `benchmark_repeat1.json` | Две исходные серии с query ID |
| `results/experiments_review1.json` | Все дополнительные измерения |
| `results/review_summary.json`, `review_storage_audit.json`, `partition_pruning.json` | Шум, размеры, кодеки и отбор файлов |
| `results/queries.jsonl` | Полные SQL и ответы Trino, включая ранние диагностические ошибки |
| `results/gold_top10.csv`, `validation.json` | Итог Q3 и проверки качества |
| `results/environment.json`, `image_digests.txt`, `python_packages.txt` | Стенд и версии |

Ранние диагностические ошибки в журнале не входят в измеряемые CSV. В отчёте используется серверное время `elapsedTimeMillis`; клиентское `wall_s` сохранено отдельно. Замеры прогретые, кеши не сбрасывались. У ORC настройка GZIP физически соответствует ZLIB.

## Проверить готовые результаты без Docker

Python 3.12, сторонние библиотеки для этих двух проверок не нужны:

```powershell
python scripts/verify_results.py --run-id main
python scripts/verify_results.py --run-id repeat1
python scripts/verify_review.py
```

Генератор использует сохранённые `main`, `repeat1` и `review1`, не выполняет запросы к Docker:

```powershell
python scripts/render_standalone_report.py
```

Он воспроизводит компактный отчёт, а не добавляет исходники и журналы в документ. При смене стенда или набора серий нужно отдельно обновить его входы и выводы; опубликованный отчёт описывает выполненную работу 13.10.2026.

## Запустить существующий стенд

Docker Desktop должен работать в режиме Linux containers. Исходный стенд имел около 16 ГиБ общей памяти Docker; Trino ограничен 8 CPU / 8 ГиБ, heap JVM = 6 ГиБ. Для первого развёртывания нужен доступ к Docker-реестрам и PyPI.

```powershell
docker compose up -d --build
docker compose exec -T tools python -u scripts/bootstrap.py
docker compose exec -T trino trino --execute "SELECT * FROM lakehouse.gold.shipping_priority_top10 ORDER BY revenue DESC"
```

Если основной Bronze / Silver / Gold ещё не построен, сначала выполните новую базовую серию и `pipeline.py`, как показано ниже. S3-данные хранятся в именованных Docker volumes, в репозиторий не включены. На новом компьютере они генерируются из коннектора TPC-H.

| Интерфейс | Адрес |
|---|---|
| Trino | http://localhost:8080 |
| Lakekeeper | http://localhost:8181/ui/ |
| S3 console | http://localhost:9001 |
| S3 endpoint | http://localhost:9000 |

Учебные значения доступа: S3 `lakehouse / lakehouse_local_lab`, PostgreSQL `lakekeeper / lakekeeper_lab`. Внешние порты привязаны к `127.0.0.1`; PostgreSQL доступен внутри сети Docker.

## Повторить базовый эксперимент

```powershell
./run_all.ps1
# Или задать новый, ещё не использованный идентификатор:
./run_all.ps1 -RunId fresh1
```

Скрипт запускает стек, сохраняет одну новую базовую серию по всем восьми комбинациям, строит / проверяет основные слои и проверяет результаты. Он сохраняет итоговый отчёт двух исходных сессий. Идентификаторы `main` и `repeat1` уже присутствуют в опубликованных результатах, поэтому для новых серий выбирайте другие имена.

Ручной эквивалент:

```powershell
docker compose up -d --build
docker compose exec -T tools python -u scripts/bootstrap.py
docker compose exec -T tools python -u scripts/benchmark.py --run-id fresh1
docker compose exec -T tools python -u scripts/pipeline.py
docker compose exec -T tools python -u scripts/demos.py
docker compose exec -T tools python -u scripts/duckdb_demo.py
docker compose exec -T tools python -u scripts/storage_audit.py --run-id fresh1
docker compose exec -T tools python -u scripts/verify_results.py --run-id fresh1
```

`pipeline.py` сохраняет существующие основные таблицы; демонстрация изменения схемы проводится на отдельной копии. `run_all.ps1` обновляет общие служебные файлы проверок и окружения. Исходные файлы `benchmark_main.json` и `benchmark_repeat1.json` сохраняются.

## Повторить дополнительные эксперименты

После подготовки основного медальона:

```powershell
docker compose exec -T tools python -u scripts/review_experiments.py --run-id extra2
```

Получится `results/experiments_extra2.json` с новой серией и новыми вспомогательными таблицами. Обработчики `review_summary.py`, `review_finalize.py`, `partition_pruning_evidence.py`, `verify_review.py` и генератор отчёта настроены на конкретные опубликованные серии `main`, `repeat1`, `review1`; для обработки новых имён нужно изменить их входы. Время, UUID файлов и snapshot ID при повторении отличаются, поэтому опубликованный ID в `restore_snapshot.sql` не переносится на новый стенд автоматически. `demos.py` получает текущий snapshot ID самостоятельно.

Остановить контейнеры, сохранив данные:

```powershell
docker compose down
```