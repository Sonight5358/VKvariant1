"""One self-contained Markdown report: results, evidence, SQL and replay sources."""
import json
import re
import statistics as st
from datetime import datetime,timezone,timedelta
from decimal import Decimal
from trino_client import ROOT,RESULTS

def read(name):
    return json.loads((RESULTS/name).read_text(encoding='utf-8-sig'))
def table(headers,rows):
    return '\n'.join(['| '+' | '.join(headers)+' |','| '+' | '.join(['---']*len(headers))+' |']+
                     ['| '+' | '.join(str(c).replace('\n','<br>') for c in r)+' |' for r in rows])
def med(rows,key):
    return st.median(r[key] for r in rows)
def count(n):
    return f'{n:,}'.replace(',',' ')
def details(title,content,language='text'):
    return f'<details>\n<summary>{title}</summary>\n\n```{language}\n{content.strip()}\n```\n\n</details>\n'

main=read('benchmark_main.json')
repeat=read('benchmark_repeat1.json')
e=read('experiments_review1.json')
noise=read('review_summary.json')['noise']
storage=read('review_storage_audit.json')
pruning=read('partition_pruning.json')
layers=read('layer_metrics.json')
validation=read('validation.json')
duck=read('duckdb_demo.json')
demos=read('demos.json')
env=read('environment.json')
gold=read('gold_top10.json')
records=[json.loads(line) for line in (RESULTS/'queries.jsonl').read_text(encoding='utf-8-sig').splitlines()]
by_id={r['id']:r for r in records if 'error' not in r}
summaries={run:{(r['format'],r['codec']):r for r in b['summary']}
           for run,b in [('main',main),('repeat1',repeat)]}
s=summaries['repeat1']
pqz=s['PARQUET','ZSTD']; pqn=s['PARQUET','NONE']; pqg=s['PARQUET','GZIP']
bronze_bytes=sum(r['bytes'] for r in layers if r['schema']=='bronze')
cpu=(RESULTS/'cpu_model.txt').read_text(encoding='utf-8-sig').split(':',1)[-1].strip()

stored={}
for key in s:
    rows=[r for r in e['materialized_writes'] if (r['format'],r['codec'])==key]
    stored[key]=[sum(r['elapsed_s'] for r in rows if r['repeat']==rep) for rep in (1,2,3)]
benchmark_table=table(['Формат','Кодек','Размер, МБ','CTAS из TPC-H, с','CTAS из Bronze, с','Q3, с','Q3: ввод, МБ','Агрегация, с'],
    [[fmt,codec,f"{s[fmt,codec]['bytes_median']/1e6:.3f}",f"{s[fmt,codec]['write_median_s']:.3f}",
      f"{st.median(stored[fmt,codec]):.3f}",f"{s[fmt,codec]['q3_median_s']:.3f}",
      f"{s[fmt,codec]['q3_physical_mb']:.3f}",f"{s[fmt,codec]['aggregate_median_s']:.3f}"] for fmt,codec in s])
session_table=table(['Формат / кодек','Запись main / repeat1, с','Q3 main / repeat1, с','Агрегация main / repeat1, с'],
 [[fmt+' / '+codec]+[f"{summaries['main'][fmt,codec][metric+'_median_s']:.3f} / {s[fmt,codec][metric+'_median_s']:.3f}"
                     for metric in ('write','q3','aggregate')] for fmt,codec in s])
noise_table=table(['Метрика','Макс. разность медиан сессий, с','Макс. диапазон одной группы, с','Принятый порог, с'],
 [[{'write':'Запись 3 таблиц','q3':'Q3','aggregate':'Полная агрегация'}[r['metric']]]+
  [f"{r[k]:.3f}" for k in ('max_session_median_delta_s','max_within_group_range_s','empirical_threshold_s')] for r in noise])

def layer_table(schema):
    rows=[]
    for r in layers:
        if r['schema']!=schema:
            continue
        fields=r['ddl'].split('(',1)[1].split('\n)')[0]
        names=', '.join(line.strip().split()[0] for line in fields.splitlines() if line.strip())
        why={'customer':'Два поля для сегмента и связи; все исходные строки.',
             'orders':'Ключи, дата и приоритет; единый формат со строками заказов.',
             'lineitem':'Цена, скидка и дата отгрузки; linenumber нужен для проверки ключа строки.',
             'order_lines':'Целевой очищенный факт Q3; 30 519 строк не требуют разбиения на мелкие файлы.',
             'order_revenue':'Полный агрегат по всем 11 620 подходящим заказам.',
             'shipping_priority_top10':'Итог варианта 1; единый формат удобнее сопровождения, сжатие здесь не определяет выбор.'}[r['table']]
        rows.append([f"`{schema}.{r['table']}`",names,count(r['rows']),f"{r['bytes']/1e6:.6f}",
                     'PARQUET / ZSTD','нет',why])
    return table(['Таблица','Колонки','Строк','Вес, МБ','Формат / кодек','Партиционирование','Обоснование'],rows)

fullrows=[r for r in e['partition_queries'] if r['table']=='order_lines_full_review1']
partrows=[r for r in e['partition_queries'] if r['table']=='order_lines_monthly_review1']
part_table=table(['Silver-копия','Строк всего','Data-файлов','Вес, МБ','Q3, с','Диапазон, с','Ввод, МБ','processedRows'],
 [[name,count(m['rows']),m['files'],f"{m['bytes']/1e6:.3f}",f"{med(rows,'elapsed_s'):.3f}",
   f"{min(r['elapsed_s'] for r in rows):.3f}-{max(r['elapsed_s'] for r in rows):.3f}",
   f"{med(rows,'physical_bytes')/1e6:.3f}",count(med(rows,'processed_rows'))]
  for name,m,rows in [('Без партиций',e['full_fact_metrics'],fullrows),('month(orderdate)',e['monthly_fact_metrics'],partrows)]])
layer_table_compare=table(['Источник результата Q3','Время, с','Диапазон, с','Ввод, МБ','processedRows'],
 [[name,f"{med(rs,'elapsed_s'):.3f}",f"{min(r['elapsed_s'] for r in rs):.3f}-{max(r['elapsed_s'] for r in rs):.3f}",
   f"{med(rs,'physical_bytes')/1e6:.6f}",count(med(rs,'processed_rows'))]
  for name,rs in [(n,[r for r in e['layer_queries'] if r['layer']==n])
                 for n in ('Bronze','Silver full','Silver Q3','Gold')]])
sf1=[r for r in e['scale_queries'] if r['sf']==1]
sf3=[r for r in e['scale_queries'] if r['sf']==3]
sf3bytes=sum(r['bytes'] for r in e['sf3_metrics'].values())
scale_table=table(['Масштаб','customer / orders / lineitem','Bronze, МБ','Q3, с','Ввод, МБ','Planning, мс'],
 [['sf1','150 000 / 1 500 000 / 6 001 215',f'{bronze_bytes/1e6:.3f}',f"{med(sf1,'elapsed_s'):.3f}",f"{med(sf1,'physical_bytes')/1e6:.3f}",med(sf1,'planning_ms')],
  ['sf3',' / '.join(count(e['sf3_metrics'][t]['rows']) for t in ('customer','orders','lineitem')),
   f'{sf3bytes/1e6:.3f}',f"{med(sf3,'elapsed_s'):.3f}",f"{med(sf3,'physical_bytes')/1e6:.3f}",med(sf3,'planning_ms')]])
baseline_table=table(['Источник','customer, с','orders, с','lineitem, с','Сумма трёх запросов, с'],
 [[prefix]+[f"{med([r for r in e['baseline'] if r['source']==prefix and r['table']==t],'elapsed_s'):.3f}" for t in ('customer','orders','lineitem')]+
  [f"{st.median([sum(r['elapsed_s'] for r in e['baseline'] if r['source']==prefix and r['repeat']==rep) for rep in (1,2,3)]):.3f}"]
  for prefix in ('tpch.sf1','lakehouse.bronze')])
gold_table=table(['orderkey','revenue','orderdate','shippriority'],
 [[r[0],f'{Decimal(str(r[1])):.4f}',r[2],r[3]] for r in gold])
ev=e['evolution']
before,after=ev['before'],ev['after']
evo_table=table(['Показатель','До ALTER','После ADD и RENAME'],
 [['Строк',count(before['values'][0][0]),count(after['values'][0][0])],
  ['Сумма net_revenue',before['values'][0][1],after['values'][0][1]],
  ['Data-файлов',len(before['files']),len(after['files'])],
  ['Байт data-файлов',sum(r[1] for r in before['files']),sum(r[1] for r in after['files'])],
  ['Снапшотов данных',len(before['snapshots']),len(after['snapshots'])],
  ['Snapshot ID',before['snapshots'][0][0],after['snapshots'][0][0]],
  ['Полный путь data-файла совпадает','Да','Да']])

def interval(b):
    times=[datetime.fromisoformat(by_id[r['query_id']]['timestamp_utc']).astimezone(timezone(timedelta(hours=3)))
           for r in b['writes']+b['queries']]
    return min(times).strftime('%H:%M:%S')+' - '+max(times).strftime('%H:%M:%S')
snap=demos['snapshot']
orig_snapshot=snap['id']
checksums=table(['Таблица','Строк','Checksum одинаков для TPC-H и Bronze'],
 [[t,count(next(r['rows'] for r in e['baseline'] if r['table']==t)),next(r['checksum'] for r in e['baseline'] if r['table']==t)]
  for t in ('customer','orders','lineitem')])
critique_table=table(['Пункт','Оценка и действие'],[
 ['1. Универсальный Silver','Не является обязательным исправлением: вариант 1 разрешает фильтры на Silver. Добавлены два универсальных факта для проверки альтернативы; все 11 620 заказов Gold совпали.'],
 ['2. Evolution на копии','Обоснованное улучшение. Основной Silver очищен от демонстрационного NULL-поля, ALTER проверен на отдельной копии с контролем файлов и снапшотов.'],
 ['3. Партиционирование','Обосновано для универсального факта. Выполнены пять измерений после прогрева, проверены размеры и фактические границы дат файлов.'],
 ['4. Вычитание генерации','Простое вычитание методологически некорректно. Контроль с checksum выполнен, но не вычитается; отдельно замерен CTAS из сохранённого Bronze.'],
 ['5. Шум','Обоснованно. Повторены все 8 комбинаций, приведены две сессии и консервативный эмпирический порог без заявления статистической значимости.'],
 ['6. Объём чтения','Обоснованно. Добавлен physicalInputBytes из журнала Trino. Это измеряемый ввод запроса, а не строго детерминированный размер всего набора файлов.'],
 ['7. Различия ORC и Parquet','Числа подтверждены, причины помечены как гипотезы: уровни сжатия и профили CPU отдельно не проверялись.'],
 ['8. Сравнение слоёв','Обоснованно. Выполнена одна перемешанная серия по Bronze, универсальному Silver, целевому Silver и Gold с проверкой одинакового результата.'],
 ['9. Масштаб и планирование','Добавлен отдельный sf3. Утверждение о преобладании планирования не подтверждено счётчиками Trino.'],
 ['10. Разные размеры Bronze','Пояснено: основной слой и медиана трёх benchmark-копий - разные физические файлы.'],
 ['11. DuckDB','Формулировка уточнена: SQL над Arrow после чтения Iceberg через PyIceberg, без нативного Iceberg-сканирования DuckDB.'],
 ['12. Скриншоты','В полученном задании они не обязательны, разрешена демонстрация через консоль. В один MD встроены фактические выводы, таблицы и query ID; внешние картинки не требуются.'],
 ['13. Silo и образы','Добавлены первичный источник о форке MinIO и digest использованных образов прямо в этот файл.']])

text=f'''# Отчёт по практической №1 - Lakehouse, ELT и бенчмарк форматов

## 1. Вариант и масштаб

| Поле | Значение |
|---|---|
| ФИО | Вояга Даниил Максимович |
| Группа | БИВТ-24-СП-3 |
| Вариант | 1 - TPC-H Q3 «Shipping Priority» |
| Основной масштаб | `tpch.sf1`, одинаковый для Bronze, Silver и Gold |
| Дополнительный эксперимент | Отдельный Bronze `sf3`, не смешанный с основными слоями |
| Дата | 13.10.2026 |

Сценарий Q3 - аналитика заказов клиентов сегмента BUILDING. Отбираются заказы с `orderdate < 1995-03-15`, а в выручку входят только их строки с `shipdate > 1995-03-15`. Выручка строки равна `extendedprice * (1 - discount)`. Результат - 10 заказов с наибольшей суммой такой выручки. Это синтетические данные TPC-H, генерируемые коннектором Trino; колонки используются без префиксов `l_`, `o_`, `c_`.

Выбран `sf1`: 150 000 клиентов, 1 500 000 заказов и 6 001 215 строк заказов позволяют выполнить повторные проверки на одном узле с 8 ГиБ памяти для Trino. Измерения Q3 занимают примерно 0,4-0,7 с, поэтому выводы о разнице в сотые доли секунды требуют проверки повторяемости. Контроль на `sf3` приведён ниже; он показывает увеличение времени с объёмом, но не заменяет эксперимент на большом распределённом кластере. `sf1` - масштаб всего TPC-H, а не размер выбранных проекций трёх таблиц в байтах.

Отчёт самостоятельный: результаты, SQL, конфигурация, команды и исходники для повторения приведены внутри этого MD. Для чтения и оценки не требуются соседние файлы или изображения.

## 2. Реально поднятые сервисы и решения

| Сервис / версия | Роль | Обоснование |
|---|---|---|
| Silo `RELEASE.2026-09-03T13-18-01Z` | S3-совместимое объектное хранилище | Совместимый форк MinIO: [первичный источник PGSTY](https://github.com/pgsty/silo). Используется в [официальном примере Lakekeeper](https://github.com/lakekeeper/lakekeeper/blob/main/examples/minimal/docker-compose.yaml). Официальные образы MinIO при развёртывании вернули ошибку доступа; Silo сохранил требуемую S3-архитектуру. В Compose сервис называется `minio`. |
| PostgreSQL 17.4 | База состояния каталога Lakekeeper | Поддерживаемая транзакционная БД; аналитические строки хранятся в S3, а не в PostgreSQL. |
| Lakekeeper 0.14.0 | Iceberg REST Catalog | Отделяет метаданные таблиц от вычислений и позволяет разным клиентам читать один каталог. |
| Trino 483 | Генерация TPC-H, ELT, бенчмарки | Один SQL-движок выполняет все сравниваемые варианты; коннектор TPC-H даёт воспроизводимый источник. |
| DuckDB 1.4.3 + PyIceberg 0.10.0 + Arrow 20.0.0 | SQL над данными, считанными из Iceberg через PyIceberg | Нативное Iceberg-сканирование DuckDB не используется. PyIceberg получает снапшот из REST-каталога, Arrow читает S3, DuckDB выполняет SQL в памяти. Для локального sf1 это проще дополнительного кластера Spark. |
| `migrate` | Однократная миграция схемы PostgreSQL | Тот же образ Lakekeeper; завершение с кодом 0 является ожидаемым состоянием. |

Версии и digest образов встроены в приложение. Именованные Docker volumes сохраняют объекты S3 и состояние PostgreSQL между перезапусками. Локальные порты привязаны к `127.0.0.1`; PostgreSQL доступен внутри сети Compose. Учебные значения доступа: S3 `lakehouse / lakehouse_local_lab`, PostgreSQL `lakekeeper / lakekeeper_lab`.

Фактический вывод Docker после дополнительных проверок:

```text
{(RESULTS/'docker_ps.txt').read_text(encoding='utf-8-sig').strip()}
```

Команда развёртывания: `docker compose up -d --build`. Полный Compose и конфигурация Trino приведены в приложении, а подготовка бакета / warehouse выполняется встроенным скриптом `bootstrap.py`.

## 3. Данные и исходный запрос варианта 1

| Таблица | Колонки | Использование |
|---|---|---|
| customer | custkey, mktsegment | Сегмент BUILDING и связь с заказами. |
| orders | orderkey, custkey, orderdate, shippriority | Связи, фильтр даты заказа, группировка и вывод. |
| lineitem | orderkey, linenumber, extendedprice, discount, shipdate | Связь, фильтр отгрузки, выручка. Дополнительный linenumber позволяет проверять составной ключ строки. |

```sql
{(ROOT/'sql/q3.sql').read_text(encoding='utf-8-sig').strip()}
```

Для бенчмарков меняются только источники этого запроса. Эталон выполняется на `tpch.sf1`; основной Bronze, все benchmark-копии и Gold дают те же 10 строк.

## 4. Медальон, качество и корректность

Iceberg - формат таблицы со схемой, снапшотами и ссылками на файлы; Parquet / ORC - форматы data-файлов; NONE / SNAPPY / GZIP / ZSTD - настройки сжатия. Вес ниже - сумма `file_size_in_bytes` актуальных data-файлов из `$files`, сверенная с реальными S3 HEAD. Метаданные, манифесты, старые версии и экспериментальные копии в вес основного слоя не включаются. 1 МБ = 1 000 000 байт.

### 4.1. Bronze

{layer_table('bronze')}

Все исходные строки сохранены, условия Q3 на Bronze не применяются, цена и скидка остаются `double` как у коннектора. Общий физический вес основного Bronze - {bronze_bytes/1e6:.6f} МБ. Каждая таблица имеет один data-файл. Партиционирование по orderkey породило бы чрезмерное число мелких объектов; для этой проекции sf1 выбран компактный непартиционированный слой.

### 4.2. Silver

{layer_table('silver')}

Подсказка именно к варианту 1 предусматривает фильтр по сегменту и датам на Silver и соединение в факт строк заказов. Поэтому основной `silver.order_lines` - **целевой факт Q3** из 30 519 строк; это допустимый выбор, а не потеря строк по ошибке. Для повторного использования в нескольких бизнес-задачах альтернативой служит универсальный факт без условий Q3. Эта альтернатива реально построена и проверена отдельно.

Правила очистки: положительные ключи, отсутствие NULL в обязательных полях, цена >= 0, скидка в [0; 1]. Сегмент нормализуется `upper(trim(mktsegment))`, цена приводится к `decimal(18,2)`, скидка - к `decimal(3,2)`. Обогащение: `net_revenue` в `decimal(22,4)` и `shipment_lag_days` - разница между датами заказа и отгрузки. Ключ строки факта - `(orderkey, linenumber)`, а не один orderkey.

Исходные данные генератора корректны: 0 нарушений правил, 0 дубликатов ключей, 0 нарушенных связей, 0 изменений значений сегмента при нормализации. Очистка реализована как проверяемые правила; искусственная порча данных не добавлялась. Сокращение до 30 519 строк связано с условиями варианта.

Дополнительные копии Silver:

| Таблица | Назначение | Строк | Файлов | Вес, МБ | Формат / сжатие | Партиции |
|---|---|---|---|---|---|---|
| order_lines_full_review1 | Универсальный очищенный и обогащённый факт | 6 001 215 | {e['full_fact_metrics']['files']} | {e['full_fact_metrics']['bytes']/1e6:.6f} | Parquet / ZSTD | нет |
| order_lines_monthly_review1 | Те же строки для эксперимента | 6 001 215 | {e['monthly_fact_metrics']['files']} | {e['monthly_fact_metrics']['bytes']/1e6:.6f} | Parquet / ZSTD | month(orderdate) |
| order_lines_evo_review1 | Полная копия целевого Silver для изменения схемы | 30 519 | {len(before['files'])} | {sum(r[1] for r in before['files'])/1e6:.6f} | Parquet / ZSTD | нет |

Универсальные копии равны по числу строк и checksum всех 11 исходных / обогащённых колонок. Повторное применение Q3 к универсальному факту даёт те же 11 620 заказов: двусторонний `EXCEPT` относительно основного Gold обнаружил **0 различий во всех агрегированных строках**, не только в топ-10.

### 4.3. Gold

{layer_table('gold')}

Полный агрегат `order_revenue` сохраняет все подходящие заказы; `shipping_priority_top10` содержит результат варианта 1. При чтении явно применяется `ORDER BY revenue DESC`, поскольку физический порядок строк SQL-таблицы не гарантируется.

{gold_table}

Ключи, даты, приоритеты и порядок совпадают с эталоном. Выручка сравнивается с абсолютным допуском 0.000001 из-за различия `double` источника и `decimal` Silver / Gold; итог отображён с четырьмя знаками. Все измеряемые Q3 двух сессий и дополнительных экспериментов дали правильный результат. Для полной агрегации lineitem число строк сравнивается точно, сумма `double` - с допуском 0.05 из-за разного порядка сложения миллионов значений.

Контроль одинаковых сырых значений TPC-H и основного Bronze:

{checksums}

## 5. Методология и бенчмарки

### 5.1. Стенд, повторы и метрики

| Параметр | Значение |
|---|---|
| CPU в WSL2 | {cpu} |
| Docker / ядро | {env['docker_version']} / `{env['kernel']}` |
| Ресурсы Docker | {env['logical_cpus']} логических CPU, {env['docker_memory_bytes']/1024**3:.2f} ГиБ памяти |
| Trino | Один узел, 8 CPU / 8 ГиБ; JVM heap 6 ГиБ |
| Исполнение | task.concurrency=4; task.min-writer-count=1 и task.max-writer-count=1 |
| main и repeat1 | По 8 комбинаций, 3 записи трёх таблиц, 1 исключённый прогрев и 5 запросов на каждую из 2 нагрузок |
| Дополнительная запись | 8 комбинаций × 3 повтора × 3 таблицы из сохранённого Bronze |
| Дополнительные чтения | По 5 повторов после прогрева; источники перемешаны в каждом раунде |
| Порядок | Последовательно; seed=20261013 для двух основных сессий, seed=20261014 для дополнительных экспериментов |
| Время | elapsedTimeMillis / 1000 из окончательного ответа Trino HTTP API |
| Физический ввод | physicalInputBytes / 1 000 000; медиана пяти измерений |
| Обработанные строки | processedRows из API; это счётчик выполнения, а не число строк бизнес-результата |
| Режим | Прогретые измерения, кеши ОС / S3 / JVM не сбрасывались |

Всего выполнено 216 измеряемых CTAS: 72 в main, 72 в repeat1 и 72 из материализованного Bronze. Основные две сессии содержат 160 измеряемых аналитических запросов. Дополнительно: 18 контрольных checksum-запросов, 10 запросов для партиционирования, 20 для сравнения слоёв и 10 для масштаба; прогревы и проверки корректности в эти количества не входят.

Время записи набора - сумма времён CTAS трёх таблиц в одном повторе, затем медиана трёх таких сумм. Размер набора рассчитывается аналогично по актуальным data-файлам. До записи writer-код всех комбинаций прогрет на `tpch.tiny`. Q3 и полная агрегация `count(*), sum(extendedprice * (1 - discount))` выполняются на таблицах третьего повтора. Серверное время включает планирование и завершение; полное клиентское `wall_s` сохраняется отдельно.

Сессии 13.10.2026, время Москвы: main {interval(main)}, repeat1 {interval(repeat)}. Точный query ID каждого измерения и его статистики встроены в приложение. Новые данные не подставлены вместо старых: две серии сравниваются явно.

`physicalInputBytes` характеризует ввод, учтённый движком при данном запросе. Он отличается от полного размера таблиц: читаются нужные столбцы / блоки, могут действовать фильтры, а чтение блоков имеет накладные расходы. Это не измерение гарантированного обращения к физическому диску хоста и не обещание строго одинакового числа байт при любом выполнении запроса.

### 5.2. Повторяемость и осторожная интерпретация

{session_table}

Для каждой комбинации вычислены разность медиан main / repeat1 и диапазон внутри каждой серии. Принят консервативный общий порог для каждой метрики: максимум из максимальной межсессионной разности и максимального внутригруппового диапазона. Он служит правилом осторожной интерпретации, **не является доверительным интервалом или доказательством статистической значимости**. Две сессии на одном стенде не позволяют оценить все источники изменчивости.

{noise_table}

Вывод о преимуществе времени делается, только если знак разницы совпадает в обеих сессиях и абсолютная разница медиан в обеих превышает соответствующий порог. Поэтому прежние формулировки о преимуществе Parquet NONE над ZSTD по полной агрегации и о преимуществе ORC NONE над Parquet NONE по Q3 заменены на нейтральное описание измерений.

### 5.3. Форматы / кодеки и запись без генератора в измеряемом CTAS

Ниже используются медианы **repeat1** для чтения и CTAS из TPC-H. Колонка «CTAS из Bronze» относится к отдельной серии с общим неизменяемым источником Parquet / ZSTD в S3. Эта серия исключает генератор TPC-H из плана записи, но включает чтение и декодирование общего источника, кодирование, сжатие, передачу в S3 и commit Iceberg. Она не называется чистым временем компрессора или чистого дискового writer.

{benchmark_table}

Таблица из исходного шаблона для одного формата:

| Сжатие Bronze в Parquet | Q3, с | Размер, МБ | Вывод |
|---|---|---|---|
| GZIP | {pqg['q3_median_s']:.3f} | {pqg['bytes_median']/1e6:.3f} | Размер близок к ZSTD, но различия записи и Q3 в обеих сессиях выше принятых порогов. |
| ZSTD | {pqz['q3_median_s']:.3f} | {pqz['bytes_median']/1e6:.3f} | Выбран как компромисс размера и времени; универсальное превосходство над всеми вариантами не заявляется. |

Контрольный запрос по каждой проекции: `SELECT count(*), to_hex(checksum(ROW(...)))`. Один прогрев, три измерения, одинаковые поля и проверка совпадения checksum:

{baseline_table}

**Эта базовая линия не вычитается из времени CTAS.** Checksum-запрос добавляет свою агрегацию; параллелизм и перекрытие генерации, сжатия и ввода-вывода в CTAS отличаются. Разность двух elapsed-времён не изолирует стоимость генератора. Корректное дополнение - реальные CTAS из уже сохранённого Bronze, приведённые выше.

Основной Bronze весит {bronze_bytes/1e6:.6f} МБ, медиана benchmark-копий Parquet / ZSTD в main - {summaries['main']['PARQUET','ZSTD']['bytes_median']/1e6:.6f} МБ, в repeat1 - {pqz['bytes_median']/1e6:.6f} МБ. Это разные физические наборы файлов: медиана рассчитывается по трём копиям, а не по основной таблице и не только по третьему повтору. Порядок строк при параллельном поступлении может различаться и немного менять упаковку файлов; во встроенных первичных результатах сохранены реальные размеры каждого повторения.

### 5.4. Партиционирование универсального Silver

Сравнивались одни и те же 6 001 215 строк, те же колонки, Parquet / ZSTD, условия Q3 и лимиты ресурсов. Одна копия без партиций, другая с `month(orderdate)`. Источники перемешаны в каждом раунде; один прогрев и пять измерений каждого запроса.

{part_table}

По этим измерениям месячная организация ускорила запрос в {med(fullrows,'elapsed_s')/med(partrows,'elapsed_s'):.2f} раза и снизила учтённый ввод в {med(fullrows,'physical_bytes')/med(partrows,'physical_bytes'):.2f} раза. Эффект существенно выше разброса повторов. Цена - 80 объектов вместо одного и увеличение суммарного размера на {(e['monthly_fact_metrics']['bytes']/e['full_fact_metrics']['bytes']-1)*100:.2f}%. Размеры месячных файлов: минимум {e['partition_file_distribution'][1]/1e6:.6f} МБ, максимум {e['partition_file_distribution'][2]/1e6:.6f} МБ, среднее {e['partition_file_distribution'][3]/1e6:.6f} МБ. Для промышленного хранилища такие файлы мелкие; при sf1 это ожидаемо.

Причина проверена чтением min / max дат в метаданных фактических Parquet-файлов. Из 80 файлов условия `orderdate < 1995-03-15` и `shipdate > 1995-03-15` допускают **{pruning['eligible_files']} файлов и {count(pruning['eligible_rows'])} исходных строк**, что совпадает со счётчиком processedRows месячного запроса. Это месяцы с ноября 1994 по март 1995. Условие по orderdate отсекает более поздние партиции, а диапазоны shipdate в файлах позволяют исключить старые месяцы. Фильтры по дням и сегменту затем оставляют 30 519 строк Q3.

Измерен совокупный эффект месячной раскладки, файловой статистики и параллельного чтения, а не изолированная стоимость только механизма партиций. Основной целевой Silver из 30 519 строк оставлен без партиций: универсальный факт гораздо больше и решает другую задачу.

### 5.5. Слои в одной сессии

Все варианты вернули одинаковый топ-10. Один прогрев и пять раундов в перемешанном порядке на том же Trino:

{layer_table_compare}

Bronze выполняет фильтры, два соединения и агрегацию. Универсальный Silver уже хранит соединённый факт и выручку строки, но ещё применяет условия Q3. Целевой Silver дополнительно материализует этот отбор, а Gold хранит готовый результат. Поэтому меньшие времена Silver / Gold объясняются ранее выполненной работой, а не одним кодеком. Время построения слоёв нужно учитывать при оценке полного ELT; читательская таблица не включает его повторно.

### 5.6. Контроль масштаба

Отдельный `sf3` записан теми же проекциями Bronze в Parquet / ZSTD; данные основного медальона остались на sf1. Запросы sf1 / sf3 перемешаны в пяти раундах после прогрева, каждый сравнивается с собственным эталоном TPC-H.

{scale_table}

Переход к sf3 увеличил время примерно в {med(sf3,'elapsed_s')/med(sf1,'elapsed_s'):.2f} раза и учтённый ввод в {med(sf3,'physical_bytes')/med(sf1,'physical_bytes'):.2f} раза. Planning на sf1 составил {med(sf1,'planning_ms')} мс при общем времени {med(sf1,'elapsed_s')*1000:.0f} мс, около {med(sf1,'planning_ms')/(med(sf1,'elapsed_s')*1000)*100:.1f}%. Значит, утверждение о преобладании именно планирования в этом опыте не подтверждается. Другие фиксированные расходы не разложены на независимые компоненты. Контроль масштаба полезен, но не заменяет полноценную повторную матрицу всех кодеков на sf3.

### 5.7. Выводы по измерениям и ограничения

Parquet / ZSTD уменьшил вес относительно Parquet / NONE примерно на {(1-pqz['bytes_median']/pqn['bytes_median'])*100:.2f}%. Разница Q3 NONE / ZSTD составляет сотые доли секунды и ниже принятого порога; установленного преимущества по времени не заявляется. По сравнению с GZIP размеры Parquet близки, но запись и Q3 с ZSTD быстрее в обеих сессиях с разницей выше порога. Дополнительная серия из сохранённого Bronze также показывает более быстрое выполнение CTAS с ZSTD, чем с GZIP.

ORC / NONE имеет меньшую наблюдаемую медиану Q3, чем Parquet / NONE, но разница ниже принятого порога в обеих сессиях. Поэтому он не объявляется уверенным победителем. Выбор Parquet / ZSTD основан прежде всего на уменьшении объёма при приемлемом времени чтения и записи.

Два отличия требуют осторожного объяснения. ORC / ZSTD весит {s['ORC','ZSTD']['bytes_median']/1e6:.3f} МБ: это больше ORC / GZIP ({s['ORC','GZIP']['bytes_median']/1e6:.3f} МБ) и на {(s['ORC','ZSTD']['bytes_median']/pqz['bytes_median']-1)*100:.2f}% больше Parquet / ZSTD. ORC / GZIP записывается быстрее Parquet / GZIP и из TPC-H, и из общего сохранённого источника. Возможные причины - различная внутренняя кодировка колонок, размеры блоков / страниц, реализации writer и параметры алгоритмов по умолчанию. **Это гипотезы:** конкретные уровни сжатия, CPU-профили и вклад каждого фактора отдельно не измерялись. Настройка `GZIP` для ORC физически соответствует ZLIB; для Parquet - GZIP, что проверено чтением файлов.

Измерения относятся к одному локальному узлу и прогретым данным. Кеши не сбрасывались, уровни кодеков по умолчанию не варьировались, фоновые нагрузки хоста не управлялись. Размеры и объём ввода полезны наряду со временем; небольшие различия времени не следует переносить на другие машины или масштабы. Партиционирование улучшило именно проверенный запрос, но создало мелкие файлы и не доказано как оптимум для всех запросов.

## 6. Осмысление и дополнительный инструмент

### 6.1. Lakehouse, schema evolution и снапшоты

Lakehouse полезен при разных источниках и нескольких вычислительных клиентах: объектное хранение отделено от SQL-движка, а Iceberg обеспечивает управляемую схему, атомарные изменения и историю таблицы. Bronze сохраняет исходные значения, Silver задаёт правила качества и обогащение, Gold ускоряет повторные бизнес-запросы. Для небольшого sf1 обычная аналитическая БД также подошла бы; эта работа демонстрирует переносимость данных и полный многослойный цикл.

Изменение схемы проверено на `silver.order_lines_evo_review1`, полной копии целевого Silver. Основная таблица не содержит демонстрационного `source_name`.

```sql
ALTER TABLE lakehouse.silver.order_lines_evo_review1
ADD COLUMN source_system VARCHAR;
ALTER TABLE lakehouse.silver.order_lines_evo_review1
RENAME COLUMN source_system TO source_name;
```

{evo_table}

После ALTER все {count(ev['null_count'])} старые строки читают новое поле как NULL. Список путей, размеры data-файлов и ID снапшотов до / после совпали точно. Следовательно, в этой демонстрации изменились метаданные схемы, без перезаписи data-файла и без создания нового снапшота данных. Новая версия metadata JSON и новый data snapshot - разные понятия; неизменное число снапшотов не означает отсутствия изменения схемы.

Time travel / rollback проверен на отдельной `demo.snapshot_demo`: {snap['before_rows']} строк до удаления, {snap['after_delete_rows']} после, {snap['historical_rows']} при чтении старого снапшота, {snap['restored_rows']} после отката. Восстановлены все ключи строк и `net_revenue`, а не только count.

```sql
SELECT count(*) FROM lakehouse.demo.snapshot_demo
FOR VERSION AS OF {orig_snapshot};
ALTER TABLE lakehouse.demo.snapshot_demo
EXECUTE rollback_to_snapshot({orig_snapshot});
SELECT count(*) FROM lakehouse.demo.snapshot_demo;
```

Snapshot ID `{orig_snapshot}` принадлежит выполненному стенду. При повторении скрипт автоматически получает текущий ID из `$refs`; старые снапшоты и их файлы должны сохраняться. История фактической demo-таблицы:

{table(['committed_at','snapshot_id','parent_id','operation'],snap['history'])}

### 6.2. Дополнительный движок и Bronze

Выбран DuckDB в контейнере `tools`. Это движок SQL над Arrow после чтения Bronze клиентом PyIceberg, без нативного Iceberg-сканирования DuckDB. Нативный сервер расширений при развёртывании отвечал HTTP 403; поддерживаемая [интеграция PyIceberg / Arrow / DuckDB](https://py.iceberg.apache.org/api/) позволила выполнить проверку.

Действия в консоли: `docker compose exec -T tools python -u scripts/duckdb_demo.py`. Скрипт подключился к `http://lakekeeper:8181/catalog`, warehouse `lakehouse`, загрузил текущие снапшоты трёх Bronze-таблиц, прочитал Parquet из `http://minio:9000` через Arrow FileIO, зарегистрировал таблицы в DuckDB и выполнил SQL. Данные не поступали из Gold или экспортированного CSV; постоянная копия в другую БД не создавалась. Этот путь рассчитан на объём, помещающийся в память.

| Проверка | Получено |
|---|---|
| DuckDB version | {duck['duckdb_version']} |
| customer count(*) | {count(duck['customer_count'])} |
| orders count(*) | {count(duck['orders_count'])} |
| lineitem count(*) | {count(duck['lineitem_count'])} |
| Клиентов BUILDING | {count(dict(duck['segments'])['BUILDING'])} |
| Q3 совпал с Trino | Да, все 10 строк с денежным допуском 0.000001 |

Фактически просмотренные строки customer:

{table(['custkey','mktsegment'],duck['customer_sample'])}

## 7. Выводы

Целевой Silver с фильтрами Q3 соответствует варианту 1, а дополнительный универсальный факт показывает возможность повторного использования очищенных данных. Parquet / ZSTD выбран за компактность и устойчивое преимущество над Parquet / GZIP по записи и Q3; малые различия с несжатыми форматами по времени не объявляются установленными. Месячная раскладка универсального факта сократила читаемый набор до пяти файлов и заметно ускорила проверенный запрос, но создала 80 мелких объектов. Изменение схемы на копии не переписало data-файл, откат восстановил все тестовые значения, а DuckDB подтвердил доступность Bronze другому инструменту. Выводы ограничены прогретым локальным стендом, основной проекцией sf1 и отдельным контролем sf3.

## Приложение А. Проверка замечаний к методологии

{critique_table}

## Приложение Б. Встроенные доказательства

Для каждого измерения указан уникальный query ID Trino, серверное время, размер / объём ввода и результат проверки. Основные серии содержат все 72 CTAS и 80 аналитических измерений каждая; прогревы исключены из этих списков. Физические размеры дополнительных объектов сверены с S3: {storage['verified_tables']} таблиц, {storage['verified_files']} data-файлов. Фактические кодеки проверены по метаданным одного реального файла каждой таблицы.

'''
text+=details('main: все первичные измерения',json.dumps(main,ensure_ascii=False,indent=2),'json')
text+=details('repeat1: все первичные измерения',json.dumps(repeat,ensure_ascii=False,indent=2),'json')
compact_e={k:v for k,v in e.items() if k not in ('partition_plans','final_ddl','full_fact_sql','monthly_sql')}
text+=details('Дополнительные эксперименты: все измерения и проверки',json.dumps(compact_e,ensure_ascii=False,indent=2),'json')
text+=details('Физические S3-файлы и кодеки',json.dumps(storage,ensure_ascii=False,indent=2),'json')
text+=details('Файлы, допустимые по диапазонам дат',json.dumps({k:v for k,v in pruning.items() if k!='files'}|
                    {'eligible_file_bounds':[r for r in pruning['files'] if r['eligible_by_dates']]},ensure_ascii=False,indent=2),'json')
text+=details('Gold, качество и вывод DuckDB',json.dumps({'validation':validation,'gold_top10':gold,'duckdb':duck},ensure_ascii=False,indent=2),'json')
text+=details('Digest использованных образов',(RESULTS/'image_digests.txt').read_text(encoding='utf-8-sig'))
text+=details('Версии Python-пакетов',(RESULTS/'python_packages.txt').read_text(encoding='utf-8-sig'))

text+='''
## Приложение В. Полное воспроизведение из одного MD

На новом стенде сохраните блоки ниже в файлы с указанными именами, сохраняя структуру каталогов. Данные отдельно скачивать не нужно. Требуется Docker Desktop с Linux containers, доступ к реестрам Docker / PyPI, свободные локальные порты 8080, 8181, 9000, 9001 и достаточные ресурсы памяти. Команды ниже рассчитаны на первое выполнение в новом проекте; завершённые идентификаторы бенчмарка намеренно не перезаписываются.

```powershell
New-Item -ItemType Directory -Force config/trino/catalog, scripts, sql, tools, results
# Сохранить приведённые ниже файлы в соответствующие каталоги.
docker compose up -d --build
docker compose exec -T tools python -u scripts/bootstrap.py
docker compose exec -T tools python -u scripts/benchmark.py --run-id main
docker compose exec -T tools python -u scripts/benchmark.py --run-id repeat1
docker compose exec -T tools python -u scripts/pipeline.py
docker compose exec -T tools python -u scripts/demos.py
docker compose exec -T tools python -u scripts/duckdb_demo.py
docker compose exec -T tools python -u scripts/review_experiments.py --run-id review1
docker compose exec -T tools python -u scripts/review_summary.py
docker compose exec -T tools python -u scripts/review_finalize.py
docker compose exec -T tools python -u scripts/partition_pruning_evidence.py
```

Воспроизводится алгоритм и данные, а не побитовая идентичность времени, UUID файлов или snapshot ID. Числа на другой машине будут отличаться. Остановить стенд с сохранением данных: `docker compose down`; именованные volumes при этом остаются.

'''
files=[('docker-compose.yml','yaml'),('tools/Dockerfile','dockerfile'),
       ('config/trino/config.properties','properties'),('config/trino/node.properties','properties'),
       ('config/trino/jvm.config','text'),('config/trino/catalog/tpch.properties','properties'),
       ('config/trino/catalog/lakehouse.properties','properties'),('sql/q3.sql','sql'),('sql/medallion.sql','sql'),
       ('scripts/trino_client.py','python'),('scripts/bootstrap.py','python'),('scripts/benchmark.py','python'),
       ('scripts/pipeline.py','python'),('scripts/demos.py','python'),('scripts/duckdb_demo.py','python'),
       ('scripts/review_experiments.py','python'),('scripts/review_summary.py','python'),
       ('scripts/review_finalize.py','python'),('scripts/partition_pruning_evidence.py','python')]
for name,language in files:
    text+=details(name,(ROOT/name).read_text(encoding='utf-8'),language)

text+='''
## Приложение Г. Документация

- [Trino: Iceberg, форматы, кодеки, метаданные и time travel](https://trino.io/docs/current/connector/iceberg.html).
- [Trino: HTTP-протокол клиента](https://trino.io/docs/current/develop/client-protocol.html).
- [Silo: первичный репозиторий форка MinIO](https://github.com/pgsty/silo).
- [Lakekeeper: пример локального Docker-стека](https://github.com/lakekeeper/lakekeeper/blob/main/examples/minimal/docker-compose.yaml).
- [PyIceberg: Arrow и DuckDB](https://py.iceberg.apache.org/api/).

'''
text=text.replace('\u2014','-').replace('\u2013','-')
text=text.split('\n## Приложение А.')[0].rstrip()+'\n'
text=text.replace(
    'Отчёт самостоятельный: результаты, SQL, конфигурация, команды и исходники для повторения приведены внутри этого MD. Для чтения и оценки не требуются соседние файлы или изображения.',
    'Отчёт содержит описание развёртывания, решения по слоям, методологию, результаты измерений, проверки корректности и выводы.')
text=text.replace(
    'Версии и digest образов встроены в приложение. Именованные Docker volumes сохраняют объекты S3 и состояние PostgreSQL между перезапусками.',
    'Версии реально использованных сервисов указаны в таблице. Именованные Docker volumes сохраняют объекты S3 и состояние PostgreSQL между перезапусками.')
text=text.replace(
    'Команда развёртывания: `docker compose up -d --build`. Полный Compose и конфигурация Trino приведены в приложении, а подготовка бакета / warehouse выполняется встроенным скриптом `bootstrap.py`.',
    '''### Команды развёртывания и выполнения

Стек развёрнут через Docker Compose. S3-хранилище и PostgreSQL используют именованные volumes. Lakekeeper запускается после миграции базы; Trino обращается к REST-каталогу и S3 по именам сервисов внутри Docker-сети.

```powershell
docker compose up -d --build
# Запуск хранилища, PostgreSQL, Lakekeeper, Trino и контейнера инструментов.
docker compose exec -T tools python -u scripts/bootstrap.py
# Создание бакета lakehouse и warehouse в Lakekeeper.
docker compose exec -T tools python -u scripts/benchmark.py --run-id main
docker compose exec -T tools python -u scripts/benchmark.py --run-id repeat1
# Две независимые серии измерений для форматов и кодеков.
docker compose exec -T tools python -u scripts/pipeline.py
# Загрузка Bronze, построение Silver и Gold, проверки качества.
docker compose exec -T tools python -u scripts/review_experiments.py --run-id review1
# Дополнительная запись, партиционирование, сравнение слоёв и sf3.
docker compose exec -T tools python -u scripts/duckdb_demo.py
# Независимая проверка Bronze через PyIceberg и DuckDB.
```

Это команды фактически выполненной работы. При повторении используются новые идентификаторы измерений, чтобы сохранить предыдущие результаты.''')
text=text.replace(
    'Точный query ID каждого измерения и его статистики встроены в приложение. Новые данные не подставлены вместо старых: две серии сравниваются явно.',
    'Каждое измерение сохранялось с уникальным query ID и статистикой Trino. В отчёте две серии показаны отдельно, чтобы оценить повторяемость результатов.')
text=text.replace(
    'Поэтому прежние формулировки о преимуществе Parquet NONE над ZSTD по полной агрегации и о преимуществе ORC NONE над Parquet NONE по Q3 заменены на нейтральное описание измерений.',
    'Различия Parquet NONE / ZSTD по полной агрегации и ORC NONE / Parquet NONE по Q3 не удовлетворяют этому правилу, поэтому установленное преимущество по времени не заявляется.')
text=text.replace(
    'Порядок строк при параллельном поступлении может различаться и немного менять упаковку файлов; во встроенных первичных результатах сохранены реальные размеры каждого повторения.',
    'Порядок строк при параллельном поступлении может различаться и немного менять упаковку файлов. Это объясняет небольшое расхождение размеров при одинаковом логическом наборе данных.')
text=text.replace('После ALTER все 30 519 старые строки читают новое поле как NULL.',
                  'После ALTER новое поле читается как NULL для всех 30 519 ранее записанных строк.')
text=text.replace('Iceberg - формат таблицы со схемой, снапшотами и ссылками на файлы;',
                  '[Iceberg](https://trino.io/docs/current/connector/iceberg.html) - формат таблицы со схемой, снапшотами и ссылками на файлы;')
compose_block=('### Конфигурация развёртывания\n\n'
               'Ниже приведён фактически использованный `docker-compose.yml`. Он фиксирует образы, '
               'настройки сервисов, зависимости запуска, volumes, порты и ограничения ресурсов. '
               'Каталог `config/trino` содержит настройки Trino и коннекторов, описанные в разделах 2 и 5.1; '
               'контейнер `tools` собирается по Dockerfile из раздела 6.2.\n\n'
               '```yaml\n'+(ROOT/'docker-compose.yml').read_text(encoding='utf-8-sig').strip()+'\n```\n\n')
text=text.replace('### Команды развёртывания и выполнения',compose_block+'### Команды развёртывания и выполнения')
text=text.replace(
    'Выбран DuckDB в контейнере `tools`. Это движок SQL над Arrow после чтения Bronze клиентом PyIceberg, без нативного Iceberg-сканирования DuckDB.',
    'Выбран DuckDB в контейнере `tools`. По классификации задания это **SQL-движок**, а не BI-инструмент или дата-каталог. '
    'Он выполняет SQL над Arrow после чтения Bronze клиентом PyIceberg, без нативного Iceberg-сканирования DuckDB. '
    'Для разовой проверки sf1 серверные компоненты [Doris](https://doris.apache.org/docs/dev/features-architecture/system-architecture/) '
    'или [StarRocks](https://docs.starrocks.io/docs/deployment/) потребовали бы дополнительной настройки и сопровождения; '
    'DuckDB работает внутри процесса Python. Поэтому для данной локальной задачи распределённый серверный стек избыточен; '
    'преимущество DuckDB по скорости над этими системами не заявляется.')
tools_block=('Образ `tools` собирается через `build: ./tools` в Compose. Фактически использованный '
             '`tools/Dockerfile` фиксирует версии библиотек для доступа к S3, чтения Iceberg / Arrow, '
             'аналитического SQL и проверки форматов:\n\n```dockerfile\n'
             +(ROOT/'tools/Dockerfile').read_text(encoding='utf-8-sig').strip()+'\n```\n\n')
text=text.replace('Действия в консоли: `docker compose exec -T tools python -u scripts/duckdb_demo.py`.',
                  tools_block+'Действия в консоли: `docker compose exec -T tools python -u scripts/duckdb_demo.py`.')
assert len(text.splitlines())<=800
assert 'Приложение' not in text and 'приложении' not in text and 'встроены в приложение' not in text
# All links point to primary web documentation; no adjacent evidence files required.
links=re.findall(r'\]\(([^)]+)\)',text)
assert all(link.startswith(('https://','http://','#')) for link in links),links
assert len(re.findall(r'^## [1-7]\.',text,re.M))==7
assert text.count('<details>')==text.count('</details>')
assert text.count('```')%2==0
(ROOT/'отчет.md').write_text(text,encoding='utf-8')
print('Standalone report:',ROOT/'отчет.md',len(text.encode('utf-8')),'bytes')
