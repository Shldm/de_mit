import sqlalchemy
import datetime as dt
from sqlalchemy import create_engine
import multiprocessing as mp
import pandas as pd
import os
import fnmatch
import difflib
import numpy as np
import teradatasqlalchemy
import json
import time

CONFIG_CON = r'config.json'


## 1. Функции подключения к данным
def pipline_get_connect(src):
    """Получение строки соединения в контексте SqlAlchemy.

    Args:
        src: имя подключения:
            MSSQL:
                con_home - Подключение к серверу

    """

    conf = pd.read_json(CONFIG_CON)
    engine_url = conf[src]['str']

    try:
        cr_eng = create_engine(engine_url)
        cr_eng.connect()
    except Exception as err:
        piplince_log('CONNECT_ERROR', 4, src, 0, 0)
    return cr_eng

    return create_engine(engine_url)


## 2. Функции запускающие и проводящие ETL процессы


def piplince_exec(engine_src, engine_dst, dst_tbl_name, sql_inc, sql_del=None, mssql=None, log=None, cnt_pool=None):
    """Запуск конвейера ETL.

    Функция запускает ETL пакет с заданными параметрами и автоматизирует разработку логирования и добавления обязательных атрибутов.

    Args:
        engine_src: Строка подключения к источнику. Необходимо получать из конфигурации проекта.
        engine_dst: Строка подключения к бд назачения. Необходимо получать из конфигурации проекта.
        dst_tbl_name: Наименование таблицы в базе назначения, в которую необходимо загрузить данные.
        sql_inc: sql скрипт в источнике для выборки данных.
        sql_del: sql скрипт для очистки данных в назначении, под загрузку инкремента
        log: если утановить "0", запуск не попадет в логирование
        cnt_pool: если = 1, запускает ETL без ассинхронного режима в одном потоке
        mssql: скрипт, который отрабатывает после загрузки данных в целевую базу

    """
    try:
        if log != 0:
            dt_start = dt.datetime.now()
            piplince_log('INFO', 1, dst_tbl_name, 0, 0)

        if sql_del != None: engine_dst.execute(sql_del)

        if cnt_pool == 1: process_frame(pd.read_sql(sql_inc, engine_src), dst_tbl_name, engine_dst.url)
        if cnt_pool == None:

            reader = pd.read_sql(sql_inc, engine_src, chunksize=50000)

            pool = mp.Pool(8, maxtasksperchild=8)
            funclist = []
            for df in reader:
                f = pool.apply_async(process_frame,
                                     kwds={'df': df,
                                           'dst_tbl_name': dst_tbl_name,
                                           'engine_dst_': engine_dst.url
                                           })
                funclist.append(f)

            for f in funclist: f.get(timeout=1500)
            pool.terminate()

        if mssql != None: engine_dst.execute(mssql)
        if log != 0: piplince_log('INFO', 2, dst_tbl_name, 0, (dt.datetime.now() - dt_start).total_seconds())

    except Exception as err:
        if log != 0: piplince_log('ERROR', 3, dst_tbl_name, 0, 0)


def process_frame(df, dst_tbl_name, engine_dst_):
    """Загрузка данных в ассинхронном режиме.

    Функция автоматизирует загрузку DF, добавляя

    Args:
        df: DataFrame для загрузки.
        dst_tbl_name: Наименование таблицы в которую необходимо грузить данные.
        engine_dst: Строка подключения типа sqlAlchemy базы приемника.

    """
    engine_dst = create_engine(engine_dst_)

    df = pipline_must_column(df)
    df.to_sql(dst_tbl_name,
              engine_dst,
              schema='dbo',
              if_exists='append',
              index=False,
              dtype=pipline_convert_column(df))
    df = None


def piplince_exec_procedure_sql(engine_src, sql_text):
    """Запуск sql скрипта на сервере БД.

    Функция запускает sql скрипт на сервере.

    Args:
        engine_src: Строка подключения к источнику. Необходимо получать из конфигурации проекта.
        sql_text: sql скрипт

    """
    engine_src.execute(sql_text)



## 3. Функции проверок и логирования

def piplince_log(type_mes, step_log, name_object, qty=0, step_time=0):
    """Процедура логирования ETl
    Логи сохраняются на сервере приемнике данных, в таблицу dq_etl

    Args:
        type_mes: 'INFO', 'ERROR', 'CHECK'
        step_log: от 1 до 4 (шаг процесса ETL). 1 - начало, 2 - завершение, 3 - ошибка, 4 - ошибка подключения к БД
        dst_tbl_name: таблица в БД, в которую загружаем данные
        qty: количество строк или чанков (при ассинхронной загрузке)
        step_time: время выполнения пакета, записывается в step_log = 2

    """
    mes = ''
    if step_log == 1 and type_mes == 'INFO':
        mes = 'Start ETL: ' + name_object
    elif step_log == 2 and type_mes == 'INFO':
        mes = 'End ETL: ' + name_object
    elif step_log == 3 and type_mes == 'ERROR':
        mes = 'ERROR ETL: ' + name_object
    elif step_log == 4 and type_mes == 'CONNECT_ERROR':
        mes = 'ERROR CONNECT: ' + name_object

    engine_dst = pipline_get_connect('con_home')
    engine_dst.execute("""INSERT INTO dq_etl VALUES(current_timestamp, ?, ?, ?, ?, ?, ?, ?)""",
                       type_mes,  # Тип логирования
                       step_log,  # Шаг логирования
                       name_object,  # Наименование процесса
                       mes,  # Сообщение
                       '',  # Дата актуальности данных
                       qty,  # К-во строк в загрузке
                       step_time)  # Время выполнения


def pipline_get_sql_quality(table_name, date_name, list_column):
    """Процедура получения результатов проверки данных
    Логи сохраняются на сервере приемнике данных, в таблицу dq_etl

    Args:
        table_name: имя таблицы в БД для проверки
        date_name: имя колонки с датой в таблице, по которой проводится проверки актуальности и полноты
        list_column: список колонок с ключевыми атрибутами по которым проверяется консистентность данных

    """

    dq_conn = pipline_get_connect('con_home')
    q = f"""

    select {list_column} 
    from {table_name}
    where cast({date_name} as Date) = (select max(cast({date_name} as Date)) from {table_name})

    """

    q2 = f"""
            SELECT 
            table_name as [Витрина],
            min_ as [Данные с],
            max_ as [Данные по],
            CASE
            WHEN DATEDIFF(day,min_,  max_) + 1 - count_day_ = 0 THEN N'Ок'
            ELSE N'Error'
            END as [Проверка на наличие всех дат],
            rn as [Всего строк в таблице],
            MIN(rn/count_day_) as [Среднее к-во записей за день],
            COUNT({date_name}) as [К-во строк в последней загрузке],
            CASE WHEN COUNT({date_name}) < MIN(rn/count_day_)/2 THEN  N'Error' 
            ELSE N'Ок' END as [Проверка на к-во загруженных строк]

            FROM 
            (
            SELECT 
            '{table_name}' as table_name,
            MIN({date_name}) as min_,
            MAX({date_name}) as max_,
            COUNT(DISTINCT CAST({date_name} as DATE)) as count_day_,
            COUNT(*) as rn

            FROM {table_name}
            ) as tb_check

            LEFT JOIN {table_name} as tb_det
            ON CAST(max_ as DATE) = CAST({date_name} AS DATE)

            GROUP BY 
            table_name,
            min_,
            max_,
            CASE
            WHEN DATEDIFF(day,min_,  max_) + 1 - count_day_ = 0 THEN N'Ок'
            ELSE N'Error'
            END,
            rn
    """
    q3 = f"""

    select max(cnt_row) as cnt_row_max, 
    min(cnt_row) as cnt_row_min
    from  
    (select cast({date_name} as Date) as {date_name} , count (*) as cnt_row
     from  {table_name}
     group by cast({date_name} as Date )) as tb


    """

    df = pd.read_sql(q, dq_conn)
    df2 = pd.DataFrame(df.describe()).T.drop(['top', 'freq'], axis=1)
    df2['all'] = (df2['count'][df2.index == date_name]).values[0]
    df2['rate_cnt'] = (df2['count'] / df2['all'] * 100).astype(int).astype(str) + '%'
    df2['rate_unique'] = ((df2['unique'] / df2['all']) * 100).astype(int).astype(str) + '%'
    df2 = df2.drop(['all', 'count', 'unique'], axis=1)

    df2 = pd.DataFrame({col: [', '.join(df2.index.astype(str) + ' - ' + df2[col].astype(str))] for col in df2.columns})
    df3 = pd.read_sql(q2, dq_conn)
    df33 = pd.read_sql(q3, dq_conn)
    df4 = df3.join(df2)
    df4 = df4.join(df33)
    df4 = df4.rename(columns={'rate_cnt': 'Процент непустых строк',
                              # 'freq':'Частота',
                              # 'top' : 'Топ значение',
                              'rate_unique': 'Процент уникальных строк',
                              'cnt_row_max': 'Максимальное к-во строк в периоде',
                              'cnt_row_min': 'Минимальное к-во строк в периоде'})

    return df4


def pipline_get_status(dst_tbl_name):
    """Получение статуса по таблице
        2 - таблица обновилась сегодня

    """
     
    engine_dst = pipline_get_connect('con_home')
    sql_inc = f"""
            SELECT step_log FROM
            (
            SELECT 
            row_number() over (partition by name_object order by date_loading desc) as rn,
            step_log
            FROM dq_etl
            WHERE  name_object LIKE N'%{dst_tbl_name}%'
            AND date_loading >= CAST(GETDATE() as DATE)
            ) as tb_part
            WHERE rn = 1
        """

    df = pd.read_sql(sql_inc, engine_dst)
    if len(df) > 0: status = df.step_log.values[0]
    else: status = 0

    return status




## 4. Вспомогательные функции работы с данными

def pipline_must_column(df_inc):
    """ Добавления в DataFrame обязательных колонок, при их отсутствии в выборке

    Returns:
        DataFrame c заполненными колонками: 'date_loading', 'date_start', 'date_end', 'date_relevance'

    """
    df_inc = df_inc.drop('date_loading', axis=1, errors='ignore')
    df_inc.insert(0, 'date_loading', dt.datetime.today())

    list_col = df_inc.columns.tolist()

    if 'date_start' not in list_col:
        df_inc.insert(1, 'date_start', (dt.date.today() - pd.offsets.YearBegin() * 120).date())

    if 'date_end' not in list_col:
        df_inc.insert(2, 'date_end', dt.date.today())

    if 'date_relevance' not in list_col:
        df_inc.insert(2, 'date_relevance', dt.date.today())

    return df_inc


def pipline_convert_column(dfparam):
    """ Конвертирование колонок DataFrame к потребностям БД назначения """
    dtypedict = {}
    list_col_date = ['date_start', 'date_end', 'date_relevance']

    for i, j in zip(dfparam.columns, dfparam.dtypes):
        if i in list_col_date:
            dtypedict.update({i: sqlalchemy.types.DATE()})
        elif "object" in str(j):
            dtypedict.update({i: sqlalchemy.types.NVARCHAR(length=255)})
        elif "datetime" in str(j):
            dtypedict.update({i: sqlalchemy.types.DateTime()})
        elif "date" in str(j):
            dtypedict.update({i: sqlalchemy.types.DATE()})
        elif "float" in str(j):
            dtypedict.update({i: sqlalchemy.types.Float(precision=3, asdecimal=True)})
        elif "int" in str(j):
            dtypedict.update({i: sqlalchemy.types.INT()})
        else:
            dtypedict.update({i: sqlalchemy.types.NVARCHAR(length=255)})
    return dtypedict


def piplince_text_similarity(s1, s2):
    """Получение процента совпадения наименования 2ух строк

    Args:
        s1: Первая строка
        s2: Вторая строка

    """
    matcher = difflib.SequenceMatcher(None, s1.lower(), s2.lower())

    return matcher.ratio()
