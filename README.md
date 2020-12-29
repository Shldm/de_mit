# de_mit
 Код, про который рассказывал на конференции
 
1. Открываете тетрадку
2. Заполняете конфиг со своим сервером
3. Создаете любую таблицу c наименование test_table
4. Запускаете код ниже, смотрите как перелались таблицаи дальше все будет понятно :)

 
dst_tbl_name = "test_table_copy"
sql_inc = ""SELECT * FROM test_table"
dq_conn = pipline_get_connect('con_home')

piplince_exec(engine_src=dq_conn, engine_dst=dq_conn, dst_tbl_name, sql_inc, sql_del= None, cnt_pool=1)
