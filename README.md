# de_mit
 Код, про который рассказывал на конференции
 
 Для деста досттаочно в тетардке пайтон, запустить код ниже, для переливки таблицы из базы  
 
 dst_tbl_name = "test_table"
sql_del = "delete from [dbo].[wh_lights_SPR_Сотрудники]"
sql_inc = "":"
dq_conn = pipline_get_connect('con_home')
piplince_exec(engine_src=dq_conn, engine_dst=dq_conn, dst_tbl_name, sql_inc, sql_del= None, cnt_pool=1)
