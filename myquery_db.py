import pyodbc
import pandas as pd
import numpy as np
import psycopg2
from db_account import db as db_basic





def connect2sqlserver(db=db_basic['sqlserver']['ksdata'], db_default=''):
    '''
    db_default: database to connect
    
    '''
    
   
    if db_default !='':
        cnxn_str = f'''
        DRIVER={db['driver']};
        SERVER={db['server']};
        DATABASE={db_default};
        UID={db['username']};
        PWD={db['password']};
        '''
    else:
        cnxn_str = f'''
        DRIVER={db['driver']};
        SERVER={db['server']};
        DATABASE={db['database']};
        UID={db['username']};
        PWD={db['password']};
        '''


    try:
        cnxn = pyodbc.connect(cnxn_str)
        cursor = cnxn.cursor()  # 创建一个游标对象，用于执行 SQL 查询
        print("Successfuly connected to SQL Server!")
        return cnxn, cursor

    # 在这里可以执行 SQL 查询等操作

    except pyodbc.Error as ex:
        sqlstate = ex.args[0]
        print(f"Connection error: {sqlstate}")
        print(ex)
    
    return None, None


def connect2postgresssql(db=db_basic['postgresssql']['ems_ks']):


    try:
        
        conn = psycopg2.connect(host=db['host'], 
                                database=db['database'], 
                                user=db['username'], 
                                password=db['password'], 
                                port=db['port'])

        cursor = conn.cursor()
        print("Successfuly connected to SQL Server!")
        return conn, cursor

    # 在这里可以执行 SQL 查询等操作

    except pyodbc.Error as ex:
        sqlstate = ex.args[0]
        print(f"Connection error: {sqlstate}")
        print(ex)
    
    return None, None

def query_sqlserver(query, db='ksdata', db_default=''):
    cnxn, cursor = connect2sqlserver(db=db_basic['sqlserver'][db], db_default=db_default)
    if not cnxn:
        return pd.DataFrame()
    
    # execute query
    cursor.execute(query)
    rows = cursor.fetchall()
    if not rows:
        return pd.DataFrame()
    rows = np.array(rows)

    columns = [column[0] for column in cursor.description]

   
    df = pd.DataFrame(rows, columns=columns)

    if cnxn:
        cursor.close()
        cnxn.close()
        print("colsoe db connection")
    
    return df


def latest_time_sqlserver(table, col_time, col_constrain:dict={}, db='ksdata', db_default='ks_project_yyk'):
    cnxn, cursor = connect2sqlserver(db=db_basic['sqlserver'][db], db_default=db_default)
    if not cnxn:
        return pd.DataFrame()
    
    # execute query
    query = f'''
    select max({col_time}) from {table} where 1=1
    '''
  

    if col_constrain:
        query_insert_con = ''
        for cc in col_constrain:
            con_value = col_constrain[cc]
            if isinstance(con_value,str):
                query_insert_con += f''' and {cc}='{con_value}' '''
            else:
                query_insert_con += f''' and {cc}={con_value} '''
 
        query += query_insert_con
    print(query)

    cursor.execute(query)
    rows = cursor.fetchall()
 
    if (not rows) or (not rows[0]) or (not rows[0][0]):
        return ''
    #rows = np.array(rows)
    result = rows[0][0].strftime('%Y-%m-%d %H:%M:%S')

    return result



def chunk_list(l:list, check_size:int=500):
    re_list = []
    i = 1
    while i <=len(l):
        re_list.append(l[i-1:i-1+check_size])
        i += check_size
    return re_list


def chunk_date(start_time:str, end_time:str, freq:str='D'):
    date_range = pd.date_range(start=start_time, end=end_time, freq=freq)

    result = list(set([start_time]+list(str(i) for i in date_range) + [end_time]))
    result.sort() # inplace change
    return result



def query_postgresserver(query, db='ems_ks'):
    conn, cursor = connect2postgresssql(db=db_basic['postgresssql'][db])
    if not conn:
        return pd.DataFrame()
    
    # execute query
    cursor.execute(query)
    rows = cursor.fetchall()
    rows = np.array(rows)

    columns = [column[0] for column in cursor.description]

    df = pd.DataFrame(rows, columns=columns)

    if conn:
        cursor.close()
        conn.close()
        print("colsoe db connection")
    
    return df

def query_ems(db, query):
    if db=='ems_ks':
        df = query_postgresserver(query=query, db=db)
    elif df=='ems_gz':
        df = query_sqlserver(query=query, db='ems_gz')
    return df

def query_ksdata(query, db='ksdata', db_default=''):
    df = query_sqlserver(query=query, db=db, db_default=db_default)

    return df


def write_ksdata_append(df, col, table_name='',schema_name='ods',table_database='ks_project_yyk', db='ksdata'):
    '''
    df: dataframe to write
    col: columns to write
    '''

    if df.empty:
        print("DataFrame 为空，没有数据需要写入。")
        return
    
    cnxn, cursor = connect2sqlserver(db=db_basic['sqlserver'][db], db_default=table_database)

    # 获取列名
    #columns = list(dataframe.columns)
    # 为了安全，对列名进行处理，防止注入
    #sanitized_columns = str(col)[1:-1]
    sanitized_columns = ', '.join([f"[{c.replace(']', '').replace('[', '')}]" for c in col])

    placeholders = ', '.join(['?'] * len(col))  # 生成 (?, ?, ?) 这样的占位符
    insert_query = f"INSERT INTO {table_name} ({sanitized_columns}) VALUES ({placeholders})"

    # 将 DataFrame 转换为元组列表
    #important:
    df = df.replace(np.nan, None) # odbc 插入时无法识别nan, 换成None
    values_to_insert = [tuple(row) for row in df[col].values]
    cursor.fast_executemany = True
    #try:
        #for v_sub in chunk(values_to_insert, 500):
        # 4. 使用 executemany 批量插入数据
    print(f'insert_query: {insert_query}\nvalues_to_insert:{values_to_insert}')
    
    cursor.executemany(insert_query, values_to_insert)
    cnxn.commit()
    print(f"Successfully inster data in {table_name}: rows {len(df)}")

    #except Exception as e:
    #    print(f"写入 SQL Server 数据库时发生错误: {e}\ninsert_query:{insert_query}\nvalues_to_insert：{values_to_insert}")
    #finally:
        # 5. 关闭数据库连接
    if cnxn:
        cursor.close()
        cnxn.close()



def write_ksdata_updateorignore_duiplicate(df:pd.DataFrame, unique_key_column:list,col_update:list, table_name:str='',col_insert_rest:list=[], unique_method:str='update',schema_name:str='ods',table_database:str='ks_project_yyk', db:str='ksdata'):
    '''
    df: dataframe to write
    unique_key_column: unique key
    unique_key_column:col_update: columns to update when unique exist
    unique_method:  update col_update or not, ['update', 'ignore']
    '''

    if df.empty:
        print("DataFrame 为空，没有数据需要写入。")
        return
    
    cnxn, cursor = connect2sqlserver(db=db_basic['sqlserver'][db], db_default=table_database)
    col_update_special = [f'[{i}]' for i in col_update]
    col_insert_rest_special = [f'[{i}]' for i in col_insert_rest]
    unique_key_column_special = [f'[{i}]' for i in unique_key_column]
    col_total = unique_key_column_special+col_update_special+col_insert_rest_special

    #query_unique_key = ','.join([f'? AS {i}'  for i in unique_key_column_special])
    query_source = ','.join([f'? AS {i}' for i in col_total])
    query_col = ','.join(col_total)
    query_set = ','.join([f'target.{i}=source.{i}' for i in col_update_special])
    query_col_insert = ','.join([f'source.{i}' for i in col_total])
    #query_col_insert_unique_key = ','.join([f'source.{i}' for i in unique_key_column_special])

    merge_conditon =  ' and '.join([f'target.{i}=source.{i}' for i in unique_key_column_special])

    
    query = f'''MERGE {table_name} AS target
                USING (SELECT {query_source}) AS source
                ON ({merge_conditon})
                WHEN MATCHED THEN
                    UPDATE SET {query_set}
                WHEN NOT MATCHED THEN
                    INSERT ({query_col})
                    VALUES ({query_col_insert});
            '''
    # remove when matched then
    if unique_method=='ignore':
            query = f'''MERGE {table_name} AS target
                        USING (SELECT {query_source}) AS source
                        ON ({merge_conditon})
                        WHEN NOT MATCHED THEN
                            INSERT ({query_col})
                            VALUES ({query_col_insert});
                    '''
           
    print(query)

    # 将 DataFrame 转换为元组列表
    df = df.replace(np.nan, None) # odbc 插入时无法识别nan, 换成None
    values_to_insert = [tuple(i) for i in df[unique_key_column+col_update+col_insert_rest].values]
    cursor.fast_executemany = True
    #try:
        #for v_sub in chunk(values_to_insert, 500):
        # 4. 使用 executemany 批量插入数据
    cursor.executemany(query, values_to_insert)
    cnxn.commit()
    print(f"Successfully inster data in {table_name}: rows {len(df)}")

    #except Exception as e:
    #    print(f"写入 SQL Server 数据库时发生错误: {e}\ninsert_query:{query}\nvalues_to_insert：{values_to_insert}")
    #finally:
        # 5. 关闭数据库连接
    if cnxn:
        cursor.close()
        cnxn.close()


    #use case
    #query_db.write_ksdata_update_duiplicate(df=df_test, unique_key_column='test_items', col_update=['spec_center'], table_name='spc_meta_test_parameters', unique_method='update', schema_name='ods')

def get_tasks(task_title='SPC', table='ks_project_yyk.my_task_table.task_spc_isra'):
    query = f'''
    select *
    from {table}
    where 1=1
    and status='PENDING'
    and task_title='{task_title}'
    '''
    print(query)
    df_tasks = query_ksdata(query)
    return df_tasks



    