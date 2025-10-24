import pandas as pd
import myquery_db as query_db
import etl_config as ec
import multiprocessing
from datetime import datetime, timedelta
import etl_fun2call as efc
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed


tables_isra = ec.tables_isra

#table to read raw data
address_dict = ec.address_dict

refresh_freq = ec.refresh_freq

# global varinat:
col_unique_day= ['task_title','production_line','task_day']
col_update_day = ['status']
col_insert_rest_day = ['createtime','latest_time']
last_roll_dict = {}
col_unique_roll = ['task_title', 'production_line', 'task_day','roll_number']
col_update_roll =['status']
col_insert_rest_roll = ['createtime', 'last_roll','file_path']


def create_task_day(day_pre=1):
    production_lines = list(ec.address_dict.keys())
    date_range_list = []
    task_title='ISRA'
    global last_roll_dict

    for production_line in production_lines:

        region = 'korean' if production_line=='K1' else 'cn'
        curren_time = efc.get_current_date(region=region)#.strftime('%Y-%m-%d')
        if production_line=='C6':
            last_time,last_roll_temp = 0,0
            
        else:
            last_time,last_roll_temp = efc.last_time_roll(production_line=production_line)
            last_roll_dict[production_line] = last_roll_temp
        #===================================================================
        #for MANUAL injected data!
        #last_time = '2025-03-01'
        #last_roll = 0
        #last_time = datetime.strptime(last_time, '%Y-%m-%d')
        #curren_time = '2025-09-17'
        #curren_time = datetime.strptime(curren_time, '%Y-%m-%d')
        #===================================================================
        print(f'production_line{production_line} last_time{last_time}, last_roll{last_roll_temp}')
        

        if last_time == 0:
            #===================================================================
            #for MANUAL injected data!
            #last_time = '2025-09-16'
            #last_time = datetime.strptime(last_time, '%Y-%m-%d')
            last_time = curren_time - timedelta(days=day_pre)

        time_diff = int((pd.to_datetime(curren_time)-pd.to_datetime(last_time)).total_seconds())
        if time_diff < (refresh_freq*60):
            print(f'time_diff is {time_diff},latest_time-{last_time},time_now-{curren_time}, do not need to refresh')
            continue


        print(last_time,curren_time)
        curren_time_next = curren_time + timedelta(days=1)
        date_range_list_temp = efc.mydate_range(start=last_time, end=curren_time_next, freq='D') # day rolls_last
        print(date_range_list_temp)

        df_temp = pd.DataFrame(date_range_list_temp,
                columns=['task_day']
            )
        df_temp['production_line'] = production_line
        df_temp['task_title'] = task_title
        df_temp['createtime'] = curren_time
        df_temp['status'] = 'PENDING'
        df_temp['latest_time'] = pd.to_datetime(last_time).strftime('%Y-%m-%d %H:%M:%S')
        date_range_list.append(df_temp)
    
    if len(date_range_list)==0:
        return

  

    df_task = pd.concat(date_range_list)
    df_task['createtime'] = pd.to_datetime(df_task['createtime']).dt.strftime('%Y-%m-%d %H:%M:%S')
    df_task['latest_time'] = pd.to_datetime(df_task['latest_time']).dt.strftime('%Y-%m-%d %H:%M:%S')

    # get task already registered:
    query_task_day_registered = f'''
        select distinct production_line, task_day
        from {ec.tables_isra['task_day']}
        where 1=1
        and status <> 'TODAY'
    '''
    df_task_day_registered_already = query_db.query_ksdata(query_task_day_registered)

    #df_task_new = df_task.merge(df_task_day_registered_already, on=['production_line','task_day'])
    # 1. Perform a left merge
    df_merged = df_task.merge(
        df_task_day_registered_already[['production_line', 'task_day']],
        on=['production_line', 'task_day'],
        how='left',
        indicator=True  # Add a column to show the source of the row
    )

    # 2. Filter for rows that only exist in the left DataFrame (df_task_new)
    df_new_tasks_to_register = df_merged[df_merged['_merge'] == 'left_only'].drop(columns=['_merge'])
    print('df_new_tasks_to_register: ',df_new_tasks_to_register.head())
    if df_new_tasks_to_register.empty:
        print('df_new_tasks_to_register is empty')
        return

    #write task to table:
    query_db.write_ksdata_updateorignore_duiplicate(df=df_new_tasks_to_register,
                                                    unique_key_column=col_unique_day,
                                                    col_update=col_update_day,
                                                    col_insert_rest = [],
                                                    table_name=ec.tables_isra['task_day']
                                                    #unique_method='ignore'
                                                    )
    print('new increased DAY_task: ', len(df_new_tasks_to_register))




def work_get_files_2read(date_task,folder_root,production_line,last_roll,folder_root_mirror,user_server_mirror=False):
    
    region = 'korean' if production_line=='K1' else 'cn'
    curren_time = efc.get_current_date(region=region)#.strftime('%Y-%m-%d')
    folder_root_2read = folder_root
    date_task_read = date_task
    if user_server_mirror:
        folder_root_2read = folder_root_mirror
        date_task_read = date_task.replace('\\','/')

    file_path_list_temp = efc.get_files_2read(range_list0=[1,date_task_read],folder_root=folder_root_2read, last_roll=0)
   

    if len(file_path_list_temp)<1:
        print(f'No new ROLL files found for production_line-{production_line}, day_task-{date_task}')
        update_task_day_status(production_line=production_line,date_task=date_task,curren_time=curren_time, task_day_status='EMPTY')
        return
    if user_server_mirror:
        df_file_path_list_temp = pd.DataFrame(file_path_list_temp, columns=['file_path_mirror'])
        df_file_path_list_temp = path_mirror2real(df_file_path_list_temp)
    else:
        df_file_path_list_temp = pd.DataFrame(file_path_list_temp, columns=['file_path'])


   
    df_file_path_list_temp['roll_number'] = df_file_path_list_temp['file_path'].str.split('\\').str[-1].str.extract(r'^(\d+)', expand=False)

    
    
    # get roll numbers already in the task table:
    roll_number_exist = []
    query = f'''
    select distinct roll_number
    from ks_project_yyk.my_task_table.task_isra_roll_level_search
    where 1=1
    and production_line = '{production_line}'
    and task_day = '{date_task}'
    '''
    df_roll_number_exist = query_db.query_ksdata(query)
    if not df_roll_number_exist.empty:
        roll_number_exist = df_roll_number_exist['roll_number'].tolist()
    
    df_file_path_list_temp_new = df_file_path_list_temp[~df_file_path_list_temp['roll_number'].isin(roll_number_exist)]
    
    if df_file_path_list_temp_new.empty:
        print(f'No new ROLL files to add for production_line-{production_line}, day_task-{date_task}')
        update_task_day_status(production_line=production_line,date_task=date_task,curren_time=curren_time,task_day_status='DONE')
        return
    
    # add other columns:
    df_file_path_list_temp_new['production_line'] = production_line
    df_file_path_list_temp_new['task_day'] = date_task
    df_file_path_list_temp_new['last_roll'] = last_roll
    df_file_path_list_temp_new['task_title'] = 'ISRA'
    df_file_path_list_temp_new['status'] = 'PENDING'

    
    
    df_file_path_list_temp_new['createtime']=curren_time.strftime('%Y-%m-%d %H:%M:%S')
    #df_file_path_list_temp_new['createtime']= df_file_path_list_temp_new['createtime'].dt.strftime('%Y-%m-%d %H:%M:%S')


    # write into table:
    global col_unique_roll #= ['task_title', 'production_line', 'task_day','roll_number']
    global col_update_roll #=['status']
    global col_insert_rest_roll #= ['createtime', 'last_roll','file_path']

    query_db.write_ksdata_updateorignore_duiplicate(df=df_file_path_list_temp_new,
                                                unique_key_column=col_unique_roll,
                                                col_update=col_update_roll,
                                                col_insert_rest = col_insert_rest_roll,
                                                table_name=ec.tables_isra['task_roll'],
                                                unique_method='ignore'
                                                )
    print('new increased task: ', len(df_file_path_list_temp_new))
    update_task_day_status(production_line=production_line,date_task=date_task,curren_time=curren_time,task_day_status='DONE')


def update_task_day_status(production_line,date_task,curren_time,task_day_status='DONE'):
    #update task_table for day:
    df_date_task_finished = pd.DataFrame([[production_line,date_task]], 
                                         columns=['production_line','task_day']
                                         ) 
    #task_day_status = 'DONE'
    curren_time_next = curren_time + timedelta(days=1)
    if date_task in [curren_time.strftime(r'%Y\%b\%d'), curren_time_next.strftime(r'%Y\%b\%d')]:
        task_day_status = 'TODAY'
    
    df_date_task_finished['status'] = task_day_status
    df_date_task_finished['task_title'] = 'ISRA'
    df_date_task_finished['updatetime'] = curren_time.strftime('%Y-%m-%d %H:%M:%S')
    query_db.write_ksdata_updateorignore_duiplicate(df=df_date_task_finished,
                                        unique_key_column=col_unique_day,
                                        col_update=col_update_day+['updatetime'],
                                        table_name=ec.tables_isra['task_day']
                                        )
    print(f'status updated:,production_line:{production_line}, date_task:{date_task},task_day_status:{task_day_status}')


def update_task_roll_status(production_line,roll_number,date_task,task_roll_status='DONE'):
    #update task_table for day:
    region = 'korean' if production_line=='K1' else 'cn'
    curren_time = efc.get_current_date(region=region)#.strftime('%Y-%m-%d')

    df_roll_task_finished = pd.DataFrame([[production_line,date_task,roll_number]], 
                                         columns=['production_line','task_day','roll_number']
                                         ) 
    #task_day_status = 'DONE'
    df_roll_task_finished['task_title'] = 'ISRA'
    df_roll_task_finished['status'] = task_roll_status
    df_roll_task_finished['updatetime'] = curren_time.strftime('%Y-%m-%d %H:%M:%S')
    
 
    query_db.write_ksdata_updateorignore_duiplicate(df=df_roll_task_finished,
                                        unique_key_column=col_unique_roll,
                                        col_update=col_update_roll+['updatetime'],
                                        table_name=ec.tables_isra['task_roll']
                                        )
    print(f'status updated:,production_line:{production_line}, date_task:{task_roll_status},task_roll_status:{task_roll_status}')

def create_task_roll_signal_thread():
    query_task_day = f'''
    select d.production_line,d.task_day
    from {ec.tables_isra['task_day']} d
    --left join {ec.tables_isra['task_roll']} r on r.production_line=d.production_line and r.task_day=d.task_day
    where 1=1
    and d.status <> 'DONE'
    and d.status <> 'EMPTY'
    and d.task_title='ISRA'
    --group by d.production_line,d.task_day
    '''
    df_task_day_pending = query_db.query_ksdata(query=query_task_day)
    if df_task_day_pending.empty:
        print('No pending DAY_task found!')
        return
    
    print('df_task_day_pending',df_task_day_pending)
    df_task_day_pending = df_task_day_pending.drop_duplicates()


    # read roll file path
    #file_path_list = []
    for a in tqdm(df_task_day_pending[['task_day','production_line']].values):
        print('a:',a)
        date_task = a[0]
        production_line = a[1]
        folder_root = ec.address_dict[production_line]

        work_get_files_2read(date_task=date_task,folder_root=folder_root,production_line=production_line,last_roll=0)
        
def run_task_work_get_files_2read(params):
    """一个包装函数，用于线程池执行实际工作"""
    date_task = params['date_task']
    production_line = params['production_line']
    print(f"Processing Task: {production_line} on {date_task}")
    
    # 调用原有的工作函数
    # work_get_files_2read 的返回值将成为 f.result() 的返回值
    return work_get_files_2read(
        date_task=date_task,
        folder_root=params['folder_root'],
        folder_root_mirror=params['folder_root_mirror'],
        user_server_mirror=params['user_server_mirror'],
        production_line=production_line,
        last_roll=params['last_roll']
    )




def create_task_roll_multi_thread(user_server_mirror=False):
    query_task_day = f'''
    select d.production_line,d.task_day
    from {ec.tables_isra['task_day']} d
    --left join {ec.tables_isra['task_roll']} r on r.production_line=d.production_line and r.task_day=d.task_day
    where 1=1
    and d.status <> 'DONE'
    and d.status <> 'EMPTY'
    and d.task_title='ISRA'
    --group by d.production_line,d.task_day
    '''
    df_task_day_pending = query_db.query_ksdata(query=query_task_day)
    if df_task_day_pending.empty:
        print('No pending DAY_task found!')
        return
    
    print('df_task_day_pending',df_task_day_pending)
    df_task_day_pending = df_task_day_pending.drop_duplicates()


    # read roll file path
    #file_path_list = []
    # 1. 准备任务参数列表
    # 将 DataFrame 的每一行转换为一个字典或元组作为任务参数
    task_parameters = []
    for index, row in df_task_day_pending[['task_day', 'production_line']].iterrows():
        production_line = row['production_line']
        folder_root = ec.address_dict.get(production_line, None) # 使用 .get() 避免 key error
        folder_root_mirror = ec.address_dict_real2mirror.get(folder_root, None)
        
        if folder_root is not None:
            task_parameters.append({
                'date_task': row['task_day'],
                'folder_root': folder_root,
                'production_line': production_line,
                'last_roll': 0,
                'folder_root_mirror':folder_root_mirror,
                'user_server_mirror':user_server_mirror

            })

    # 2. 定义执行任务的包装函数

    # 3. 多线程执行
    results = []
    # 线程数设置：如果任务是 I/O 密集型，可以设置得高一些，如 8, 16, 或 32。
    # 如果你想实现 '单核-单thread'，就设为 1，但这样就失去了并发加速的目的。
    MAX_WORKERS = 8 

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        # 提交所有任务到线程池
        futures = [executor.submit(run_task_work_get_files_2read, params) for params in task_parameters]
        
        # 迭代已完成的任务并显示进度
        for f in tqdm(as_completed(futures), total=len(futures), desc="Processing day tasks"):
            try:
                # 获取任务结果
                result = f.result()
                results.append(result)
            except Exception as e:
                print(f"Task failed for a day: {e}")

    print("All tasks finished.")

def path_real2mirror(df0):
    df = df0.copy()
    if df.empty:
        print('df is empty')
        return
    df['file_path_mirror'] = df['file_path'].copy()
    for p, mp in ec.address_dict_real2mirror.items():
    # 关键：显式设置 regex=False 进行字面量替换
        df['file_path_mirror'] = \
            df['file_path_mirror'].str.replace(p, mp, regex=False)#.str.replace('\\','/')
    df['file_path_mirror'] = df['file_path_mirror'].str.replace('\\','/')
    return df

def path_mirror2real(df0):
    df = df0.copy()
    if df.empty:
        print('df is empty')
        return
    df['file_path'] = df['file_path_mirror'].copy()
    for p, mp in ec.address_dict_real2mirror.items():
    # 关键：显式设置 regex=False 进行字面量替换
        df['file_path'] = \
            df['file_path'].str.replace(mp, p, regex=False)#.str.replace('\\','/')
    df['file_path'] = df['file_path'].str.replace('/','\\')
    return df


def get_roll_tasks():
    query_get_roll_task = f'''
    select production_line, file_path, roll_number, task_title, task_day
    from {ec.tables_isra['task_roll']}
    where 1=1
    and status in ('PENDING','FAILED')
    and task_title = 'ISRA'
    '''
    df_get_roll_task = query_db.query_ksdata(query_get_roll_task)

    #get mirror path in linux server
    df_get_roll_task_mirror = path_real2mirror(df_get_roll_task)
    return df_get_roll_task_mirror


def work_create_task(user_server_mirror=False):
    # create task_day table
    create_task_day()

    # create_task_roll: multithread
    create_task_roll_multi_thread(user_server_mirror=user_server_mirror)


def update_task_roll_status_based_on_result_table():
    ''' for special case : need to manually update roll task table status
    '''
    query_roll_result = f'''
    select r.production_line, r.roll_number, r.task_day
    from {efc.tables_isra['task_roll']} r
    join {efc.tables_isra['meta']} m on r.production_line=m.production_line and r.roll_number=m.roll_number
    where 1=1
    and r.status = 'PENDING'
    '''
    df_roll_finished_task_open = query_db.query_ksdata(query_roll_result)
    if df_roll_finished_task_open.empty:
        print('NO missed roll status updating, all updated')
        return
    region = 'cn'
    curren_time = efc.get_current_date(region=region)#.strftime('%Y-%m-%d')

   
    #task_day_status = 'DONE'
    df_roll_finished_task_open['task_title'] = 'ISRA'
    df_roll_finished_task_open['status'] = 'DONE'
    df_roll_finished_task_open['updatetime'] = curren_time.strftime('%Y-%m-%d %H:%M:%S')
    df_roll_finished_task_open = df_roll_finished_task_open.drop_duplicates()
    
 
    query_db.write_ksdata_updateorignore_duiplicate(df=df_roll_finished_task_open,
                                        unique_key_column=col_unique_roll,
                                        col_update=col_update_roll+['updatetime'],
                                        table_name=ec.tables_isra['task_roll']
                                        )
    print(f'ROLL task status updated:, Manually, NUMBER: {len(df_roll_finished_task_open)}' , df_roll_finished_task_open.head())




if __name__ == '__main__':
    work_create_task()