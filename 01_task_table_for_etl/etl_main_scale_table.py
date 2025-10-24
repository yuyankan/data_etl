from etl_config import key_columns_map,non_flaw_types, address_dict
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
import etl_fun2call as efc
import os
from tqdm import tqdm
#import myquery_db as query_db
import concurrent.futures
from functools import partial
import isra_task_table as itt

# 定义一个新函数，用于处理单个文件的解析和保存
def process_roll_task(params):
    """
    单个线程执行的任务逻辑：解析数据、保存数据和更新状态。
    
    Args:
        params (dict): 包含任务所需参数的字典。
        efc: 用于保存数据的对象或接口。
        parse_data_func (callable): 用于解析数据的函数。
        update_status_func (callable): 用于更新任务状态的函数。
        
    Returns:
        str: 任务执行的摘要信息。
    """
    pl = params['production_line']
    fp = params['file_path']
    rn = params['roll_number']
    td = params['task_day']
    fp_mirror = params['file_path_mirror']
    user_server_mirror = params['user_server_mirror']
    
    fp_2read = fp
    if user_server_mirror:
        fp_2read = fp_mirror
    
    flaws_df, finish_status = efc.parse_data(fp_2read)

    
    # 核心逻辑：解析数据
    
    
    # 根据解析结果决定保存数据
    if finish_status == 'DONE':
        # 假设 efc.save_data 接受 [DataFrame], production_line, file_path
        efc.save_data([flaws_df], pl, file_path=fp)
    
    # 更新任务状态
    # 假设 update_status_func 接受 production_line, roll_number, task_day, status
    itt.update_task_roll_status(pl, rn, td, task_roll_status=finish_status)
    
    return f"Roll {rn} on {td} ({pl}) finished with status: {finish_status}"
        # save


def run_roll_tasks_multithreaded(user_server_mirror=False,max_workers=16):
    """
    使用多线程并发处理一批 Roll 任务。

    Args:
        df_get_roll_task (pd.DataFrame): 包含待处理 Roll 任务信息的 DataFrame。
        efc_handler: 用于保存数据的对象或接口。
        parse_data_func (callable): 用于解析数据的函数 (parse_data)。
        update_status_func (callable): 用于更新任务状态的函数 (update_task_roll_status)。
        max_workers (int): 线程池的最大工作线程数。

    Returns:
        list: 所有任务的执行结果摘要列表。
    """
    # 1. 准备任务参数列表
    df_get_roll_task = itt.get_roll_tasks()
    if df_get_roll_task.empty:
        print('no new tasks')
        return
    task_parameters = []
    required_cols = ['production_line', 'file_path','roll_number', 'task_title', 'task_day', 'file_path_mirror']
    
    # 检查必要的列是否存在
    if not all(col in df_get_roll_task.columns for col in required_cols):
        raise ValueError(f"DataFrame 缺少必需的列: {required_cols}")


    # 将每一行数据封装成字典
    for row in df_get_roll_task[required_cols].values:
        task_parameters.append({
            'production_line': row[0],
            'file_path': row[1],
            'roll_number': row[2],
            'task_title': row[3],
            'task_day': row[4],
            'file_path_mirror':row[5],
            'user_server_mirror':user_server_mirror
        })

    # 2. 多线程执行和进度条显示
    results = []
    total_tasks = len(task_parameters)
    print(f"Starting {total_tasks} roll tasks with {max_workers} threads...")

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # 提交所有任务。将依赖作为 extra arguments 传入 process_roll_task
        futures = [
            executor.submit(process_roll_task, params)
            for params in task_parameters
        ]
        
        # 使用 tqdm 包装 as_completed 来实时显示完成进度
        for f in tqdm(as_completed(futures), total=total_tasks, desc="Processing Roll Tasks"):
            try:
                # 获取任务结果
                result_message = f.result()
                results.append(result_message)
            except Exception as e:
                # 捕获并打印任务执行中发生的任何异常
                print(f"\n[ERROR] A roll task failed: {e}")
                
    print("All tasks finished.")
    return results




def work_create_task_only():
        # 构造任务列表
    print('----构造任务列表----')
    itt.work_create_task()

def work_parse_task_only(user_server_mirror=False):
    #deal with task
    run_roll_tasks_multithreaded(user_server_mirror=user_server_mirror)

def work_all(user_server_mirror=False):
        # 构造任务列表
    print('----构造任务列表----')
    itt.work_create_task(user_server_mirror=user_server_mirror)

    #deal with task
    run_roll_tasks_multithreaded(user_server_mirror=user_server_mirror)



    

if __name__ =='__main__':
    work_parse_task_only(user_server_mirror=False)
    #work_all(user_server_mirror=False)

    
