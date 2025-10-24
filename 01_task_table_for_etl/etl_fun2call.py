

import datetime
import pytz
import myquery_db as query_db
from etl_config import meta_name_mapping,tables_isra,key_columns_map, columns_meta, columns_meta_uniquekey,columns_meta_2write,columns_result_uniquekey,columns_result_2write,non_flaw_types
import pandas as pd
import os
import myquery_db as query_db
import io
from tenacity import retry, stop_after_attempt, wait_fixed, retry_if_exception_type
import re

STATE_FILE = 'STATE_FILE.txt'




# 假设 `requests` 或其他库是用来从远程下载文件的，这里仅作演示
# 实际远程文件读取操作可能在 `open` 内部或之前发生
# 比如使用 `smbprotocol` 等库来访问远程共享文件
# 假设 `open` 可能会抛出 FileNotFoundError 或其他 I/O 错误
# retry_if_exception_type 能够指定我们想重试哪种异常

# 定义我们想要重试的异常类型，例如 IOError, FileNotFoundError 等
# 如果你使用特定的远程文件访问库，请检查它抛出的异常类型
# 比如 smbprotocol.exceptions.SMBException, paramiko.SSHException 等
RETRY_EXCEPTIONS = (IOError, FileNotFoundError, OSError) 

# `@retry` 装饰器会帮助我们处理重试逻辑
@retry(
    stop=stop_after_attempt(5), # 最多重试3次
    wait=wait_fixed(10),         # 每次重试等待5秒
    retry=retry_if_exception_type(RETRY_EXCEPTIONS) # 仅在指定异常时重试
)
def get_lines(dft_file_path):
    """
    尝试从远程读取文件，并在失败时重试。
    """
    print(f"尝试读取文件: {dft_file_path}")
    lines = []
    try:
        with open(dft_file_path, 'r', encoding='utf-8') as f:
            lines = f.readlines()
            print("以 'utf-8' 编码成功读取。")
    except UnicodeDecodeError:
        try:
            with open(dft_file_path, 'r', encoding='gbk') as f:
                lines = f.readlines()
                print("以 'gbk' 编码成功读取。")
        except Exception as e:
            # 如果 'gbk' 也失败了，抛出异常，让 retry 装饰器捕获并重试
            # 如果你不想重试编码问题，可以将这一段异常处理放到 @retry 之外
            raise RuntimeError(f"使用 'utf-8' 和 'gbk' 编码读取文件失败: {e}")
    except Exception as e:
        # 抛出异常，让 @retry 捕获并重试
        # 这就是捕捉远程连接中断错误的关键
        raise IOError(f"读取文件时发生 I/O 错误: {e}")

    return lines

def save_data_meta(df_flaw):
    #df_flaw.to_csv('df_flaw.csv', index=False)


    for c in columns_meta:
        # for roll_lengh:
        if c=='Roll length':
            continue
        if c not in df_flaw.columns:
            print(f'{c} not in df_flaw:', df_flaw.head())
            return pd.DataFrame()
        
    df_meta = df_flaw[columns_meta].drop_duplicates()

    #write to table
    query_db.write_ksdata_updateorignore_duiplicate(df=df_meta, 
                                                unique_key_column=columns_meta_uniquekey,
                                                col_update=columns_meta_2write,
                                                table_name= tables_isra['meta'],
                             )
    
    # get new id:s
    df_meta_all = query_db.query_ksdata(f'select * from {tables_isra['meta']}')
    df_meta_all.rename(columns={'id':'productid'}, inplace=True)
    df_meta[columns_meta_uniquekey] = df_meta[columns_meta_uniquekey].astype(str)
    df_meta_all[columns_meta_uniquekey] = df_meta_all[columns_meta_uniquekey].astype(str)
    df_meta_id = df_meta.merge(df_meta_all[columns_meta_uniquekey+['productid']], on=columns_meta_uniquekey, how='left')


    #print('df_meta',df_meta[columns_meta_uniquekey], df_meta.dtypes)
    ##print('df_meta_all',df_meta_all[columns_meta_uniquekey],df_meta_all.dtypes)
    #print('df_meta_id',df_meta_id)
    #print('8'*20)
    # check if there is no:
    conda_na = df_meta_id['productid'].isna()
    df_na = df_meta_id[conda_na]
    df_notna = df_meta_id[~conda_na]
    if not df_na.empty:
        print('product id is none', df_na)
        print('df_meta',df_meta[columns_meta_uniquekey], df_meta.dtypes)
        print('df_meta_all',df_meta_all[columns_meta_uniquekey],df_meta_all.dtypes)
        print('df_meta_id',df_meta_id)

    return df_notna[columns_meta_uniquekey+['productid']]


def save_data(df_result_list, production_line,file_path=''):


    if len(df_result_list)<1:
        print('no data!!!', production_line, file_path)
        return
    df_result = pd.concat(df_result_list)
    df_result['production_line'] = production_line
    #df_result['country'] = country

    #df_meta_id
    df_meta_id = save_data_meta(df_flaw=df_result)
    df_result[columns_meta_uniquekey] = df_result[columns_meta_uniquekey].astype(str)
    df_result_merge = df_result.merge(df_meta_id,on=columns_meta_uniquekey, how='left')
    df_result_merge = df_result_merge.drop_duplicates()
    check_non =df_result_merge['productid'].isna()
    df_result_merge[check_non].to_csv('noon.csv', index=False)
    #df_meta_id.to_csv('df_meta_id.csv')
    #df_result.to_csv('df_result.csv')


    query_db.write_ksdata_updateorignore_duiplicate(df=df_result_merge, 
                                            unique_key_column=columns_result_uniquekey,
                                            col_update=columns_result_2write,
                                            table_name= tables_isra['result'],
                            )
    print('add data: ', production_line,file_path, len(df_result_merge))

    



def get_lines0(dft_file_path):
    lines = []
    try:
        with open(dft_file_path, 'r', encoding='utf-8') as f:
            lines = f.readlines()
    except UnicodeDecodeError:
        try:
            with open(dft_file_path, 'r', encoding='gbk') as f:
                lines = f.readlines()
        except Exception as e:
            print(f"警告: 使用 'utf-8' 和 'gbk' 编码读取文件 {dft_file_path} 时均失败: {e}")
          
        
    except Exception as e:
        print(f"警告: 读取文件 {dft_file_path} 时发生未知错误: {e}")
    
    return lines

def find_header_bottom_line_index(lines):
    '''
    # 通过遍历文件的每一行，查找中英文格式中独特的表头字符串来确定文件语言。
    # 这是最可靠的语言判断方式，因为它直接基于数据结构本身。
    '''
    header_line_index = len(lines)-1
    file_language = None
    for i, line in enumerate(lines):
        if line.strip().startswith('Flaw #,Flaw Type'):
            file_language = 'en'
            header_line_index = i
            break
        elif line.strip().startswith('缺陷#,缺陷类型'):
            file_language = 'ch'
            header_line_index = i
            break
    
    bottom_line_index = len(lines)

    for j in range(header_line_index+1, len(lines)):
        if ('Roll length' in lines[j]) or ('Number of flaws' in lines[j]):
            bottom_line_index = j
            break
    
    return header_line_index, file_language,bottom_line_index



def read_line_df(lines, header_line_index,bottom_line_index):
    # --- 4. 加载并解析核心 CSV 数据 ---
    # 从表头行开始，将文件的剩余部分视为一个 CSV 格式的字符串，并读入 DataFrame。
    # 使用 io.StringIO 可以让 pandas 直接读取内存中的字符串，避免了创建临时文件。
    # low_memory=False 用于防止因列类型推断不一致而产生的 DtypeWarning。
    csv_content = "".join(lines[header_line_index:bottom_line_index])# last 2 lines is roll lengh, number of flowas
    
    df = pd.read_csv(io.StringIO(csv_content), low_memory=False)
    return df


def last_time_roll(production_line):
    query = f'''select top 1 reportdate, roll_number
                from
                {tables_isra['meta']}
                where 1=1
                --and country='country'
                and production_line ='{production_line}'
                order by reportdate desc
    '''
    print('query for roll', query)
    df_re = query_db.query_ksdata(query=query)
    result = 0,0
    if not df_re.empty:
        result = df_re.values[0]
    return result

def get_meta(lines, header_line_index,bottom_line_index):
    meta_lines = lines[:header_line_index] +lines[bottom_line_index:]
    meta_string_map = {}

    for line in meta_lines:
        # 找到第一个冒号，并将其作为分隔符
        if ':' in line:
            key, value = line.split(':', 1)
            
            # 去除键和值两端的多余空格和制表符
            key = key.strip()
            value = value.strip()
            
            # 将键值对存入字典
            meta_string_map[key] = value

    # 打印结果
    #print(meta_string_map)
    return meta_string_map

def clean_flaw(df,active_column_map):
        # 使用选定的映射字典，将列名重命名为标准格式。
    df.rename(columns=active_column_map, inplace=True)

    # .copy() 用于避免后续操作中出现 SettingWithCopyWarning。
    df['flaw_id'] = pd.to_numeric(df['flaw_id'], errors='coerce') # 'coerce'会将无法转换的值变为NaT
    flaws_df = df[(~df['flaw_type'].isin(non_flaw_types)) & (df['flaw_id'] > 0)].copy()

    return flaws_df


def parse_data(file_path):
    '''
    RETURN: file_path status: empty: or invalid
    '''
    flaws_df = pd.DataFrame()
    lines = get_lines(file_path)
    result_return = [flaws_df,'FAILED'] # need to redo again
    # 如果文件内容为空，则直接返回
    if len(lines)<1:
        print(f"警告: 文件 {file_path} 为空，已跳过。")
        result_return[1] = 'EMPTY'
        return result_return


    # --- 2. 定位数据表头并确认语言 ---
   
    header_line_index, file_language,bottom_line_index =find_header_bottom_line_index(lines)

    # 如果循环结束后仍未找到任何一个表头，则无法解析文件。
    if file_language is None:
        print(f"警告: 无法在文件 {file_path} 中找到有效的数据表头，已跳过。")
        result_return[1] = 'INVALID'
        return result_return
    
    # --- 3. 定义标准化列名映射 ---
    df = read_line_df(lines=lines, header_line_index=header_line_index,bottom_line_index=bottom_line_index)
    active_column_map = key_columns_map[file_language]
    df = df[list(active_column_map.keys())]

    flaws_df = clean_flaw(df=df,active_column_map=active_column_map)

    #check if empty:
    if (flaws_df.empty) or (len(flaws_df)<1):
        result_return[1] = 'NO_FLAW'
        print('flaws_df is empty')
        return result_return

    # get meta
    meta_info_dict = get_meta(lines, header_line_index,bottom_line_index)
    print('meta_info_dict',meta_info_dict)

    flaws_df['product'] = meta_info_dict['Product']
    flaws_df['roll_number'] = meta_info_dict['Roll Number']
    
    flaws_df['roll_width_(mm)'] = meta_info_dict['Width']
    flaws_df['lot'] = meta_info_dict['Lot']

 
    try:
        flaws_df['roll_length_(m)'] = meta_info_dict['Roll length']
        flaws_df['flaw_number'] = meta_info_dict['Number of flaws']
    except Exception as e:
    #flaws_df['recipe'] = meta_info_dict['Recipe']
        print('ERROR: ',e)

    
    for ls in meta_name_mapping:
        if ls in meta_info_dict:
            flaws_df[meta_name_mapping[ls]] = meta_info_dict[ls]

    #
    flaws_df['flaw_id'] = flaws_df['flaw_id'].astype(int)
    flaws_df['reportdate'] = pd.to_datetime(flaws_df['reportdate'],format='%B %d,%Y %I:%M:%S %p')


 
    result_return = [flaws_df,'DONE']

    return result_return


    # clen







def save_last_checked_timestamp(curren_time,roll_num_max):
    """将新的时间戳保存到状态文件中。"""
    with open(STATE_FILE, "w") as f:
        f.write(str(curren_time) + "\n")  # 写入时间戳并换行
        f.write(str(roll_num_max) + "\n") # 写入 roll_num_max 并换行



def extract_roll_number(filename):
    """
    使用正则表达式从文件名中提取 '月' 前面的数字。
    例如：'57873Apr01.dft' -> 57873
    """
    # 匹配一个或多个数字，后面跟着一个大写字母和两个小写字母
    pattern = r'\d+(?=[A-Z][a-z]{2})'
    match = re.search(pattern, filename)
    if match:
        return int(match.group(0))
    return None


# for oneday only
@retry(
    stop=stop_after_attempt(5), # 最多重试3次
    wait=wait_fixed(10),         # 每次重试等待5秒
    retry=retry_if_exception_type(RETRY_EXCEPTIONS) # 仅在指定异常时重试
)
def get_files_2read(range_list0, folder_root,last_roll):
    roll_path_list = []
    file_path_list= []
    roll_num_max = int(last_roll) # make str
    task_index, day2search = range_list0

    folder_temp = os.path.join(folder_root, day2search)
    print(f"当前处理文件夹: {folder_temp}")
    
    try:
        if(task_index!=0):
        # 递归遍历目录树
            for dirpath, dirnames, filenames in os.walk(folder_temp):
                for filename in filenames:
                    if filename.lower().endswith('.dft'):
                        full_path = os.path.join(dirpath, filename)
                        file_path_list.append(full_path)
        else:
            for dirpath, dirnames, filenames in os.walk(folder_temp):
                for filename in filenames:
                    if filename.lower().endswith('.dft'):
                        full_path = os.path.join(dirpath, filename)
                        # 尝试处理文件，如果出现错误则跳过
                        try:    
                            roll_number = extract_roll_number(filename)
                            if roll_number is not None and roll_number > roll_num_max:
                                file_path_list.append(full_path)
                            else:
                                file_path_list.append(full_path)
                        except Exception as e:
                            # 捕捉处理单个文件时的异常，例如文件被删除
                            print(f"警告: 处理文件 '{full_path}' 时发生错误: {e}")
                            continue # 跳过当前文件，继续下一个

    except FileNotFoundError:
        # 捕捉整个文件夹不存在的异常
        print(f"警告: 文件夹 '{folder_temp}' 不存在或无法访问，已跳过。")
        return # 跳过当前文件夹，进入下一个 day_rest

    except PermissionError:
        # 捕捉访问权限不足的异常
        print(f"警告: 无法访问文件夹 '{folder_temp}'，权限不足。已跳过。")
        return # 跳过当前文件夹，进入下一个 day_rest

    except Exception as e:
        # 捕捉其他未知的 os.walk 异常
        print(f"警告: 遍历文件夹 '{folder_temp}' 时发生未知错误: {e}")
        return # 跳过当前文件夹，进入下一个 day_rest

    return file_path_list
    #return roll_path_list, file_path_list, roll_num_max

def get_files_2read_common(range_list, folder_root,last_roll):
    roll_path_list = []
    file_path_list= []
    roll_num_max = str(last_roll) # make str

    #for rest:
    for i, day_rest in  enumerate(range_list):
        foler_temp = os.path.join(folder_root,day_rest) # roll numbers
        
        #check if there is this file:
        if not os.path.exists(foler_temp):
            print(f'folder {foler_temp} not exist!!!')
            continue

        rolls = os.listdir(foler_temp)

        if len(rolls)<1:
            continue
        if i==0:
            rolls = [r for r in rolls if str(r)>=roll_num_max] # rewrite last roll
        
        
        for r in rolls:
            file_temp = f'{r}{day_rest.replace('\\','')[4:]}.dft' # dft file
            roll_path = os.path.join(foler_temp,r)
            roll_path_list.append(roll_path)
            file_path_list.append(os.path.join(roll_path,file_temp))
        
        # get max roll number
        #roll_num_max = max(roll_num_max, max(rolls))

    return file_path_list
    #return roll_path_list, file_path_list, roll_num_max
        

def mydate_range(start, end, freq='D'):
    print(start, end)

    range_list = pd.date_range(start,end, freq=freq)
    print(range_list)
    #range_list = [start] + range_list+ [end]
    range_list = list(set(range_list.strftime(r'%Y\%b\%d'))) # 2025\Sep\12
    #add c6 history honly
    #range_list = list(set(range_list.strftime(r'%y\%m\%d'))) # 25\09\12
    
    range_list.sort()

    return range_list



def get_last_checked_timestamp():
    """
    从状态文件中读取上次检查的时间戳和最大的 roll_num。
    """
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE, "r") as f:
            lines = f.readlines()
            if len(lines) >= 2:
                try:
                    timestamp = float(lines[0].strip())
                    roll_num_max = int(lines[1].strip())
                    return timestamp, roll_num_max
                except (ValueError, IOError):
                    # 文件内容无效，返回默认值
                    return 0.0, 0
    # 文件不存在或内容不足，返回默认值
    return 0.0, 0



def get_current_date(region='korean'):
    """
    获取当前韩国标准时间并格式化为 YYYY\Mon\DD 格式。
    """
    # 定义韩国时区
    region_mapping = {'korean':'Asia/Seoul','cn':'Asia/Shanghai'}
    korea_tz = pytz.timezone(region_mapping[region])
    
    # 获取当前韩国时间
    current_time = datetime.datetime.now(korea_tz)
    current_time = current_time.replace(tzinfo=None)

    
    return current_time


def get_image_url(df1_flaw, add_root):
    import pandas as pd

    # 1. Convert the 'reportdate' column to datetime only once
    df1_flaw['reportdate_dt'] = pd.to_datetime(df1_flaw['reportdate'])

    # 2. Extract and store the formatted strings in temporary variables to avoid repeated calls
    year_str = df1_flaw['reportdate_dt'].dt.strftime('%Y')
    month_str = df1_flaw['reportdate_dt'].dt.strftime('%B')
    day_str = df1_flaw['reportdate_dt'].dt.strftime('%d')

    # 3. Use .astype(str) once for roll_number and flaw_id
    roll_number_str = df1_flaw['roll_number'].astype(str)
    flaw_id_str = df1_flaw['flaw_id'].astype(str)

    # 4. Use `df.apply` or f-strings for efficient string concatenation
    df1_flaw['image_url'] = (
        add_root + year_str + '\\' + month_str + '\\' + day_str + '\\' + roll_number_str + 
        '\\Images\\Images001' + roll_number_str + day_str + '-' + flaw_id_str +'.BMP'
    )
    