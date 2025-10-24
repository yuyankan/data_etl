key_columns_map= {'en':{
                    'Flaw #': 'flaw_id',
                    'Flaw Type': 'flaw_type',
                    'Flaw Length': 'length',
                    'Flaw Width': 'width',
                    'Flaw Area': 'area',
                    'Flaw Downweb Position': 'downweb_position_(m)',
                    'Flaw Crossweb Position': 'crossweb_position_(mm)',
                    'Flaw image filename': 'image_url'
                },
                'ch': {
                    '缺陷#': 'flaw_id',
                    '缺陷类型': 'flaw_type',
                    '缺陷长度': 'length',
                    '缺陷宽度': 'width',
                    '缺陷面积': 'area',
                    '缺陷纵向位置': 'downweb_position_(m)',
                    '缺陷宽方向位置': 'crossweb_position_(mm)',
                    '缺陷图象文件名': 'image_url'
                }
}
refresh_freq = 1 #1min

non_flaw_types = ['Start Inspection', 'Stop Inspection', 'Face Splice'] # used for filteration
columns_meta = ['production_line','product','roll_number','reportdate','line_speed_(m/min)','roll_length_(m)','roll_width_(mm)','lot','flaw_number']
columns_meta_uniquekey = ['production_line','product','roll_number']
columns_meta_2write = list(set(columns_meta).difference(columns_meta_uniquekey))


columns_result_uniquekey = ['productid','flaw_id']
columnse_result_2write_all = ['flaw_id', 'flaw_type', 'length', 'width', 'area',
                    'downweb_position_(m)', 'crossweb_position_(mm)', 'productid'
                    ] #'image_url',
columns_result_2write = list(set(columnse_result_2write_all).difference(columns_result_uniquekey))
#: country-productionline
address_dict ={ 
    'K1':r'\\147.121.160.30\data\K1',
    'C4':r'\\10.161.18.24\e\Data',
    'C6':r'\\10.161.19.24\g\Data'
           }



address_dict_real2mirror = { 
    # 必须使用原始字符串 (r'') 来确保反斜杠被正确地作为字面字符处理
    r'\\147.121.160.30\\data\\K1': '/data_mount/K1',
    r'\\147.121.160.30\data\K1': '/data_mount/K1',
    r'\\10.161.18.24\e\Data': '/data_mount/C4',
    r'\\10.161.19.24\g\Data': '/data_mount/C6',
}


tables_isra = {
    'result':'ks_project_yyk.ISRA_report.isra_result_detail',
    'meta':'ks_project_yyk.ISRA_report.isra_result_meta',
    'task_day':'ks_project_yyk.my_task_table.task_isra_day_level_search',
    'task_roll':'ks_project_yyk.my_task_table.task_isra_roll_level_search'
}

meta_name_mapping = {'报告日期':'reportdate',
                        'Report Date':'reportdate',
                        'Line Speed': 'line_speed_(m/min)',
                        'Line Speed (m/min)':'line_speed_(m/min)'
                        }