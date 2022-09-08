import os
import json
import time
import numpy as np
import pandas as pd
import networkx as nx
from datetime import datetime
from functools import wraps
from common.log_utils import get_logger

logger = get_logger(__name__)


def local_time_format():
    local_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
    return local_time


def now_string():
    now_time_string = datetime.now().strftime('%Y-%m-%d %H-%M-%S')
    return now_time_string


def local_date_format():
    local_date = time.strftime('%Y%m%d', time.localtime(time.time()))
    return local_date


def open_object_general(data_dir, dict_name):
    """打开json文件"""
    with open(os.path.join(data_dir, dict_name + '.json'), 'r', encoding='utf-8') as f:
        data = json.loads(f.read())
    return data


def df2excel(df, output_dir, file_name):
    writer = pd.ExcelWriter(os.path.join(output_dir, file_name + '.xlsx'))
    df.to_excel(writer)
    writer.save()


def save_object_general(event_graph_dict, data_dir, dict_name):
    with open(os.path.join(data_dir, dict_name + '.json'), 'w', encoding='utf-8') as f:
        f.write(json.dumps(event_graph_dict, sort_keys=True, indent=2, cls=NpEncoder,
                           separators=(',', ': '), ensure_ascii=False))


def dict_union(ori_dict, nested_dict_list):
    """执行多个字典的合并"""
    for nested_dict in nested_dict_list:
        ori_dict = dict(ori_dict, **nested_dict)
    return ori_dict


class NpEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, np.integer):
            return int(obj)
        elif isinstance(obj, np.floating):
            return float(obj)
        elif isinstance(obj, np.ndarray):
            return obj.tolist()
        else:
            return super(NpEncoder, self).default(obj)


def get_time_diff_expr(diff_minutes):
    """
    :param diff_minutes: 两个时间的分钟整数差
    :return: 转化为X年X月X日 X时X分的中文描述
    """
    h, m = divmod(diff_minutes, 60)
    d, h = divmod(h, 24)
    M, d = divmod(d, 30)
    Y, M = divmod(M, 12)
    time_diff_expr = '{}年{}月{}日 {}时{}分'.format(Y, M, d, h, m)
    return time_diff_expr


def flatten(li):
    """
    将子列表元素展开
    :param li:
    :return:
    """
    return sum(([x] if not isinstance(x, list) else flatten(x) for x in li), [])


def get_time_diff(df, col_time):
    """
    用所有数据的当前时间减去上一次时间
    :return:
    """
    df[col_time] = pd.to_datetime(df[col_time])
    time_series = df[col_time]
    choices = time_series.shift(0) - time_series.shift(1)
    choices_new = [item.seconds for item in list(choices) if item is not None]
    conditions = [True]
    df['time_diff'] = np.select(choicelist=[choices_new], condlist=conditions, default=0)
    df['time_diff'].fillna(0, inplace=True)
    return df


def to_excel_with_mul_sheets(output_dir, save_name, **kwargs):
    with pd.ExcelWriter(os.path.join(output_dir, save_name)) as writer:
        for sheet, df in kwargs.items():
            df.to_excel(writer, sheet_name=sheet, index=False)
        writer.close()


def open_graph(open_dir, fname):
    G = nx.Graph()
    json_file = open_object_general(open_dir, fname)
    G.add_nodes_from(json_file['nodes'])
    G.add_edges_from(json_file['edges'])
    return G


def make_dir_if_not_exists(input_dir):
    """
    判断一个文件夹是否存在，如不存在，则创建该文件夹
    :return:
    """
    if os.path.exists(input_dir):
        pass
    else:
        os.makedirs(input_dir)


def make_dir_if_not_exists_and_del(f_input_dir, s_input_dir):
    """
    每次只保留新的目录和文件   删除其他所有旧的
    父目录  f_input_dir
    子目录  s_input_dir
    删除当前文件下的所有文件夹和文件
    :return:
    """
    # 删除
    for root, dirs, files in os.walk(f_input_dir, topdown=False):
        for name in files:
            os.remove(os.path.join(root, name))
        for name in dirs:
            os.rmdir(os.path.join(root, name))
    # 在创建
    os.makedirs(s_input_dir)
    return


def judge_dir_exists(input_dir):
    """
    判断一个文件夹是否存在，如不存在，返回0
    :return:
    """
    if os.path.exists(input_dir):
        return 1
    return 0


def fn_timer(function):
    @wraps(function)
    def function_timer(*args, **kwargs):
        t0 = time.time()
        result = function(*args, **kwargs)
        t1 = time.time()
        logger.info("Total time running %s: %s seconds" %
              (function.__name__, str(t1 - t0))
              )
        return result

    return function_timer
