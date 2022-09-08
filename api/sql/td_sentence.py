# -*- coding: utf-8 -*-
# Author: Mh
# Date: 2022/6/25 15:31
# Function:
from common.log_utils import get_logger

logger = get_logger(__name__)


class TDengineSentence:
    @staticmethod
    def get_pv_by_pid(pid, timeformat):
        sql = """select * from pd_{} where ts>='{}' and ts<'{}'""".format(pid, timeformat[0], timeformat[1])
        logger.info('get_pv_by_pid[sql]: {}'.format(sql))
        return sql

    @staticmethod
    def get_pv_by_pids(pids, timeformat, type='standard'):
        if type == 'tbname':
            pids_str = [f"pd_{pid}" for pid in pids]
            pids_str = tuple(pids_str)
            sql = """select * from pipeline_data where tbname in {} and ts>='{}' and ts<'{}'""".format(pids_str, timeformat[0], timeformat[1])
        elif type == 'standard':
            pids_tp = tuple(pids)
            sql = """select * from pipeline_data where position_id in {} and ts>='{}' and ts<'{}'""".format(pids_tp, timeformat[0], timeformat[1])
        else:
            raise NotImplemented('please use tbname or standard')
        logger.info('get_pv_by_pid[sql]: {}'.format(sql))
        return sql

    @staticmethod
    def show_super_tables():
        sql = """show stable""".format()
        logger.info('show_super_tables[sql]: {}'.format(sql))
        return sql

    @staticmethod
    def show_sub_tables(stable):
        sql = """select tbname from {}""".format(stable)
        logger.info('show_super_tables[sql]: {}'.format(sql))
        return sql

