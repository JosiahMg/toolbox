# -*- coding: utf-8 -*-
# Author: Mh
# Date: 2022/9/6 11:11
# Function: 操作PG库的sql语句
from common.log_utils import get_logger

logger = get_logger(__name__)


class GetDataFromPG:
    def __init__(self, params):
        self.project_id = params['topology_id']
        self.node_tb_name = params['node_tb_name']
        self.pipe_tb_name = params['pipe_tb_name']
        self.field_name = params['field_name']

    def get_node_by_project_id(self):
        sql = """select gid, dno, name, type, st_x(geom), st_y(geom) from {} where {} = '{}' """.format(
            self.node_tb_name, self.field_name, self.project_id)
        logger.info('pg sql: {}'.format(sql))
        return sql

    def get_pipe_by_project_id(self):
        sql = """select gid, pipematerial, pipelength, pipediam, pressurerating, wallthickness, source, target, ST_AsText(geom) from {} where {} = '{}' """.format(
            self.pipe_tb_name, self.field_name, self.project_id)
        logger.info('pg sql: {}'.format(sql))
        return sql
