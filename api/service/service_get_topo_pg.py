# -*- coding: utf-8 -*-
# Author: Mh
# Date: 2022/9/8 10:29
# Function:
from common.log_utils import get_logger
from app.get_topo_pg import GetTopoPG


logger = get_logger(__name__)


class ServiceGetTopoPG:
    def __init__(self, params: dict):
        self.params_valid(params)
        self.operator = GetTopoPG(params)

    @staticmethod
    def params_valid(params):
        logger.debug(f'get topology from pg: {params}')
        if 'host' not in params:
            raise Exception('host is required')
        if 'port' not in params:
            raise Exception('port is required')
        if 'db_name' not in params:
            raise Exception('db_name is required')
        if 'topology_id' not in params:
            raise Exception('topology_id is required')

    def execute(self):
        return self.operator.execute()