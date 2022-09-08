# 为算法的数据处理部分调取数据库信息

from common.log_utils import get_logger

logger = get_logger(__name__)


class SqlSentence(object):
    def __init__(self):
        # 利用re_cfg，初始化re的表内容、表名
        pass

    @staticmethod
    def get_monitor_position_info():
        sql = 'SELECT position_id, position, device_id ' \
              'FROM monitor_position '
        logger.info('sql: {}'.format(sql))
        return sql

    @staticmethod
    def get_device_info():
        sql = 'SELECT device_id, company_id ' \
              'FROM device '
        logger.info('sql: {}'.format(sql))
        return sql

    @staticmethod  # zy
    def get_pipe_by_project_id(project_id):
        sql = """select gid, pipematerial, pipelength, pipediam, pressurerating,
                  wallthickness, source, target from dt_pipeline
                  where third_project_id = '{}' """.format(project_id)
        logger.info('sql: {}'.format(sql))
        return sql

    @staticmethod  # sb
    def get_node_by_project_id(project_id):
        sql = """select gid, dno from dt_node  where third_project_id = '{}' """.format(project_id)
        logger.info('sql: {}'.format(sql))
        return sql

    @staticmethod
    def get_node_map_device_type_by_project_id(project_id):
        sql = """select gid, dno from dt_node  where third_project_id = '{}' """.format(project_id)
        logger.info('sql: {}'.format(sql))
        return sql

    @staticmethod
    def get_table_exist_by_project_id(project_id):
        sql = """select id from dt_gis_area where third_project_id='{}' limit 1""".format(project_id)
        logger.info('sql: {}'.format(sql))
        return sql

    @staticmethod
    def get_device_id_by_imie(device_imei):
        sql = """select device_id from bus_device where device_imei='{}' limit 1""".format(device_imei)
        logger.info('sql: {}'.format(sql))
        return sql

    @staticmethod
    def get_position_id_by_device_id(device_id):
        sql = """select position_id from rel_monitor_position where device_id='{}' and position=0""".format(device_id)
        logger.info('sql: {}'.format(sql))
        return sql

    @staticmethod
    def get_imei_from_device():
        sql = """select device_imei from bus_device"""
        logger.info('sql: {}'.format(sql))
        return sql

    @staticmethod
    def get_device_id_by_pid(pid):
        sql = """select device_id from rel_monitor_position where position_id='{}' and position=0""".format(pid)
        logger.info('sql: {}'.format(sql))
        return sql

    @staticmethod
    def get_imei_by_device_id(device_id):
        sql = """select device_imei from bus_device where device_id='{}'""".format(device_id)
        logger.info('sql: {}'.format(sql))
        return sql
