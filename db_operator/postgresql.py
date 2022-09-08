import psycopg2
from psycopg2 import extras as ex
from conf.config import PostgresqlConfig
from common.log_utils import get_logger

logger = get_logger(__name__)


class PostgreOp(object):
    def __init__(self, con, postgresql_dict=None):
        if con == 'area':
            self.conn = psycopg2.connect(host=postgresql_dict['host'], port=postgresql_dict['port'],
                                         user=postgresql_dict['user'], password=str(postgresql_dict['password']),
                                         database=postgresql_dict['db_name'])
        else:
            self.conn = psycopg2.connect(host=PostgresqlConfig['host'], port=PostgresqlConfig['port'],
                                         user=PostgresqlConfig['user'], password=str(PostgresqlConfig['password']),
                                         database=PostgresqlConfig['db_name'])
        try:
            self.cursor = self.conn.cursor()
        except Exception as e:
            logger.error("连接数据库出错: {}".format(e))
            raise ValueError("连接数据库出错")

    def query(self, sql):
        try:
            self.cursor.execute(sql)
            self.conn.commit()
            # self.cursor.execute('UPDATE signal_origin SET used=1 WHERE sig_index = 4858 ;')
            return self._row2dict()
        except Exception as e:
            logger.error("查询数据库出错:{}".format(e))
            raise ValueError('查询数据库出错')
            # finally:
            #     self.close()

    def binary_query(self, sql):
        try:
            self.cursor.execute(sql)
            self.conn.commit()
            return self._tup2dict()
        except Exception as e:
            logger.error("查询数据库出错:{}".format(e))
            raise ValueError('查询数据库出错')

    def delete(self, table_name, where):
        delete_sql = "DELETE FROM {} {} ".format(table_name, where)
        logger.info('sql: {}'.format(delete_sql))
        try:
            self.cursor.execute(delete_sql)
            self.conn.commit()
        except Exception as e:
            logger.error("删除数据出错".format(e))
            raise ValueError("删除数据出错")

    def insert_many(self, table_name, fields, values_list):
        fields_str = '({})'.format(', '.join(fields))

        insert_many_template = "INSERT INTO {}{} " \
                               "VALUES %s".format(table_name, fields_str)
        try:
            ex.execute_values(self.cursor, insert_many_template, values_list, page_size=10000)
            self.conn.commit()
        except Exception as e:
            logger.error("批量插入数据出错:{}".format(e))
            raise ValueError('批量插入数据出错')
            # finally:
            #     self.close()

    def _row2dict(self):
        columns = [col[0] for col in self.cursor.description]
        return [dict(zip(columns, row)) for row in self.cursor.fetchall()]

    def _tup2dict(self):
        return dict(self.cursor.fetchall())

    def close(self):
        self.cursor.close()
        self.conn.close()
