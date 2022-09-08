# -*- coding: utf-8 -*-
# Author: Mh
# Date: 2022/9/8 10:29
# Function:
from common.log_utils import get_logger
from db_operator.postgresql import PostgreOp
import os
import pandas as pd
from conf.path_config import output_dir, resource_dir
from api.sql.pg_sentence import GetDataFromPG
from common.common_tool import make_dir_if_not_exists, open_object_general

logger = get_logger(__name__)


class GetTopoPG:
    def __init__(self, params: dict):
        self.pg = PostgreOp(params)
        self.pg_sql = GetDataFromPG(params)
        self.topology_id = params['topology_id']
        self.output_dir = os.path.join(output_dir, self.topology_id)
        self.stable_topo_dir = os.path.join(resource_dir, self.topology_id)
        # 保存点表和线标文件csv的路径
        self.origin_data_path = os.path.join(self.output_dir, 'stable_topo')
        # 通过稳态分割的拓扑topology.json 需要通过前端传递过来保存到以下路径
        self.stable_topo_path = os.path.join(self.stable_topo_dir, 'stable_topo')

    @staticmethod
    def origin_data_field_map(df, filed_dict):
        """
        node  pipe  字段映射
        """
        logger.debug(f'remap node or pipeline columns: {filed_dict}')
        df = df.rename(columns=filed_dict)
        return df

    def get_topo_sql(self):
        """
        拓扑的点表和线表存在 slsl_data_deliver数据库的 dt_node_merge和 dt_pipeline_merge两个表里
        表中的projectid字段表征属于该区域的点表或线表
        topology_id:
        - qz_high_merge: 青岛的高压数据
        - dg_zb_mid_merge: 东莞中部中压数据
        - qz_high_merge: 泉州高压数据
        """
        logger.debug('begin pull origin nodes and pipes file from pg database')
        node_sql = self.pg_sql.get_node_by_project_id()
        pipe_sql = self.pg_sql.get_pipe_by_project_id()
        node_data_list = self.pg.query(node_sql)
        pipe_data_list = self.pg.query(pipe_sql)

        # 点表或线表不存在
        if not pipe_data_list or not node_data_list:
            logger.debug(f'数据库拉取拓扑失败，该project_id的点表或者线表不存在！ topology_id:{self.topology_id}')
            raise Exception('数据库拉取拓扑失败，该project_id的点表或者线表不存在！')

        pipe_df = pd.DataFrame(pipe_data_list)
        node_df = pd.DataFrame(node_data_list)

        # field mapping
        node_field_dict = {
            'gid': 'gis_id',
            'type': 'type'
        }

        pipe_field_dict = {
            'gid': 'gis_id',
            'pipematerial': 'material',
            'pipelength': 'length',
            'pipediam': 'outer_d',
            'pressurerating': 'pressure_level',
            'wallthickness': 'thickness',
            'st_astext': 'geometry'
        }
        pipe_df = self.origin_data_field_map(pipe_df, pipe_field_dict)
        node_df = self.origin_data_field_map(node_df, node_field_dict)

        # update type
        pipe_df[['gis_id', 'source', 'target']] = pipe_df[['gis_id', 'source', 'target']].astype('str')
        # Object of type Decimal is not JSON serializable
        pipe_df[['length', 'outer_d', 'thickness']] = pipe_df[['length', 'outer_d', 'thickness']].astype('float32')
        node_df['gis_id'] = node_df['gis_id'].astype('str')

        # 将点表设备类型关键字段 dno进行映射
        # pg库查出来的点表数据进行数据映射
        dno_to_type = {
            1: '管段',
            2: '调压站',  # 调压设备
            3: '阀门',
            4: '凝水缸',
            5: '带气点',
            6: '空节点',
            7: '气源',  # 气源点
            8: '流量计',
            9: '检测点',
            10: '民用户',
            11: '工商户',
            12: '节点',
            13: '阴极保护设备'
        }
        node_df['type'] = node_df["dno"].map(dno_to_type).fillna(node_df["dno"])

        make_dir_if_not_exists(self.origin_data_path)
        pipe_df.to_csv(os.path.join(self.origin_data_path, 'pipes.csv'), index=False)
        node_df.to_csv(os.path.join(self.origin_data_path, 'nodes.csv'), index=False)
        logger.debug(f'save {self.topology_id} origin topology data to {self.origin_data_path}')
        return self.origin_data_path

    def add_area_to_pipes(self):
        """ 将稳态分割得到的area信息合并到pipes.csv中 """
        origin_pipe_filename = os.path.join(self.origin_data_path, 'pipes.csv')
        if not os.path.exists(origin_pipe_filename):
            self.get_topo_sql()
        if not os.path.exists(os.path.join(self.stable_topo_path, 'topology.json')):
            logger.warning('topology.json not exists')
            return
        topo_data = open_object_general(self.stable_topo_path, 'topology')
        area_dt_lt = []
        for area_data in topo_data:
            area_no = area_data['area_no']
            for line in area_data['info']:
                area_dt_lt.append({'gis_id': int(line['gis_id']), 'area_no': area_no})
        area_df = pd.DataFrame(area_dt_lt)
        pipes_df = pd.read_csv(origin_pipe_filename, encoding='utf-8')
        if 'area_no' not in pipes_df.columns:
            ret_pipes_df = pd.merge(pipes_df, area_df, how='inner', on='gis_id')
            logger.info(f'merged pipes save to {self.origin_data_path}')
            ret_pipes_df.to_csv(os.path.join(self.origin_data_path, 'pipes.csv'), index=False)
        else:
            logger.warning('pipes csv exist columns: area_no, ignore add_area_to_pipes')

    def execute(self):
        # Step1: 从数据库拉取数据
        filepath = self.get_topo_sql()
        # Step2: 添加area_no到pipes
        self.add_area_to_pipes()
        return filepath
