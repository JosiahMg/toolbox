import os
import yaml

from conf.path_config import project_dir


config_file = os.path.join(project_dir, f'conf/config.yaml')

with open(config_file, encoding='utf-8') as f:
    config = yaml.load(f, Loader=yaml.FullLoader)

# MySQLConfig = config['mysql']
# MongodbConfig = config['mongodb_product']
# Neo4jConfig = config['neo4j']
# PostgresqlConfig = config['postgresql']
# TDConfig = config['tdengine']
# TDengineRest = config['tdengine_rest']
# TDengineHttp = config['tdengine_rest']