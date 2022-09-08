import os

project_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

log_dir = os.path.join(project_dir, 'logs')
log_file = os.path.join(project_dir, 'logs/tools.log')
resource_dir = os.path.join(project_dir, 'resource')
output_dir = os.path.join(project_dir, 'data')