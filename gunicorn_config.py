import os

bind = '0.0.0.0:8000'

# 并行工作进程数
workers = 4
# 指定每个工作者的线程数
threads = 2
# 设置请求超时时间2分钟
timeout = 300
# 超时重启
graceful_timeout = 300

current_dir = os.path.dirname(__file__)
gun_log_dir = os.path.join(current_dir, "logs")

# 设置进程文件目录
# pidfile = os.path.join(gun_log_dir, 'gunicorn.pid')

# 设置访问日志和错误信息日志路径
# accesslog = os.path.join(gun_log_dir, 'diagnosis_gunaccess.log')
errorlog = os.path.join(gun_log_dir, 'gunicorn_error.log')

# 设置gunicorn访问日志格式，错误日志无法设置
access_log_format = '%(t)s %(p)s %(h)s "%(r)s" %(s)s %(L)s %(b)s %(f)s" "%(a)s"'

# 设置日志记录水平
# 正式使用时建议使用warning
# loglevel = 'debug'
loglevel = 'info'
# loglevel = 'warning'