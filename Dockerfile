# 基础镜像为python3.9
FROM python:3.9

# 在镜像中创建目录，用来存放本机中的django项目
RUN mkdir /usr/src/app
# 将本机 . 也就是当前目录下所有文件都拷贝到image文件中指定目录
COPY . /usr/src/app
# 将/usr/src/app指定为工作目录
WORKDIR /usr/src/app

# 在image中安装运行django项目所需要的依赖
RUN pip install -i https://pypi.doubanio.com/simple/ -r requirements.txt --no-cache-dir


EXPOSE 8000

# 启动命令
RUN chmod u+x scripts/start.sh
CMD ["sh", "scripts/start.sh"]