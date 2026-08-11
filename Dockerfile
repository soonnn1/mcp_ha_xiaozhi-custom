# 使用Alpine官方Python镜像（体积仅约50MB）
FROM python:3.13.3-alpine

# 设置工作目录
WORKDIR /app

# 先复制依赖文件以利用Docker缓存
COPY requirements.txt .

# 安装纯Python依赖（无需额外系统依赖）
RUN pip install -i https://pypi.tuna.tsinghua.edu.cn/simple -r requirements.txt

# 复制应用代码
COPY . .

# 设置运行时环境变量（建议通过docker run注入敏感信息）
ENV XIAOZHI_MCP_ENDPOINT= \
    API_ACCESS_TOKEN= \
    HA_MCP_ENDPOINT= \
    MCP_TRANSPORT=auto

# 启动命令
ENTRYPOINT ["python", "mcp_pipe.py"]
