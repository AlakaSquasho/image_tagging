# 使用轻量级 Python 镜像
FROM python:3.11-slim-bullseye

# 设置工作目录
WORKDIR /app

# 设置环境变量以加速构建
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

# 第一阶段：安装最小化系统依赖
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    libgl1 \
    libglib2.0-0 \
    libsm6 \
    libxext6 \
    && rm -rf /var/lib/apt/lists/*

# 第二阶段：创建虚拟环境并安装 Python 依赖
# 使用 --no-build-isolation 避免编译优化库
RUN pip install --upgrade pip setuptools wheel && \
    pip install --no-build-isolation --no-cache-dir \
    python-telegram-bot \
    Pillow \
    ImageHash \
    paddleocr \
    paddlepaddle \
    aiohttp \
    numpy \
    opencv-python-headless \
    python-dotenv \
    requests

# 复制项目代码到工作目录
COPY . .

# 创建数据目录
RUN mkdir -p /app/downloads && \
    chmod 755 /app/downloads

# 清理缓存和临时文件
RUN apt-get clean && \
    find /usr/local/lib/python3.11 -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true && \
    find /usr/local/lib/python3.11 -type f -name "*.pyc" -delete

# 定义容器启动时执行的命令
CMD ["python", "telegram_bot.py"]