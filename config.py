# config.py

# -- Telegram Bot 配置 --
BOT_TOKEN = "123:abc"  # 替换为你的 Bot Token
ALLOWED_USER_ID = 123456  # 替换为你的 Telegram User ID，只有此用户可以与Bot交互

# -- 文件路径配置 --
IMAGE_DOWNLOAD_PATH = "./downloads"  # Bot下载和索引图片的文件夹
DB_PATH = "image_index.db"         # 索引数据库文件路径
LOG_FILE_PATH = "bot.log"          # 日志文件路径
MAX_IMAGES_IN_DOWNLOAD_FOLDER = 300

# -- OCR 配置 --
OCR_SCHEDULED_TIME = "04:00"  # 每天执行OCR的时间 (格式: HH:MM, 24小时制)
OCR_MAX_RETRIES = 3           # OCR失败后最多重试次数
OCR_BATCH_SIZE = 5           # 单次处理的最大图片数量（内存优化，不影响总处理数）
                              # 说明：定时任务会循环调用，直到所有待处理图片都完成
                              # 例如：如果有 25 张待处理，会分 3 次处理（10+10+5）
