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
OCR_SCHEDULED_TIME = "04:00"  # 每天执行OCR的时间 (格式: HH:MM, 24小时制, 北京时间UTC+8)
                              # 注意：此配置使用北京时间，系统会自动转换为UTC时间进行调度
OCR_MAX_RETRIES = 3           # OCR失败后最多重试次数
                              # 注意：此重试次数同时适用于OCR处理和图片处理失败的重试
OCR_BATCH_SIZE = 5           # 单次处理的最大图片数量（内存优化，不影响总处理数）
                              # 说明：定时任务会循环调用，直到所有待处理图片都完成
                              # 例如：如果有 25 张待处理，会分 3 次处理（10+10+5）

# -- 定时任务配置 --
SCHEDULER_MISFIRE_GRACE_TIME = 300   # 任务延迟容忍时间（秒），超过此时间的延迟任务将被跳过
SCHEDULER_MAX_INSTANCES = 1          # 同一任务的最大并发实例数，防止任务重叠执行
SCHEDULER_COALESCE = True            # 合并延迟的任务执行，避免积压

# -- 搜索配置 --
MAX_RESULTS = 5               # 搜索返回的最大结果数量（适用于图片搜索和文本搜索）
