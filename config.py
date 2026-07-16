# config.py

# Telegram Bot 配置
BOT_TOKEN = "123:abc"  # 替换为你的 Bot Token
ALLOWED_USER_ID = 123456  # 替换为你的 Telegram User ID，只有此用户可以与Bot交互

# 文件路径配置
IMAGE_DOWNLOAD_PATH = "./downloads"  # Bot 下载和索引图片的文件夹
DB_PATH = "image_index.db"
LOG_FILE_PATH = "bot.log"
MAX_IMAGES_IN_DOWNLOAD_FOLDER = 300

# OCR 配置
OCR_SCHEDULED_TIME = "04:00"  # 北京时间，运行时会转换为 UTC 调度
OCR_MAX_RETRIES = 3  # 同时用于 OCR 重试和图片处理失败重试
OCR_BATCH_SIZE = 5  # 单批上限，不影响定时任务持续处理直到队列清空

# Mac 快捷指令 OCR 配置
MAC_SHORTCUTS = "ocr-file"  # 非空时走 shortcuts OCR；为空时回退到 PaddleOCR

# OCR 后处理配置
OCR_POST_FILTER_PATTERNS = [
    r'^\s*$',                 # 空行或只有空白字符的行
    r'^[\d\s]+$',             # 只有数字和空白的行
    r'^[^\w一-鿿]+$', # 只有符号的行（不包含字母、数字、中文）
]

# 定时任务配置
SCHEDULER_MISFIRE_GRACE_TIME = 300
SCHEDULER_MAX_INSTANCES = 1
SCHEDULER_COALESCE = True

# 搜索配置
MAX_RESULTS = 5
FIND_PAGINATION_ENABLED = True
FIND_PAGE_SIZE = 9  # 运行时会限制在 1-9
RANDOM_DEFAULT_COUNT = 9

# 语言配置
DEFAULT_LANGUAGE = "zh"
SUPPORTED_LANGUAGES = ("zh", "en")

# 失败记录配置
FAILED_OCR_DEFAULT_LIMIT = 5
