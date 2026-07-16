import logging
import os
import signal
import sys
from datetime import time
from typing import Optional

from telegram import Update
from telegram.ext import ApplicationBuilder, CallbackQueryHandler, CommandHandler, ContextTypes, MessageHandler, filters
from telegram.request import HTTPXRequest

from bot_admin import failed_command, getocr_command, help_command, language_command, setmessageid_command, tag_command, untag_command
from bot_common import BotDeps
from bot_media import handle_photo
from bot_ocr import ocr_command, scheduled_ocr_task
from bot_search import find_command, random_command
from bot_ui import create_handle_find_page_callback
from config import BOT_TOKEN, DB_PATH, IMAGE_DOWNLOAD_PATH, LOG_FILE_PATH, OCR_SCHEDULED_TIME
from image_searcher import ImageSimilaritySearcher


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE_PATH),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

logging.getLogger('httpx').setLevel(logging.WARNING)
logging.getLogger('httpcore').setLevel(logging.WARNING)


searcher = ImageSimilaritySearcher(db_path=DB_PATH)
os.makedirs(IMAGE_DOWNLOAD_PATH, exist_ok=True)
logger.info(f"Image download path: {IMAGE_DOWNLOAD_PATH}")
deps = BotDeps(searcher=searcher, logger=logger)


# 入口文件只保留共享依赖初始化、handler wiring 和 polling 生命周期管理。
def bind_handler(handler, deps: BotDeps):
    async def wrapped(update, context):
        return await handler(deps, update, context)
    return wrapped


def parse_scheduled_time(time_str: str) -> Optional[time]:
    try:
        hour, minute = map(int, time_str.split(':'))
        utc_hour = (hour - 8) % 24
        utc_time = time(hour=utc_hour, minute=minute)

        logger.info(f"Scheduled time converted: Beijing {time_str} -> UTC {utc_time.strftime('%H:%M')}")
        return utc_time
    except (ValueError, AttributeError):
        logger.error(f"Invalid time format: {time_str}. Expected HH:MM")
        return None


async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    logger.error(f"Update {update} caused error {context.error}", exc_info=context.error)

    if isinstance(context.error, Exception):
        error_name = context.error.__class__.__name__
        error_msg = str(context.error)
        logger.warning(f"Network/Connection error occurred: {error_name}: {error_msg}. Bot will continue polling...")


def signal_handler(signum, frame):
    logger.info(f"收到信号 {signum}，正在关闭机器人...")
    sys.exit(0)


def create_application():
    request = HTTPXRequest(
        connection_pool_size=30,
        read_timeout=45.0,
        write_timeout=45.0,
        connect_timeout=45.0,
        pool_timeout=20.0,
        http_version="1.1",
    )

    app = (
        ApplicationBuilder()
        .token(BOT_TOKEN)
        .request(request)
        .get_updates_request(HTTPXRequest(
            connection_pool_size=20,
            read_timeout=90.0,
            write_timeout=45.0,
            connect_timeout=45.0,
            pool_timeout=20.0,
            http_version="1.1",
        ))
        .build()
    )

    app.add_error_handler(error_handler)

    app.add_handler(CallbackQueryHandler(create_handle_find_page_callback(deps), pattern=r"^(find_page|find_random):"))
    app.add_handler(CommandHandler('find', bind_handler(find_command, deps)))
    app.add_handler(CommandHandler('r', bind_handler(random_command, deps)))
    app.add_handler(CommandHandler('ocr', bind_handler(ocr_command, deps)))
    app.add_handler(CommandHandler('tag', bind_handler(tag_command, deps)))
    app.add_handler(CommandHandler('untag', bind_handler(untag_command, deps)))
    app.add_handler(CommandHandler('link', bind_handler(setmessageid_command, deps)))
    app.add_handler(CommandHandler('getocr', bind_handler(getocr_command, deps)))
    app.add_handler(CommandHandler('failed', bind_handler(failed_command, deps)))
    app.add_handler(CommandHandler('help', bind_handler(help_command, deps)))
    app.add_handler(CommandHandler('language', bind_handler(language_command, deps)))
    app.add_handler(CommandHandler('lang', bind_handler(language_command, deps)))
    app.add_handler(MessageHandler(filters.PHOTO, bind_handler(handle_photo, deps)))

    scheduled_ocr_time = parse_scheduled_time(OCR_SCHEDULED_TIME)
    if scheduled_ocr_time:
        job_queue = app.job_queue
        job_queue.run_daily(
            lambda context: scheduled_ocr_task(deps, context),
            time=scheduled_ocr_time,
            name="daily_ocr_task"
        )
        logger.info(f"✅ Scheduled daily OCR task at Beijing time {OCR_SCHEDULED_TIME} (UTC {scheduled_ocr_time.strftime('%H:%M')})")
    else:
        logger.warning(f"Failed to parse OCR scheduled time: {OCR_SCHEDULED_TIME}")

    return app


if __name__ == '__main__':
    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)

    logger.info("Starting bot...")
    logger.info("🤖 机器人启动中...")

    retry_count = 0
    max_retries = 10
    base_retry_interval = 15
    application = None

    while True:
        try:
            if application is None or retry_count > 0:
                logger.info(f"创建新的Application实例 (重试次数: {retry_count})...")
                application = create_application()

            logger.info("开始polling...")
            application.run_polling(allowed_updates=Update.ALL_TYPES, drop_pending_updates=False)
            break
        except KeyboardInterrupt:
            logger.info("收到键盘中断信号，正在优雅关闭...")
            break
        except Exception as e:
            retry_count += 1
            error_name = e.__class__.__name__
            error_msg = str(e)
            logger.error(f"Polling出错 ({retry_count}/{max_retries}): {error_name}: {error_msg}", exc_info=True)
            application = None

            if retry_count >= max_retries:
                logger.error(f"已达到最大重试次数({max_retries})，停止bot")
                break

            retry_interval = min(base_retry_interval * (2 ** (retry_count - 1)), 300)
            logger.info(f"{retry_interval}秒后尝试重新启动polling（指数退避）...")
            import time as _time
            _time.sleep(retry_interval)
