import logging
import os
import shutil
import glob
import signal
import sys
from uuid import uuid4
from datetime import datetime, time
import asyncio

try:
    from zoneinfo import ZoneInfo
except ImportError:
    # Python < 3.9 fallback
    try:
        from backports.zoneinfo import ZoneInfo
    except ImportError:
        ZoneInfo = None

from telegram import Update, InputFile, MessageOriginChannel, InlineKeyboardButton, InlineKeyboardMarkup, InputMediaPhoto
from telegram.ext import ApplicationBuilder, ContextTypes, CommandHandler, MessageHandler, filters, CallbackQueryHandler

from config import (BOT_TOKEN, ALLOWED_USER_ID, IMAGE_DOWNLOAD_PATH, DB_PATH, LOG_FILE_PATH,
                   MAX_IMAGES_IN_DOWNLOAD_FOLDER, OCR_SCHEDULED_TIME, OCR_MAX_RETRIES, OCR_BATCH_SIZE,
                   MAX_RESULTS, SCHEDULER_MISFIRE_GRACE_TIME, SCHEDULER_MAX_INSTANCES, SCHEDULER_COALESCE,
                   FAILED_OCR_DEFAULT_LIMIT, FIND_PAGINATION_ENABLED, FIND_PAGE_SIZE)
from image_searcher import ImageSimilaritySearcher

from typing import Dict, Optional, List, Tuple

# --- 日志设置 ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE_PATH),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# 控制台输出的日志中，httpx的相关日志不需要写入bot.log。
# 通过设置httpx和httpcore库的日志级别来减少日志输出。
logging.getLogger('httpx').setLevel(logging.WARNING)
logging.getLogger('httpcore').setLevel(logging.WARNING)


def create_progress_bar(current: int, total: int, bar_length: int = 20) -> str:
    """
    创建 ASCII 进度条。
    :param current: 当前进度
    :param total: 总数
    :param bar_length: 进度条长度（默认20个字符）
    :return: 进度条字符串
    """
    if total == 0:
        return "■" * bar_length + " 0%"

    percentage = current / total
    filled = int(bar_length * percentage)
    bar = "█" * filled + "░" * (bar_length - filled)
    percent_str = f"{percentage * 100:.1f}%"

    return f"{bar} {percent_str}"


def get_find_page_size() -> int:
    """
    获取 /find 分页每页数量，并限制在 1-9 之间。
    超出范围则回退为默认值 9。
    """
    try:
        page_size = int(FIND_PAGE_SIZE)
    except (TypeError, ValueError):
        page_size = 9

    if page_size < 1 or page_size > 9:
        return 9
    return page_size


def paginate_results(results: List[Dict], page: int, page_size: int) -> Tuple[List[Dict], int]:
    """
    分页切片并返回当前页结果与总页数。
    """
    total = len(results)
    total_pages = max(1, (total + page_size - 1) // page_size)
    safe_page = min(max(page, 1), total_pages)
    start = (safe_page - 1) * page_size
    end = start + page_size
    return results[start:end], total_pages


def build_find_keyboard(page: int, total_pages: int, query_id: str) -> Optional[InlineKeyboardMarkup]:
    if total_pages <= 1:
        return None

    prev_page = max(1, page - 1)
    next_page = min(total_pages, page + 1)

    buttons = [
        InlineKeyboardButton("上一页", callback_data=f"find_page:{query_id}:{prev_page}"),
        InlineKeyboardButton(f"{page}/{total_pages}", callback_data="find_page:noop:0"),
        InlineKeyboardButton("下一页", callback_data=f"find_page:{query_id}:{next_page}"),
    ]
    return InlineKeyboardMarkup([buttons])


def get_find_summary_text(state: Dict, page: int, total_pages: int) -> str:
    summary = state.get("summary", "")
    total = len(state.get("results", []))
    return f"{summary}\n第 {page}/{total_pages} 页（共 {total} 条）"


def build_find_summary_text(state: Dict, page: int, total_pages: int, page_results: List[Dict]) -> str:
    summary_text = get_find_summary_text(state, page, total_pages)
    link_lines = [
        result['telegram_message_id']
        for result in page_results
        if result.get('telegram_message_id')
    ]
    if link_lines:
        summary_text = f"{summary_text}\n\n" + "\n".join(link_lines)
    return summary_text


async def render_find_page(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    query_id: str,
    page: int,
    *,
    is_callback: bool,
) -> None:
    state = context.user_data.get("find_pagination", {}).get(query_id)
    if not state:
        if is_callback and update.callback_query:
            await update.callback_query.answer("分页已失效，请重新搜索。", show_alert=False)
        return

    results = state.get("results", [])
    page_size = state.get("page_size", 9)
    page_results, total_pages = paginate_results(results, page, page_size)
    page = min(max(page, 1), total_pages)

    keyboard = build_find_keyboard(page, total_pages, query_id)
    summary_text = build_find_summary_text(state, page, total_pages, page_results)

    chat_id = update.effective_chat.id

    summary_message_id = state.get("summary_message_id")
    if summary_message_id:
        try:
            await context.bot.delete_message(chat_id=chat_id, message_id=summary_message_id)
        except Exception:
            pass
        summary_message_id = None
        state["summary_message_id"] = None

    message_ids = state.get("message_ids", [])
    if message_ids:
        for message_id in message_ids:
            try:
                await context.bot.delete_message(chat_id=chat_id, message_id=message_id)
            except Exception:
                continue
        message_ids = []

    media_group = []
    for result in page_results:
        if not os.path.exists(result['path']):
            logger.warning(f"Search result file not found: {result['path']}")
            continue
        try:
            with open(result['path'], 'rb') as photo_file:
                media_group.append(InputMediaPhoto(media=photo_file.read()))
        except Exception as e:
            logger.error(f"发送搜索结果图片失败: {e}")

    if media_group:
        try:
            media_messages = await context.bot.send_media_group(chat_id=chat_id, media=media_group)
            message_ids.extend([m.message_id for m in media_messages])
        except Exception as e:
            logger.error(f"发送图片组失败: {e}")

    if summary_message_id:
        message_ids.append(summary_message_id)
    else:
        summary_message = await context.bot.send_message(
            chat_id=chat_id,
            text=summary_text,
            reply_markup=keyboard,
            reply_to_message_id=update.message.message_id if update.message else None
        )
        summary_message_id = summary_message.message_id
        message_ids.append(summary_message_id)

    state["message_ids"] = message_ids
    state["current_page"] = page
    state["summary_message_id"] = summary_message_id


# --- 初始化搜索器和下载路径 ---
searcher = ImageSimilaritySearcher(db_path=DB_PATH)
os.makedirs(IMAGE_DOWNLOAD_PATH, exist_ok=True)
logger.info(f"Image download path: {IMAGE_DOWNLOAD_PATH}")


def get_image_files_in_folder(folder_path: str) -> List[str]:
    """
    获取指定文件夹下所有图片文件的路径。
    过滤掉子文件夹，只查找顶层图片文件。
    """
    image_extensions = ('.jpg', '.jpeg', '.png', '.gif', '.bmp', '.tiff', '.webp')
    files = []
    try:
        for item in os.listdir(folder_path):
            item_path = os.path.join(folder_path, item)
            # 确保是文件且不是目录
            if os.path.isfile(item_path) and item_path.lower().endswith(image_extensions):
                files.append(item_path)
    except OSError as e:
        logger.error(f"Error listing files in {folder_path}: {e}")
    except Exception as e:
        logger.error(f"Unexpected error in get_image_files_in_folder: {e}")

    return files


async def check_and_archive_images(download_folder: str, max_count: int, searcher_instance: ImageSimilaritySearcher, context: ContextTypes.DEFAULT_TYPE):
    """
    检查下载文件夹中的图片数量，如果达到阈值则进行归档。
    归档规则：所有图片移动到一个新文件夹，命名为 A_B (最早修改日期_最晚修改日期)。
    并更新数据库中的文件路径。
    """
    if not os.path.exists(download_folder):
        logger.warning(f"Download folder does not exist: {download_folder}")
        return

    logger.info(f"Checking image count in {download_folder}...")
    image_files = get_image_files_in_folder(download_folder)

    if len(image_files) >= max_count:
        logger.info(f"Image count ({len(image_files)}) reached or exceeded {max_count}. Initiating archive process.")

        file_modification_times = []
        valid_image_files = []
        for fpath in image_files:
            try:
                # Using st_mtime (last modification time) as it's generally reliable
                # and reflects when the file was last written (downloaded).
                mtime = os.path.getmtime(fpath)
                file_modification_times.append(mtime)
                valid_image_files.append(fpath)
            except OSError as e:
                logger.warning(f"Cannot access file {fpath}: {e}. Skipping.")
                continue
            except Exception as e:
                logger.error(f"Unexpected error getting modification time for {fpath}: {e}. Skipping.")
                continue

        if not valid_image_files:
            logger.warning("No valid image files found to archive.")
            return

        if not file_modification_times:
            logger.warning("No modification times collected for archiving.")
            return

        min_time = datetime.fromtimestamp(min(file_modification_times))
        max_time = datetime.fromtimestamp(max(file_modification_times))

        # Format folder name as YYYY.MM.DD_YYYY.MM.DD
        folder_name = f"{min_time.strftime('%Y.%m.%d')}_{max_time.strftime('%Y.%m.%d')}"
        archive_path = os.path.join(download_folder, folder_name)

        try:
            os.makedirs(archive_path, exist_ok=True)
            logger.info(f"Created archive folder: {archive_path}")
        except OSError as e:
            logger.error(f"Failed to create archive folder {archive_path}: {e}")
            return

        old_new_paths_for_db = []
        successful_moves_count = 0
        failed_moves = []

        for old_path in valid_image_files:
            try:
                file_name = os.path.basename(old_path)
                new_path = os.path.join(archive_path, file_name)

                # 避免覆盖同名文件
                if os.path.exists(new_path):
                    name, ext = os.path.splitext(file_name)
                    counter = 1
                    while os.path.exists(new_path):
                        new_path = os.path.join(archive_path, f"{name}_{counter}{ext}")
                        counter += 1
                    logger.info(f"File name conflict, renamed to: {os.path.basename(new_path)}")

                shutil.move(old_path, new_path)
                old_new_paths_for_db.append((old_path, new_path))
                successful_moves_count += 1
                logger.debug(f"Moved {old_path} to {new_path}")
            except OSError as e:
                logger.error(f"Failed to move file {old_path} during archiving: {e}")
                failed_moves.append(old_path)
            except Exception as e:
                logger.error(f"Unexpected error moving file {old_path}: {e}")
                failed_moves.append(old_path)

        # 更新数据库
        if old_new_paths_for_db:
            try:
                logger.info(f"Updating database paths for {len(old_new_paths_for_db)} archived images.")
                searcher_instance.update_archived_file_paths(old_new_paths_for_db)
                logger.info("Database paths updated successfully.")
            except Exception as e:
                logger.error(f"Failed to update database paths: {e}")
        else:
            logger.warning("No files were successfully moved, database not updated.")

        # 发送完成消息
        message = f"下载文件夹已归档。\n新文件夹: `{folder_name}`\n归档图片数量: {successful_moves_count}"
        if failed_moves:
            message += f"\n失败数量: {len(failed_moves)}"

        try:
            await context.bot.send_message(
                chat_id=ALLOWED_USER_ID,
                text=message,
                parse_mode='Markdown'
            )
        except Exception as e:
            logger.error(f"Failed to send archive notification: {e}")
    else:
        logger.info(f"Image count ({len(image_files)}) is below {max_count}. No archive needed.")


async def handle_photo_with_retry(update: Update, context: ContextTypes.DEFAULT_TYPE, max_retries: int = OCR_MAX_RETRIES) -> bool:
    """
    带重试机制的图片处理函数。
    
    Args:
        update: Telegram Update对象
        context: Telegram Context对象
        max_retries: 最大重试次数，默认使用OCR配置的重试次数
        
    Returns:
        bool: 处理成功返回True，失败返回False
    """
    photo = update.message.photo[-1] # Get the largest photo size
    current_message_id = update.message.message_id # Bot's received message ID
    
    # Extract original message ID for database storage if it's a forwarded channel message
    telegram_msg_id_for_db = ""
    forward_origin = update.message.forward_origin
    if isinstance(forward_origin, MessageOriginChannel):
        # Telegram channel usernames are unique, can form a direct link
        if forward_origin.chat.username:
            telegram_msg_id_for_db = f"https://t.me/{forward_origin.chat.username}/{forward_origin.message_id}"
            logger.info(f"Detected forwarded message from channel with original ID: {telegram_msg_id_for_db}")
        else:
            logger.info("Forwarded message from private channel or supergroup, no public link.")
    else:
        logger.info("Message is not a forwarded channel message.")

    # Determine file extension
    file_ext = os.path.splitext(photo.file_unique_id)[1] or '.jpg'
    temp_save_path = None
    
    for attempt in range(max_retries + 1):  # +1 因为第一次不算重试
        try:
            # 生成临时文件路径，确保文件夹存在
            if not os.path.exists(IMAGE_DOWNLOAD_PATH):
                os.makedirs(IMAGE_DOWNLOAD_PATH, exist_ok=True)
            
            temp_save_path = os.path.join(IMAGE_DOWNLOAD_PATH, f"temp_{uuid4()}{file_ext}")
            
            # 下载文件
            file = await context.bot.get_file(photo.file_id)
            await file.download_to_drive(temp_save_path)
            
            # 验证文件是否成功下载
            if not os.path.exists(temp_save_path) or os.path.getsize(temp_save_path) == 0:
                raise Exception(f"Downloaded file is empty or doesn't exist: {temp_save_path}")
            
            logger.info(f"Downloaded photo to temporary path {temp_save_path} (attempt {attempt + 1})")

            # Check if the message caption contains the /find command
            if update.message.caption and update.message.caption.strip().lower() == '/find':
                # --- Execute search logic ---
                await search_by_image(update, context, temp_save_path)
                return True
            else:
                # --- Execute add/deduplication logic ---
                # 1. Check for exact duplicate first
                exact_match_results = searcher.search_similar_images(temp_save_path, threshold=0, max_results=1)
                
                if exact_match_results and exact_match_results[0].get('similarity') == 1.0:
                    exact_match_data = exact_match_results[0]
                    existing_telegram_message_id_in_db = exact_match_data.get('telegram_message_id')
                    
                    if existing_telegram_message_id_in_db:
                        await update.message.reply_text(f"此图片已存在。\n原消息ID: {existing_telegram_message_id_in_db}", reply_to_message_id=current_message_id)
                        logger.info(f"Duplicate image received, original telegram_message_id: {existing_telegram_message_id_in_db}")
                    else:
                        try:
                            with open(exact_match_data['path'], 'rb') as photo_file:
                                caption = (f"此图片已存在，但无原消息ID。\n"
                                           f"文件路径: `{os.path.basename(exact_match_data['path'])}`\n"
                                           f"文件哈希: `{exact_match_data['file_hash']}`\n"
                                           f"更新时间: {datetime.fromtimestamp(exact_match_data['updated_time']).strftime('%Y-%m-%d %H:%M:%S')}")
                                await context.bot.send_photo(
                                    chat_id=update.effective_chat.id,
                                    photo=InputFile(photo_file),
                                    caption=caption,
                                    parse_mode='Markdown',
                                    reply_to_message_id=current_message_id
                                )
                                logger.info(f"Duplicate image received with no source message ID, sent details for {exact_match_data['path']}")
                        except FileNotFoundError:
                            logger.warning(f"Existing file not found: {exact_match_data['path']}. Cannot send to user.")
                            await update.message.reply_text("此图片已存在，但原始文件丢失。", reply_to_message_id=current_message_id)
                        except Exception as e:
                            logger.error(f"Error sending existing image details: {e}")
                            await update.message.reply_text("处理现有图片时发生错误。", reply_to_message_id=current_message_id)
                else:
                    # 2. If it's a new image, rename and add to index
                    permanent_path = os.path.join(IMAGE_DOWNLOAD_PATH, f"{current_message_id}_{photo.file_unique_id}{file_ext}")
                    
                    try:
                        os.rename(temp_save_path, permanent_path)
                        temp_save_path = None  # Mark as None to prevent deletion in finally block
                    except OSError as e:
                        raise Exception(f"Failed to rename file {temp_save_path} to {permanent_path}: {e}")

                    # Add image to index - now returns bool (True/False) instead of OCR text
                    # OCR will be processed later by scheduled task
                    index_success = searcher.add_image_to_index(permanent_path, telegram_msg_id_for_db)
                    if index_success:
                        pending_count = searcher.get_pending_ocr_count(OCR_MAX_RETRIES)
                        await update.message.reply_text(f"该图片已成功建立索引。\nOCR处理将在定时任务中进行。\n当前待处理OCR图片数: {pending_count}", 
                                                        reply_to_message_id=current_message_id, parse_mode='Markdown')
                    else:
                        raise Exception("图片索引建立失败")
                    
                    # After successfully indexing a new image, check for archiving
                    await check_and_archive_images(IMAGE_DOWNLOAD_PATH, MAX_IMAGES_IN_DOWNLOAD_FOLDER, searcher, context)
                
                return True  # 成功处理
                
        except Exception as e:
            logger.error(f"Error handling photo attempt {attempt + 1}/{max_retries + 1} with message_id {current_message_id}: {e}")
            
            # 如果还有重试机会，继续重试
            if attempt < max_retries:
                logger.info(f"Retrying photo processing... (attempt {attempt + 2}/{max_retries + 1})")
                # 清理临时文件（如果存在）
                if temp_save_path and os.path.exists(temp_save_path):
                    try:
                        os.remove(temp_save_path)
                        temp_save_path = None
                    except OSError:
                        pass
                # 短暂延迟后重试
                await asyncio.sleep(1)
                continue
            else:
                # 已达到最大重试次数，放弃处理
                logger.error(f"Failed to handle photo after {max_retries + 1} attempts with message_id {current_message_id}")
                await update.message.reply_text(
                    f"图片处理失败（已重试{max_retries}次）。\n请检查日志或稍后重试。", 
                    reply_to_message_id=current_message_id
                )
                return False
        
        finally:
            # Clean up temporary file if it still exists
            if temp_save_path and os.path.exists(temp_save_path):
                try:
                    os.remove(temp_save_path)
                    logger.info(f"Cleaned up temporary file: {temp_save_path}")
                except OSError as e:
                    logger.error(f"Failed to clean up temporary file {temp_save_path}: {e}")
    
    return False  # 不应该到达这里，但为了安全起见


async def handle_photo(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    处理用户发送的图片。
    - 如果图片附带 /find 命令（caption），则执行搜索。
    - 否则，检查图片是否已存在。若不存在，则添加索引；若存在，则根据是否有原消息ID返回相应结果。
    
    现在包含重试机制：如果处理失败，会自动重试，重试次数与OCR配置保持一致。
    """
    try:
        logger.info(f"📸 Received photo from user {update.message.from_user.id}, message_id: {update.message.message_id}")
        
        if update.message.from_user.id != ALLOWED_USER_ID:
            logger.warning(f"❌ Unauthorized user {update.message.from_user.id} tried to interact.")
            return

        logger.info(f"✅ User authorized, sending processing message...")
        await update.message.reply_text("处理中...")
        
        # 调用带重试机制的处理函数
        logger.info(f"🔄 Starting photo processing with retry mechanism...")
        await handle_photo_with_retry(update, context)
        logger.info(f"✅ Photo processing completed for message_id: {update.message.message_id}")
    except Exception as e:
        logger.error(f"❌ Critical error in handle_photo: {e}", exc_info=True)
        try:
            await update.message.reply_text(f"处理图片时发生严重错误: {str(e)}")
        except:
            pass



async def search_by_image(update: Update, context: ContextTypes.DEFAULT_TYPE, query_image_path: str):
    """
    根据给定的图片路径执行搜索并回复结果。
    首先检查是否有完全匹配，如果有且有原消息ID则直接回复ID并返回。
    如果完全匹配但无原消息ID，则发送图片和详细信息。
    否则，发送所有相似结果。
    """
    try:
        # search_similar_images returns a list of dicts, sorted by similarity descending.
        # An exact match (similarity 1.0) would be the first item if found.
        results = searcher.search_similar_images(query_image_path)
        
        if not results:
            await update.message.reply_text("未找到匹配结果。", reply_to_message_id=update.message.message_id)
            return

        first_result = results[0]
        # Check if the *first* result is an exact match (similarity == 1.0)
        if first_result.get('similarity') == 1.0:
            existing_telegram_message_id_in_db = first_result.get('telegram_message_id')
            
            if existing_telegram_message_id_in_db:
                # Case 1: Found exact match with a stored original message ID.
                await update.message.reply_text(f"找到完全匹配的结果。\n原消息ID: {existing_telegram_message_id_in_db}", 
                                                reply_to_message_id=update.message.message_id)
                return
            else:
                # Case 2: Found exact match but no original message ID. Send the image with details.
                try:
                    if not os.path.exists(first_result['path']):
                        logger.warning(f"Exact match file not found: {first_result['path']}.")
                        await update.message.reply_text("找到完全匹配的结果，但原始文件丢失。", reply_to_message_id=update.message.message_id)
                        return
                    
                    with open(first_result['path'], 'rb') as photo_file:
                        caption = (f"找到完全匹配的结果，但无原消息ID。\n"
                                   f"文件路径: `{os.path.basename(first_result['path'])}`\n"
                                   f"文件哈希: `{first_result['file_hash']}`\n"
                                   f"更新时间: {datetime.fromtimestamp(first_result['updated_time']).strftime('%Y-%m-%d %H:%M:%S')}")
                        await context.bot.send_photo(
                            chat_id=update.effective_chat.id,
                            photo=InputFile(photo_file),
                            caption=caption,
                            parse_mode='Markdown',
                            reply_to_message_id=update.message.message_id
                        )
                    logger.info(f"Sent exact match image details for {first_result['path']}")
                    return
                except IOError as e:
                    logger.error(f"IO error reading exact match file {first_result['path']}: {e}")
                    await update.message.reply_text("读取文件时发生错误。", reply_to_message_id=update.message.message_id)
                    return
                except Exception as e:
                    logger.error(f"Error sending exact match image details: {e}")
                    await update.message.reply_text("处理完全匹配图片时发生错误。", reply_to_message_id=update.message.message_id)
                    return
            
        # If we reach here, it means there was no exact match (similarity < 1.0)
        # Now, send all found similar results.
        if FIND_PAGINATION_ENABLED and len(results) > 1:
            query_id = str(uuid4())
            page_size = get_find_page_size()
            context.user_data.setdefault("find_pagination", {})[query_id] = {
                "results": results,
                "mode": "image",
                "page_size": page_size,
                "summary": f"未找到完全匹配的结果，以下是 {len(results)} 个相似结果:",
                "message_ids": []
            }
            await render_find_page(update, context, query_id, 1, is_callback=False)
            return

        await update.message.reply_text(f"未找到完全匹配的结果，以下是 {len(results)} 个相似结果:",
                                        reply_to_message_id=update.message.message_id)

        for result in results:
            try:
                if not os.path.exists(result['path']):
                    logger.warning(f"Search result file not found: {result['path']}.")
                    await update.message.reply_text(f"无法发送结果，文件已不存在: `{os.path.basename(result['path'])}`", 
                                                    reply_to_message_id=update.message.message_id, parse_mode='Markdown')
                    continue
                
                with open(result['path'], 'rb') as photo_file:
                    caption_parts = []
                    if result.get('telegram_message_id'):
                        caption_parts.append(f"原消息ID: {result['telegram_message_id']}")
                    
                    caption_parts.append(f"文件路径: `{os.path.basename(result['path'])}`")
                    caption_parts.append(f"文件哈希: `{result['file_hash']}`")
                    caption_parts.append(f"更新时间: {datetime.fromtimestamp(result['updated_time']).strftime('%Y-%m-%d %H:%M:%S')}")
                    if 'similarity' in result:
                        caption_parts.append(f"相似度: {result['similarity']:.2%}")
                    if 'ocr_text' in result and result['ocr_text']:
                        display_ocr_text = result['ocr_text'][:100] + "..." if len(result['ocr_text']) > 100 else result['ocr_text']
                        caption_parts.append(f"OCR文本: `{display_ocr_text}`")
                    
                    caption = "\n".join(caption_parts)
                    
                    await context.bot.send_photo(
                        chat_id=update.effective_chat.id,
                        photo=InputFile(photo_file),
                        caption=caption,
                        parse_mode='Markdown',
                        reply_to_message_id=update.message.message_id
                    )
            except IOError as e:
                logger.error(f"IO error reading search result file {result['path']}: {e}")
                await update.message.reply_text(f"读取文件时发生错误: `{os.path.basename(result['path'])}`", 
                                                reply_to_message_id=update.message.message_id, parse_mode='Markdown')
            except Exception as e:
                logger.error(f"Failed to send search result photo {result['path']}: {e}")
                await update.message.reply_text(f"发送搜索结果图片时发生错误: `{os.path.basename(result['path'])}`", 
                                                reply_to_message_id=update.message.message_id, parse_mode='Markdown')
    
    except Exception as e:
        logger.error(f"Unexpected error in search_by_image: {e}", exc_info=True)
        await update.message.reply_text("搜索时发生意外错误。", reply_to_message_id=update.message.message_id)


async def find_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """处理 /find 命令 (文本或图片搜索)"""
    logger.info(f"🔍 Received /find command from user {update.message.from_user.id}")
    
    if update.message.from_user.id != ALLOWED_USER_ID:
        logger.warning(f"❌ Unauthorized user {update.message.from_user.id} tried to interact with /find.")
        return

    # Mode 1: Reply to a photo to search by image
    if update.message.reply_to_message and update.message.reply_to_message.photo:
        photo = update.message.reply_to_message.photo[-1]
        file_ext = os.path.splitext(photo.file_unique_id)[1] or '.jpg'
        temp_file_path = os.path.join(IMAGE_DOWNLOAD_PATH, f"temp_search_{uuid4()}{file_ext}")
        try:
            if not os.path.exists(IMAGE_DOWNLOAD_PATH):
                os.makedirs(IMAGE_DOWNLOAD_PATH, exist_ok=True)
            
            file = await context.bot.get_file(photo.file_id)
            await file.download_to_drive(temp_file_path)
            
            if not os.path.exists(temp_file_path) or os.path.getsize(temp_file_path) == 0:
                logger.error(f"Downloaded file is empty or doesn't exist: {temp_file_path}")
                await update.message.reply_text("下载文件失败，文件为空。", reply_to_message_id=update.message.message_id)
                return
            
            await search_by_image(update, context, temp_file_path)
        except IOError as e:
            logger.error(f"IO error processing search via replied photo: {e}", exc_info=True)
            await update.message.reply_text("下载或读取文件时发生错误。", reply_to_message_id=update.message.message_id)
        except Exception as e:
            logger.error(f"Error processing search via replied photo: {e}", exc_info=True)
            await update.message.reply_text("通过回复图片搜索时发生错误。", reply_to_message_id=update.message.message_id)
        finally:
            if os.path.exists(temp_file_path):
                try:
                    os.remove(temp_file_path)
                    logger.info(f"Cleaned up temporary search file: {temp_file_path}")
                except OSError as e:
                    logger.error(f"Failed to clean up temporary search file {temp_file_path}: {e}")
    
    # Mode 2: Search by keywords (text after /search command)
    elif context.args:
        try:
            # 解析搜索参数
            search_mode = 'exact'  # 默认模式：精确匹配（不分词）
            max_results = MAX_RESULTS  # 默认结果数
            keywords_args = list(context.args)
            
            # 解析参数
            i = 0
            while i < len(keywords_args):
                arg = keywords_args[i]
                if arg.startswith('--'):
                    param = arg[2:]  # 移除 '--'
                    # 支持 --com 作为 --comprehensive 的别名
                    if param == 'com':
                        param = 'comprehensive'
                    if param in ['exact', 'comprehensive', 'contains']:
                        search_mode = param
                        keywords_args.pop(i)
                        continue
                    else:
                        await update.message.reply_text(
                            f"无效的搜索模式: {arg}\n"
                            f"支持的模式: --exact (默认), --comprehensive (--com), --contains",
                            reply_to_message_id=update.message.message_id
                        )
                        return
                elif arg.startswith('-n=') or arg.startswith('--max=') or (arg.startswith('-') and arg[1:].isdigit()):
                    # 解析结果数参数，支持 -n=5, --max=5, -5 三种格式
                    try:
                        if arg.startswith('-n='):
                            max_results = int(arg[3:])
                        elif arg.startswith('--max='):
                            max_results = int(arg[6:])
                        elif arg.startswith('-') and arg[1:].isdigit():
                            # 支持 -5 这种简化格式
                            max_results = int(arg[1:])
                        
                        if max_results <= 0:
                            raise ValueError("结果数必须大于0")
                        keywords_args.pop(i)
                        continue
                    except ValueError as e:
                        await update.message.reply_text(
                            f"无效的结果数参数: {arg}\n"
                            f"请使用 -数字, -n=数字 或 --max=数字 格式，如 -5 或 -n=5",
                            reply_to_message_id=update.message.message_id
                        )
                        return
                i += 1
            
            keywords = " ".join(keywords_args)
            if not keywords.strip():
                await update.message.reply_text(
                    "请提供搜索关键词。\n\n"
                    "用法示例：\n"
                    "• `/find 关键词` (精确匹配，不分词)\n"
                    "• `/find --comprehensive 关键词` 或 `/find --com 关键词` (全面搜索，包含分词)\n"
                    "• `/find --contains 关键词` (内存遍历搜索，最准确)\n"
                    "• `/find -5 关键词` (限制5个结果)\n"
                    "• `/find -n=5 关键词` (限制5个结果)\n"
                    "• `/find --max=10 --com 关键词` (全面搜索，最多10个结果)",
                    parse_mode='Markdown',
                    reply_to_message_id=update.message.message_id
                )
                return
            
            results = searcher.search_by_text(keywords, max_results=max_results, search_mode=search_mode)
            if not results:
                await update.message.reply_text(
                    f"未找到文本匹配结果 (模式: {search_mode})。", 
                    reply_to_message_id=update.message.message_id
                )
                return
            
            # 构建搜索模式说明
            mode_desc = {
                'exact': '精确',
                'comprehensive': '全面', 
                'contains': '包含'
            }.get(search_mode, search_mode)
            
            # 当只有一个结果时
            if len(results) == 1:
                result = results[0]
                if result.get('telegram_message_id'):
                    message = f"找到1个文本匹配结果 ({mode_desc}模式)，原消息ID：{result['telegram_message_id']}"
                    await update.message.reply_text(message, reply_to_message_id=update.message.message_id, parse_mode='HTML')
                else:
                    filename = os.path.basename(result['path'])
                    message = f"找到1个文本匹配结果 ({mode_desc}模式)，文件路径：<code>{filename}</code>"
                    await update.message.reply_text(message, reply_to_message_id=update.message.message_id, parse_mode='HTML')

                    # 发送图片文件
                    try:
                        if os.path.exists(result['path']):
                            with open(result['path'], 'rb') as photo:
                                await context.bot.send_photo(
                                    chat_id=update.effective_chat.id,
                                    photo=InputFile(photo, filename=filename),
                                    caption=f"📁 {filename}",
                                    reply_to_message_id=update.message.message_id
                                )
                    except Exception as e:
                        logger.error(f"发送搜索结果图片失败: {e}")
                        await update.message.reply_text(f"发送图片失败: {filename}")
            else:
                if FIND_PAGINATION_ENABLED and len(results) > 1:
                    query_id = str(uuid4())
                    page_size = get_find_page_size()
                    context.user_data.setdefault("find_pagination", {})[query_id] = {
                        "results": results,
                        "mode": "text",
                        "search_mode": search_mode,
                        "page_size": page_size,
                        "summary": f"找到 {len(results)} 个文本匹配结果 ({mode_desc}模式):",
                        "message_ids": []
                    }
                    await render_find_page(update, context, query_id, 1, is_callback=False)
                    return

                # 当有多个结果时，先回复总数
                await update.message.reply_text(
                    f"找到 {len(results)} 个文本匹配结果 ({mode_desc}模式):",
                    reply_to_message_id=update.message.message_id
                )

                # 分类处理结果：有消息ID的和没有消息ID的
                with_message_id = []
                without_message_id = []

                for result in results:
                    if result.get('telegram_message_id'):
                        with_message_id.append(result)
                    else:
                        without_message_id.append(result)

                # 处理有消息ID的结果 - 合并为一条消息
                if with_message_id:
                    message_lines = []
                    for idx, result in enumerate(with_message_id, 1):
                        message_lines.append(f"{idx}. 原消息ID：{result['telegram_message_id']}")

                    combined_message = "\n".join(message_lines)
                    await context.bot.send_message(
                        chat_id=update.effective_chat.id,
                        text=combined_message,
                        parse_mode='HTML'
                    )

                # 处理没有消息ID的结果 - 单条发送并附带图片
                for idx, result in enumerate(without_message_id, len(with_message_id) + 1):
                    filename = os.path.basename(result['path'])
                    await context.bot.send_message(
                        chat_id=update.effective_chat.id,
                        text=f"{idx}. 文件路径：<code>{filename}</code>",
                        parse_mode='HTML'
                    )

                    # 发送图片文件
                    try:
                        if os.path.exists(result['path']):
                            with open(result['path'], 'rb') as photo:
                                await context.bot.send_photo(
                                    chat_id=update.effective_chat.id,
                                    photo=InputFile(photo, filename=filename),
                                    caption=f"📁 {filename}"
                                )
                    except Exception as e:
                        logger.error(f"发送搜索结果图片失败: {e}")
                        await context.bot.send_message(
                            chat_id=update.effective_chat.id,
                            text=f"⚠️ 发送图片失败: {filename}"
                        )
        except Exception as e:
            logger.error(f"Error during text search: {e}", exc_info=True)
            await update.message.reply_text("文本搜索时发生错误。", reply_to_message_id=update.message.message_id)
    
    # Invalid usage of /find command
    else:
        help_text = """使用方法：
1. <code>/find &lt;关键词&gt;</code> (精确匹配，不分词)
2. <code>/find --comprehensive &lt;关键词&gt;</code> 或 <code>/find --com &lt;关键词&gt;</code> (全面搜索，包含分词)
3. <code>/find --contains &lt;关键词&gt;</code> (内存遍历搜索，最准确)
4. 回复一张图片并发送 <code>/find</code> (图片搜索)"""
        await update.message.reply_text(help_text, parse_mode='HTML', reply_to_message_id=update.message.message_id)


async def handle_find_page_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    if not query or not query.data:
        return

    parts = query.data.split(":")
    if len(parts) != 3:
        await query.answer()
        return

    _, query_id, page_str = parts
    if query_id == "noop":
        await query.answer()
        return

    try:
        page = int(page_str)
    except ValueError:
        await query.answer()
        return

    await query.answer()
    await render_find_page(update, context, query_id, page, is_callback=True)


async def ocr_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    处理 /ocr 命令，立即对所有未OCR的图片进行OCR处理

    与定时任务不同的是，/ocr 会一次性处理所有待处理的图片，
    不受 OCR_BATCH_SIZE 的限制（但内存允许的情况下）
    """
    import gc
    
    logger.info(f"🔤 Received /ocr command from user {update.message.from_user.id}")
    
    if update.message.from_user.id != ALLOWED_USER_ID:
        logger.warning(f"❌ Unauthorized user {update.message.from_user.id} tried to interact with /ocr.")
        return
    
    pending_count = searcher.get_pending_ocr_count(OCR_MAX_RETRIES)
    if pending_count == 0:
        await update.message.reply_text("没有待处理的OCR图片。")
        return
    
    # 发送初始状态消息
    status_message = await update.message.reply_text(
        f"⏳ 开始处理 {pending_count} 张待OCR的图片\n\n"
        f"{create_progress_bar(0, pending_count)}\n"
        f"0/{pending_count} 张已处理"
    )
    
    try:
        # 关键改进：循环处理所有待处理图片，直到完成，并实时更新进度条
        total_stats = {'processed': 0, 'succeeded': 0, 'failed': 0, 'skipped': 0}
        iteration = 0
        max_iterations = 100  # 防止无限循环的安全阈值
        start_time = datetime.now()  # 记录开始时间，用于计算总耗时
        last_update_time = start_time  # 记录上次更新时间，避免过于频繁的 API 调用
        
        while iteration < max_iterations:
            iteration += 1
            remaining = searcher.get_pending_ocr_count(OCR_MAX_RETRIES)
            if remaining == 0:
                logger.info(f"Force OCR: All images processed after {iteration} iterations.")
                break
            
            logger.info(f"Force OCR iteration {iteration}: Processing {remaining} pending images...")
            # Run blocking OCR task in a separate thread
            loop = asyncio.get_running_loop()
            stats = await loop.run_in_executor(
                None, 
                lambda: searcher.process_ocr_pending_images(batch_size=OCR_BATCH_SIZE, max_retries=OCR_MAX_RETRIES)
            )
            
            # 累计统计
            total_stats['processed'] += stats['processed']
            total_stats['succeeded'] += stats['succeeded']
            total_stats['failed'] += stats['failed']
            total_stats['skipped'] += stats['skipped']
            
            # 每处理完一批后，更新进度条（为避免 API 限流，只在有意义的进度时更新，最多每 0.5 秒更新一次）
            now = datetime.now()
            if (now - last_update_time).total_seconds() >= 0.5 or remaining == 0:
                try:
                    # 计算当前耗时
                    elapsed = now - start_time
                    elapsed_str = f"{int(elapsed.total_seconds())}s"
                    
                    progress_text = (
                        f"⏳ 正在处理 {pending_count} 张待OCR的图片\n\n"
                        f"{create_progress_bar(total_stats['processed'], pending_count)}\n"
                        f"{total_stats['processed']}/{pending_count} 张已处理\n\n"
                        f"⏱️ 已用时: {elapsed_str}"
                    )
                    await context.bot.edit_message_text(
                        chat_id=update.effective_chat.id,
                        message_id=status_message.message_id,
                        text=progress_text
                    )
                    last_update_time = now
                except Exception as e:
                    logger.debug(f"Failed to update progress message: {e}")
            
            # 如果本轮没有处理任何图片，说明都是失败的，避免无限循环
            if stats['processed'] == 0:
                logger.warning(f"No images were processed in iteration {iteration}, stopping.")
                break
            
            # 每批次处理后显式触发垃圾回收，及时释放内存
            # 注意：OCR引擎采用懒加载模式，每批处理完会自动清理，下次需要时自动加载
            gc.collect()
        
        # 计算总耗时
        end_time = datetime.now()
        total_elapsed = end_time - start_time
        elapsed_minutes = int(total_elapsed.total_seconds() // 60)
        elapsed_seconds = int(total_elapsed.total_seconds() % 60)
        total_time_str = f"{elapsed_minutes}m {elapsed_seconds}s" if elapsed_minutes > 0 else f"{elapsed_seconds}s"
        
        # 构建详细的反馈消息
        message = (
            f"✅ OCR处理完成！\n\n"
            f"{create_progress_bar(total_stats['processed'], pending_count)}\n"
            f"总计：{total_stats['processed']}/{pending_count} 张处理\n\n"
            f"📊 处理统计:\n"
            f"  成功: {total_stats['succeeded']}\n"
            f"  失败: {total_stats['failed']}\n"
            f"  跳过: {total_stats['skipped']}\n"
            f"  迭代次数: {iteration}\n\n"
            f"⏱️ 总耗时: {total_time_str}"
        )
        
        # 添加失败处理说明
        if total_stats['failed'] > 0:
            message += (
                f"\n\n⚠️ 注意：\n"
                f"有 {total_stats['failed']} 张图片 OCR 失败。\n"
                f"这些图片会在下次定时任务中自动重试（最多 {OCR_MAX_RETRIES} 次）。\n"
                f"如果仍然失败，可能原因：\n"
                f"  • 图片质量差或文字不清楚\n"
                f"  • OCR 模型异常\n"
                f"  • 服务器资源不足"
            )
        
        # 添加成功提示
        if total_stats['succeeded'] > 0:
            message += (
                f"\n\n✨ {total_stats['succeeded']} 张图片已可进行文本搜索\n"
                f"使用 /search 关键词 即可搜索"
            )
        
        # 更新最终消息
        await context.bot.edit_message_text(
            chat_id=update.effective_chat.id,
            message_id=status_message.message_id,
            text=message
        )
        logger.info(f"Force OCR completed: {total_stats}, iterations: {iteration}")
        
        # 最终垃圾回收，确保所有OCR处理产生的临时对象被清理
        gc.collect()
        logger.info(f"Memory cleanup completed after force OCR")
        
    except Exception as e:
        logger.error(f"Error during force OCR: {e}", exc_info=True)
        error_message = f"❌ OCR处理出现错误: {str(e)}\n\n请检查日志文件或重试。"
        try:
            await context.bot.edit_message_text(
                chat_id=update.effective_chat.id,
                message_id=status_message.message_id,
                text=error_message
            )
        except:
            await update.message.reply_text(error_message)


async def tag_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    处理 /tag 命令，手动设置OCR结果。
    命令用法：回复一张图片并发送 "/tag 文本内容"
    例如：/tag 猫 薛条 可爱
    """
    logger.info(f"🏷️ Received /tag command from user {update.message.from_user.id}")
    
    if update.message.from_user.id != ALLOWED_USER_ID:
        logger.warning(f"❌ Unauthorized user {update.message.from_user.id} tried to interact with /tag.")
        return
    
    # 检查是否回复了一个消息
    if not update.message.reply_to_message:
        await update.message.reply_text(
            "请回复一个包含图片的消息并使用 /tag 命令。\n\n"
            "用法：回复图片后发送 `/tag 文本内容`",
            parse_mode='Markdown',
            reply_to_message_id=update.message.message_id
        )
        return
    
    # 检查回复的消息是否包含图片
    replied_message = update.message.reply_to_message
    if not replied_message.photo:
        await update.message.reply_text(
            "请回复一个包含图片的消息。",
            reply_to_message_id=update.message.message_id
        )
        return
    
    # 获取OCR文本内容
    if not context.args:
        await update.message.reply_text(
            "请提供OCR文本内容。\n\n"
            "用法：`/tag 文本内容`",
            parse_mode='Markdown',
            reply_to_message_id=update.message.message_id
        )
        return
    
    # 连接所有参数作为OCR文本
    ocr_text = " ".join(context.args)
    
    try:
        # 获取回复消息的Telegram消息ID
        replied_message_id = replied_message.message_id
        
        # 构造数据库中的telegram_message_id
        # 需要查找数据库中相应的记录，可能需要通过图片特征查找
        
        # 首先下载图片并获取其特征
        photo = replied_message.photo[-1]
        file_ext = os.path.splitext(photo.file_unique_id)[1] or '.jpg'
        temp_file_path = os.path.join(IMAGE_DOWNLOAD_PATH, f"temp_setocr_{uuid4()}{file_ext}")
        
        try:
            if not os.path.exists(IMAGE_DOWNLOAD_PATH):
                os.makedirs(IMAGE_DOWNLOAD_PATH, exist_ok=True)
            
            file = await context.bot.get_file(photo.file_id)
            await file.download_to_drive(temp_file_path)
            
            if not os.path.exists(temp_file_path) or os.path.getsize(temp_file_path) == 0:
                logger.error(f"Downloaded file is empty or doesn't exist: {temp_file_path}")
                await update.message.reply_text(
                    "下载图片失败，无法设置OCR。",
                    reply_to_message_id=update.message.message_id
                )
                return
            
            # 通过图片特征查找数据库中的记录
            similar_results = searcher.search_similar_images(temp_file_path, threshold=0, max_results=1)
            
            if not similar_results or similar_results[0].get('similarity') != 1.0:
                await update.message.reply_text(
                    "未在数据库中找到该图片的记录。\n\n"
                    "请确认该图片已经被索引。",
                    reply_to_message_id=update.message.message_id
                )
                return
            
            # 获取图片的file_hash和telegram_message_id
            image_record = similar_results[0]
            file_hash = image_record.get('file_hash')
            telegram_message_id_in_db = image_record.get('telegram_message_id')
            
            # 通过file_hash设置OCR结果（支持没有message_id的图片）
            success = searcher.set_manual_ocr_result_by_hash(file_hash, ocr_text)
            
            if success:
                pending_count = searcher.get_pending_ocr_count(OCR_MAX_RETRIES)
                msg_info = f"消息ID: {telegram_message_id_in_db}" if telegram_message_id_in_db else "(无消息ID)"
                # 使用 HTML 格式避免 Markdown 特殊字符解析问题
                import html
                escaped_ocr_text = html.escape(ocr_text)
                await update.message.reply_text(
                    f"✅ OCR结果已成功设置。\n\n"
                    f"OCR内容: <code>{escaped_ocr_text}</code>\n"
                    f"{msg_info}\n"
                    f"当前待处理OCR图片数: {pending_count}",
                    parse_mode='HTML',
                    reply_to_message_id=update.message.message_id
                )
                logger.info(f"User manually set OCR result for file_hash {file_hash}: '{ocr_text}'")
            else:
                await update.message.reply_text(
                    "❌ 设置OCR结果失败，请检查日志。",
                    reply_to_message_id=update.message.message_id
                )
        
        finally:
            # 清理临时文件
            if os.path.exists(temp_file_path):
                try:
                    os.remove(temp_file_path)
                    logger.info(f"Cleaned up temporary file: {temp_file_path}")
                except OSError as e:
                    logger.error(f"Failed to clean up temporary file {temp_file_path}: {e}")
    
    except Exception as e:
        logger.error(f"Error in tag_command: {e}", exc_info=True)
        await update.message.reply_text(
            "处理/tag命令时发生错误，请检查日志。",
            reply_to_message_id=update.message.message_id
        )


async def setmessageid_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    处理 /link 命令，为图片设置Telegram消息ID。
    命令用法：回复一张图片并发送 "/link <消息ID或链接>"
    例如：/link https://t.me/channel/12345
    """
    if update.message.from_user.id != ALLOWED_USER_ID:
        logger.warning(f"Unauthorized user {update.message.from_user.id} tried to interact with /link.")
        return
    
    # 检查是否回复了一个消息
    if not update.message.reply_to_message:
        await update.message.reply_text(
            "请回复一个包含图片的消息并使用 /link 命令。\n\n"
            "用法：回复图片后发送 `/link <消息ID或链接>`",
            parse_mode='Markdown',
            reply_to_message_id=update.message.message_id
        )
        return
    
    # 检查回复的消息是否包含图片
    replied_message = update.message.reply_to_message
    if not replied_message.photo:
        await update.message.reply_text(
            "请回复一个包含图片的消息。",
            reply_to_message_id=update.message.message_id
        )
        return
    
    # 获取消息ID
    if not context.args:
        await update.message.reply_text(
            "请提供消息ID或链接。\n\n"
            "用法：`/link <消息ID或链接>`",
            parse_mode='Markdown',
            reply_to_message_id=update.message.message_id
        )
        return
    
    # 连接所有参数作为消息ID
    message_id = " ".join(context.args)
    
    try:
        # 下载图片并获取其特征
        photo = replied_message.photo[-1]
        file_ext = os.path.splitext(photo.file_unique_id)[1] or '.jpg'
        temp_file_path = os.path.join(IMAGE_DOWNLOAD_PATH, f"temp_setmsgid_{uuid4()}{file_ext}")
        
        try:
            if not os.path.exists(IMAGE_DOWNLOAD_PATH):
                os.makedirs(IMAGE_DOWNLOAD_PATH, exist_ok=True)
            
            file = await context.bot.get_file(photo.file_id)
            await file.download_to_drive(temp_file_path)
            
            if not os.path.exists(temp_file_path) or os.path.getsize(temp_file_path) == 0:
                logger.error(f"Downloaded file is empty or doesn't exist: {temp_file_path}")
                await update.message.reply_text(
                    "下载图片失败，无法设置消息ID。",
                    reply_to_message_id=update.message.message_id
                )
                return
            
            # 通过图片特征查找数据库中的记录
            similar_results = searcher.search_similar_images(temp_file_path, threshold=0, max_results=1)
            
            if not similar_results or similar_results[0].get('similarity') != 1.0:
                await update.message.reply_text(
                    "未在数据库中找到该图片的记录。\n\n"
                    "请确认该图片已经被索引。",
                    reply_to_message_id=update.message.message_id
                )
                return
            
            # 获取图片信息
            image_record = similar_results[0]
            file_hash = image_record.get('file_hash')
            existing_message_id = image_record.get('telegram_message_id')
            
            # 检查是否已有消息ID
            if existing_message_id:
                await update.message.reply_text(
                    f"该图片已有消息ID：{existing_message_id}\n\n"
                    f"无法覆盖已存在的消息ID。",
                    reply_to_message_id=update.message.message_id
                )
                return
            
            # 设置消息ID
            success = searcher.set_message_id_by_hash(file_hash, message_id)
            
            if success:
                await update.message.reply_text(
                    f"✅ 消息ID已成功设置。\n\n"
                    f"消息ID: `{message_id}`",
                    parse_mode='Markdown',
                    reply_to_message_id=update.message.message_id
                )
                logger.info(f"User manually set message_id for file_hash {file_hash}: '{message_id}'")
            else:
                await update.message.reply_text(
                    "❌ 设置消息ID失败，请检查日志。",
                    reply_to_message_id=update.message.message_id
                )
        
        finally:
            # 清理临时文件
            if os.path.exists(temp_file_path):
                try:
                    os.remove(temp_file_path)
                    logger.info(f"Cleaned up temporary file: {temp_file_path}")
                except OSError as e:
                    logger.error(f"Failed to clean up temporary file {temp_file_path}: {e}")
    
    except Exception as e:
        logger.error(f"Error in setmessageid_command: {e}", exc_info=True)
        await update.message.reply_text(
            "处理/link命令时发生错误，请检查日志。",
            reply_to_message_id=update.message.message_id
        )


async def untag_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    处理 /untag 命令，清除OCR结果。
    命令用法：回复一张图片并发送 "/untag"
    """
    if update.message.from_user.id != ALLOWED_USER_ID:
        logger.warning(f"Unauthorized user {update.message.from_user.id} tried to interact with /untag.")
        return
    
    # 检查是否回复了一个消息
    if not update.message.reply_to_message:
        await update.message.reply_text(
            "请回复一个包含图片的消息并使用 /untag 命令。",
            reply_to_message_id=update.message.message_id
        )
        return
    
    # 检查回复的消息是否包含图片
    replied_message = update.message.reply_to_message
    if not replied_message.photo:
        await update.message.reply_text(
            "请回复一个包含图片的消息。",
            reply_to_message_id=update.message.message_id
        )
        return
    
    try:
        # 下载图片并获取其特征
        photo = replied_message.photo[-1]
        file_ext = os.path.splitext(photo.file_unique_id)[1] or '.jpg'
        temp_file_path = os.path.join(IMAGE_DOWNLOAD_PATH, f"temp_clearocr_{uuid4()}{file_ext}")
        
        try:
            if not os.path.exists(IMAGE_DOWNLOAD_PATH):
                os.makedirs(IMAGE_DOWNLOAD_PATH, exist_ok=True)
            
            file = await context.bot.get_file(photo.file_id)
            await file.download_to_drive(temp_file_path)
            
            if not os.path.exists(temp_file_path) or os.path.getsize(temp_file_path) == 0:
                logger.error(f"Downloaded file is empty or doesn't exist: {temp_file_path}")
                await update.message.reply_text(
                    "下载图片失败，无法清除OCR。",
                    reply_to_message_id=update.message.message_id
                )
                return
            
            # 通过图片特征查找数据库中的记录
            similar_results = searcher.search_similar_images(temp_file_path, threshold=0, max_results=1)
            
            if not similar_results or similar_results[0].get('similarity') != 1.0:
                await update.message.reply_text(
                    "未在数据库中找到该图片的记录。\n\n"
                    "请确认该图片已经被索引。",
                    reply_to_message_id=update.message.message_id
                )
                return
            
            # 获取图片的telegram_message_id
            image_record = similar_results[0]
            telegram_message_id_in_db = image_record.get('telegram_message_id')
            
            if not telegram_message_id_in_db:
                await update.message.reply_text(
                    "该图片没有对应的Telegram消息ID，无法清除OCR。",
                    reply_to_message_id=update.message.message_id
                )
                return
            
            # 清除OCR结果
            success = searcher.clear_ocr_result(telegram_message_id_in_db)
            
            if success:
                pending_count = searcher.get_pending_ocr_count(OCR_MAX_RETRIES)
                await update.message.reply_text(
                    f"✅ OCR结果已成功清除。\n\n"
                    f"该图片的OCR状态已重置为pending。\n"
                    f"当前待处理OCR图片数: {pending_count}",
                    reply_to_message_id=update.message.message_id
                )
                logger.info(f"User manually cleared OCR result for message_id {telegram_message_id_in_db}")
            else:
                await update.message.reply_text(
                    "❌ 清除OCR结果失败，请检查日志。",
                    reply_to_message_id=update.message.message_id
                )
        
        finally:
            # 清理临时文件
            if os.path.exists(temp_file_path):
                try:
                    os.remove(temp_file_path)
                    logger.info(f"Cleaned up temporary file: {temp_file_path}")
                except OSError as e:
                    logger.error(f"Failed to clean up temporary file {temp_file_path}: {e}")
    
    except Exception as e:
        logger.error(f"Error in untag_command: {e}", exc_info=True)
        await update.message.reply_text(
            "处理/untag命令时发生错误，请检查日志。",
            reply_to_message_id=update.message.message_id
        )


async def getocr_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    处理 /getocr 命令，查询图片的OCR结果。
    命令用法：
    1. 回复一张图片并发送 "/getocr"
    2. 或使用 "/getocr -l <消息ID>" 直接查询
    """
    if update.message.from_user.id != ALLOWED_USER_ID:
        logger.warning(f"Unauthorized user {update.message.from_user.id} tried to interact with /getocr.")
        return
    
    # 检查是否使用 -l 参数
    message_id_from_arg = None
    if context.args:
        # 解析参数
        i = 0
        while i < len(context.args):
            arg = context.args[i]
            if arg == '-l' and i + 1 < len(context.args):
                message_id_from_arg = context.args[i + 1]
                break
            i += 1
    
    # 模式1: 使用 -l 参数直接查询
    if message_id_from_arg:
        try:
            ocr_text = searcher.get_ocr_by_message_id(message_id_from_arg)
            
            if ocr_text is None:
                await update.message.reply_text(
                    f"❌ 未找到消息ID为 `{message_id_from_arg}` 的图片记录。",
                    parse_mode='Markdown',
                    reply_to_message_id=update.message.message_id
                )
            elif not ocr_text or ocr_text.strip() == '':
                await update.message.reply_text(
                    f"❌ 消息ID `{message_id_from_arg}` 对应的图片没有OCR结果。",
                    parse_mode='Markdown',
                    reply_to_message_id=update.message.message_id
                )
            else:
                response = f"✅ OCR结果：\n\n`{ocr_text}`"
                await update.message.reply_text(
                    response,
                    parse_mode='Markdown',
                    reply_to_message_id=update.message.message_id
                )
                logger.info(f"User queried OCR result by message_id {message_id_from_arg}: '{ocr_text[:50]}...'")
        except Exception as e:
            logger.error(f"Error querying OCR by message_id: {e}", exc_info=True)
            await update.message.reply_text(
                "查询OCR结果时发生错误，请检查日志。",
                reply_to_message_id=update.message.message_id
            )
        return
    
    # 模式2: 回复消息查询
    # 检查是否回复了一个消息
    if not update.message.reply_to_message:
        await update.message.reply_text(
            "请使用以下方式之一：\n"
            "1. 回复一张图片并发送 /getocr\n"
            "2. 使用 /getocr -l <消息ID>",
            reply_to_message_id=update.message.message_id
        )
        return
    
    # 检查回复的消息是否包含图片
    replied_message = update.message.reply_to_message
    if not replied_message.photo:
        await update.message.reply_text(
            "请回复一个包含图片的消息。",
            reply_to_message_id=update.message.message_id
        )
        return
    
    try:
        # 下载图片并获取其特征
        photo = replied_message.photo[-1]
        file_ext = os.path.splitext(photo.file_unique_id)[1] or '.jpg'
        temp_file_path = os.path.join(IMAGE_DOWNLOAD_PATH, f"temp_getocr_{uuid4()}{file_ext}")
        
        try:
            if not os.path.exists(IMAGE_DOWNLOAD_PATH):
                os.makedirs(IMAGE_DOWNLOAD_PATH, exist_ok=True)
            
            file = await context.bot.get_file(photo.file_id)
            await file.download_to_drive(temp_file_path)
            
            if not os.path.exists(temp_file_path) or os.path.getsize(temp_file_path) == 0:
                logger.error(f"Downloaded file is empty or doesn't exist: {temp_file_path}")
                await update.message.reply_text(
                    "下载图片失败，无法查询OCR。",
                    reply_to_message_id=update.message.message_id
                )
                return
            
            # 通过图片特征查找数据库中的记录
            similar_results = searcher.search_similar_images(temp_file_path, threshold=0, max_results=1)
            
            if not similar_results or similar_results[0].get('similarity') != 1.0:
                await update.message.reply_text(
                    "未在数据库中找到该图片的记录。\n\n"
                    "请确认该图片已经被索引。",
                    reply_to_message_id=update.message.message_id
                )
                return
            
            # 获取图片记录
            image_record = similar_results[0]
            ocr_text = image_record.get('ocr_text', '')
            
            # 检查OCR结果
            if not ocr_text or ocr_text.strip() == '':
                await update.message.reply_text(
                    "❌ 该图片没有OCR结果。",
                    reply_to_message_id=update.message.message_id
                )
            else:
                # 返回OCR结果
                response = f"✅ OCR结果：\n\n`{ocr_text}`"
                await update.message.reply_text(
                    response,
                    parse_mode='Markdown',
                    reply_to_message_id=update.message.message_id
                )
                logger.info(f"User queried OCR result: '{ocr_text[:50]}...'")
        
        finally:
            # 清理临时文件
            if os.path.exists(temp_file_path):
                try:
                    os.remove(temp_file_path)
                    logger.info(f"Cleaned up temporary file: {temp_file_path}")
                except OSError as e:
                    logger.error(f"Failed to clean up temporary file {temp_file_path}: {e}")
    
    except Exception as e:
        logger.error(f"Error in getocr_command: {e}", exc_info=True)
        await update.message.reply_text(
            "处理/getocr命令时发生错误，请检查日志。",
            reply_to_message_id=update.message.message_id
        )


async def failed_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    处理 /failed 命令，获取OCR失败的记录列表。
    通过回复历史消息的方式显示，用户点击引用即可跳转到对应图片。
    
    用法：
    - /failed          显示默认数量的失败记录
    - /failed -5       显示前5条失败记录
    - /failed -a       显示所有失败记录
    - /failed -all     显示所有失败记录
    """
    logger.info(f"📋 Received /failed command from user {update.message.from_user.id}")
    
    if update.message.from_user.id != ALLOWED_USER_ID:
        logger.warning(f"❌ Unauthorized user {update.message.from_user.id} tried to interact with /failed.")
        return
    
    # 解析参数
    limit = FAILED_OCR_DEFAULT_LIMIT  # 默认值
    show_all = False
    
    if context.args:
        arg = context.args[0].lower()
        if arg in ['-a', '-all', '--all']:
            show_all = True
            limit = None
        elif arg.startswith('-') and arg[1:].isdigit():
            limit = int(arg[1:])
        elif arg.isdigit():
            limit = int(arg)
    
    # 获取失败记录总数
    failed_count = searcher.get_failed_ocr_count()
    
    if failed_count == 0:
        await update.message.reply_text(
            "✅ 当前没有OCR失败的记录。",
            reply_to_message_id=update.message.message_id
        )
        return
    
    # 获取失败记录
    records = searcher.get_failed_ocr_records(limit=limit if not show_all else None)
    
    if not records:
        await update.message.reply_text(
            "✅ 当前没有OCR失败的记录。",
            reply_to_message_id=update.message.message_id
        )
        return
    
    # 先发送概要信息
    if show_all:
        summary = f"📋 OCR失败记录（全部 {len(records)} 条）\n\n以下将逐条显示，点击引用可跳转到对应图片："
    else:
        summary = f"📋 OCR失败记录（显示 {len(records)}/{failed_count} 条）\n\n以下将逐条显示，点击引用可跳转到对应图片："
    
    await update.message.reply_text(
        summary,
        reply_to_message_id=update.message.message_id
    )
    
    # 逐条发送，通过回复历史消息的方式
    sent_count = 0
    skipped_count = 0
    
    for idx, record in enumerate(records, 1):
        file_name = os.path.basename(record['file_path'])
        fail_count = record['ocr_fail_count']
        
        # 从文件名中提取消息ID（格式: {message_id}_{file_unique_id}.{ext}）
        msg_id_from_filename = None
        if '_' in file_name:
            parts = file_name.split('_')
            if parts[0].isdigit():
                msg_id_from_filename = int(parts[0])
        
        # 更新时间格式化
        update_time = ""
        if record['updated_time']:
            update_time = datetime.fromtimestamp(record['updated_time']).strftime('%m-%d %H:%M')
        
        # 构建消息内容
        message_text = (
            f"⚠️ 失败记录 #{idx}\n"
            f"失败次数: {fail_count}\n"
            f"更新时间: {update_time}\n"
            f"💡 回复此图片使用 /tag 设置标签"
        )
        
        if msg_id_from_filename:
            try:
                # 通过回复历史消息发送，用户点击引用即可跳转
                await context.bot.send_message(
                    chat_id=update.effective_chat.id,
                    text=message_text,
                    reply_to_message_id=msg_id_from_filename
                )
                sent_count += 1
                
                # 添加短暂延迟，避免发送过快被限流
                if idx < len(records):
                    await asyncio.sleep(0.3)
                    
            except Exception as e:
                # 如果回复失败（比如原消息已被删除），记录跳过
                logger.warning(f"Failed to reply to message {msg_id_from_filename}: {e}")
                skipped_count += 1
        else:
            # 没有消息ID，直接发送文件名信息
            await context.bot.send_message(
                chat_id=update.effective_chat.id,
                text=f"⚠️ 失败记录 #{idx}\n文件: `{file_name}`\n失败次数: {fail_count}\n更新时间: {update_time}\n⚠️ 无法定位原消息",
                parse_mode='Markdown'
            )
            sent_count += 1
            skipped_count += 1
    
    # 发送完成统计
    complete_msg = f"✅ 已显示 {sent_count} 条失败记录"
    if skipped_count > 0:
        complete_msg += f"\n⚠️ {skipped_count} 条无法定位原消息"
    if not show_all and failed_count > len(records):
        complete_msg += f"\n📌 使用 /failed -a 查看全部 {failed_count} 条记录"
    
    await context.bot.send_message(
        chat_id=update.effective_chat.id,
        text=complete_msg
    )


async def scheduled_ocr_task(context: ContextTypes.DEFAULT_TYPE):
    """
    定时执行OCR任务 - 处理所有待处理的图片
    
    为了避免OCR任务积压，本任务会循环调用process_ocr_pending_images，
    直到所有待处理的图片都被处理完成。
    
    修复：添加超时保护和完善的错误处理，防止任务卡死导致程序无响应
    """
    import gc
    from concurrent.futures import ThreadPoolExecutor
    
    task_start_time = datetime.now()
    logger.info(f"Scheduled OCR task started at: {task_start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    
    # 创建专用的线程池执行器，确保可以正确清理
    executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="ocr_scheduled")
    
    # 定义网络操作超时时间（秒）
    NETWORK_TIMEOUT = 30.0
    
    async def safe_send_message(text: str) -> bool:
        """带超时保护的消息发送"""
        try:
            await asyncio.wait_for(
                context.bot.send_message(chat_id=ALLOWED_USER_ID, text=text),
                timeout=NETWORK_TIMEOUT
            )
            return True
        except asyncio.TimeoutError:
            logger.error(f"Timeout sending message: {text[:50]}...")
            return False
        except Exception as e:
            logger.error(f"Error sending message: {e}")
            return False
    
    try:
        pending_count = searcher.get_pending_ocr_count(OCR_MAX_RETRIES)
        if pending_count == 0:
            logger.info("Scheduled OCR task: No pending images.")
            await safe_send_message(
                f"✅ 定时OCR任务完成\n当前无待处理图片\n执行时间: {task_start_time.strftime('%Y-%m-%d %H:%M:%S')}"
            )
            return
        
        logger.info(f"Starting scheduled OCR task for {pending_count} images...")
        
        # 关键改进：循环处理，直到没有待处理的图片
        total_stats = {'processed': 0, 'succeeded': 0, 'failed': 0, 'skipped': 0}
        iteration = 0
        max_iterations = 100  # 添加最大迭代次数防护
        
        while iteration < max_iterations:
            iteration += 1
            remaining = searcher.get_pending_ocr_count(OCR_MAX_RETRIES)
            if remaining == 0:
                logger.info(f"All pending images have been processed after {iteration} iterations.")
                break
            
            logger.info(f"OCR task iteration {iteration}: Processing {remaining} pending images...")
            
            # 使用专用执行器运行阻塞的 OCR 任务
            loop = asyncio.get_running_loop()
            try:
                # 添加超时保护：每批次最多处理 10 分钟
                stats = await asyncio.wait_for(
                    loop.run_in_executor(
                        executor, 
                        lambda: searcher.process_ocr_pending_images(batch_size=OCR_BATCH_SIZE, max_retries=OCR_MAX_RETRIES)
                    ),
                    timeout=600.0  # 10 分钟超时
                )
            except asyncio.TimeoutError:
                logger.error(f"OCR batch processing timeout in iteration {iteration}")
                total_stats['failed'] += OCR_BATCH_SIZE  # 估算失败数量
                break
            
            # 累计统计
            total_stats['processed'] += stats['processed']
            total_stats['succeeded'] += stats['succeeded']
            total_stats['failed'] += stats['failed']
            total_stats['skipped'] += stats['skipped']
            
            # 如果本轮没有处理任何图片，说明都是失败的，避免无限循环
            if stats['processed'] == 0:
                logger.warning(f"No images were processed in iteration {iteration}, stopping to avoid infinite loop.")
                break
            
            logger.info(f"Iteration {iteration} completed: {stats}")
            
            # 每批次处理后显式触发垃圾回收
            gc.collect()
            
            # 添加心跳日志，证明程序仍在运行
            logger.info(f"💓 Heartbeat: OCR task still running after iteration {iteration}")
        
        # 计算任务耗时
        task_end_time = datetime.now()
        task_duration = task_end_time - task_start_time
        duration_str = f"{int(task_duration.total_seconds())}s"
        
        # 发送完整的统计信息
        message = (
            f"✅ 定时OCR任务已完成\n\n"
            f"📊 处理统计:\n"
            f"总处理数: {total_stats['processed']}\n"
            f"成功: {total_stats['succeeded']}\n"
            f"失败: {total_stats['failed']}\n"
            f"跳过: {total_stats['skipped']}\n"
            f"迭代次数: {iteration}\n\n"
            f"⏱️ 执行信息:\n"
            f"开始时间: {task_start_time.strftime('%H:%M:%S')}\n"
            f"结束时间: {task_end_time.strftime('%H:%M:%S')}\n"
            f"执行耗时: {duration_str}"
        )
        
        if total_stats['failed'] > 0:
            message += (
                f"\n\n⚠️ 注意：有 {total_stats['failed']} 张图片 OCR 失败。"
                f"这些图片会在后续任务中继续重试（最多 {OCR_MAX_RETRIES} 次）。"
            )
        
        await safe_send_message(message)
        logger.info(f"Scheduled OCR task completed successfully: {total_stats}, iterations: {iteration}, duration: {duration_str}")
        
    except Exception as e:
        task_duration = datetime.now() - task_start_time
        duration_str = f"{int(task_duration.total_seconds())}s"
        
        logger.error(f"Error in scheduled OCR task: {e}", exc_info=True)
        
        error_message = (
            f"❌ 定时OCR任务出现错误\n\n"
            f"错误信息: {str(e)}\n"
            f"执行时间: {task_start_time.strftime('%Y-%m-%d %H:%M:%S')}\n"
            f"失败耗时: {duration_str}\n\n"
            f"请检查日志获取详细错误信息。"
        )
        await safe_send_message(error_message)
        
    finally:
        # 关键修复：确保执行器被正确关闭，释放线程资源
        try:
            executor.shutdown(wait=False, cancel_futures=True)
            logger.info("OCR executor shutdown completed")
        except Exception as e:
            logger.error(f"Error shutting down executor: {e}")
        
        # 最终垃圾回收
        gc.collect()
        
        # 确保记录任务结束，无论成功还是失败
        task_end_time = datetime.now()
        total_duration = (task_end_time - task_start_time).total_seconds()
        logger.info(f"🏁 Scheduled OCR task cleanup completed. Total duration: {total_duration:.1f}s")



def parse_scheduled_time(time_str: str) -> Optional[time]:
    """
    解析时间字符串 (格式: HH:MM) 为 time 对象
    注意：python-telegram-bot 的调度器使用UTC时间，
    但我们希望使用北京时间(UTC+8)来配置定时任务时间。
    因此需要将北京时间转换为UTC时间。
    """
    try:
        hour, minute = map(int, time_str.split(':'))
        # 创建北京时间的时间对象
        beijing_time = time(hour=hour, minute=minute)
        
        # 将北京时间转换为UTC时间
        # 北京时间减8小时等于UTC时间
        utc_hour = (hour - 8) % 24
        utc_time = time(hour=utc_hour, minute=minute)
        
        logger.info(f"Scheduled time converted: Beijing {time_str} -> UTC {utc_time.strftime('%H:%M')}")
        return utc_time
    except (ValueError, AttributeError):
        logger.error(f"Invalid time format: {time_str}. Expected HH:MM")
        return None


async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    全局错误处理器，处理所有未捕获的异常
    重要：此处理器会捕获网络错误，防止bot完全无响应
    """
    logger.error(f"Update {update} caused error {context.error}", exc_info=context.error)
    
    # 如果是网络错误，记录但继续运行（不会中断bot的polling）
    if isinstance(context.error, Exception):
        error_name = context.error.__class__.__name__
        error_msg = str(context.error)
        logger.warning(f"Network/Connection error occurred: {error_name}: {error_msg}. Bot will continue polling...")


def signal_handler(signum, frame):
    """
    信号处理函数，用于优雅退出
    
    Args:
        signum: 信号编号
        frame: 当前栈帧
    """
    logger.info(f"收到信号 {signum}，正在关闭机器人...")
    sys.exit(0)


def create_application():
    """
    创建并配置 Telegram Application 实例。
    
    将此逻辑封装为函数，以便在网络错误时可以重新创建整个 application，
    包括新的连接池，解决连接池损坏后无法恢复的问题。
    
    Returns:
        Application: 配置好的 Telegram Application 实例
    """
    from telegram.ext import ApplicationBuilder
    from telegram.request import HTTPXRequest
    
    # 创建自定义请求对象，增大连接池和超时时间
    # 关键修复：禁用HTTP/2以避免代理环境下的TLS握手错误
    request = HTTPXRequest(
        connection_pool_size=30,       # 增大连接池（默认1）
        read_timeout=45.0,             # 读取超时（秒）
        write_timeout=45.0,            # 写入超时（秒）
        connect_timeout=45.0,          # 连接超时（秒）
        pool_timeout=20.0,             # 连接池等待超时（秒）
        http_version="1.1",            # 禁用HTTP/2，使用HTTP/1.1以提高代理兼容性
    )
    
    app = (
        ApplicationBuilder()
        .token(BOT_TOKEN)
        .request(request)
        .get_updates_request(HTTPXRequest(
            connection_pool_size=20,   # get_updates 专用连接池
            read_timeout=90.0,         # 长轮询需要更长的读取超时
            write_timeout=45.0,
            connect_timeout=45.0,
            pool_timeout=20.0,
            http_version="1.1",        # 禁用HTTP/2，避免TLS握手错误
        ))
        .build()
    )
    
    # 注册全局错误处理器
    app.add_error_handler(error_handler)
    
    # Add handlers - 新命令体系，首字母即可区分
    app.add_handler(CallbackQueryHandler(handle_find_page_callback, pattern=r"^find_page:"))
    app.add_handler(CommandHandler('find', find_command))      # 搜索（替代search）
    app.add_handler(CommandHandler('ocr', ocr_command))        # OCR处理（替代forceOCR）
    app.add_handler(CommandHandler('tag', tag_command))        # 设置标签（替代setocr）
    app.add_handler(CommandHandler('untag', untag_command))    # 清除标签（替代clearocr）
    app.add_handler(CommandHandler('link', setmessageid_command))  # 设置消息ID（新命令）
    app.add_handler(CommandHandler('getocr', getocr_command))  # 查询OCR结果（新命令）
    app.add_handler(CommandHandler('failed', failed_command))  # 查询OCR失败记录（新命令）
    # handle_photo processes all photo messages, internal logic decides add or search
    app.add_handler(MessageHandler(filters.PHOTO, handle_photo))
    
    # Add scheduled OCR task
    # 注意：定时任务使用北京时间(UTC+8)配置，实际调度时间会自动转换为UTC
    scheduled_ocr_time = parse_scheduled_time(OCR_SCHEDULED_TIME)
    if scheduled_ocr_time:
        job_queue = app.job_queue
        
        # 添加定时任务（只注册一次）
        job_queue.run_daily(
            scheduled_ocr_task, 
            time=scheduled_ocr_time,
            name="daily_ocr_task"  # 给任务命名，防止重复注册
        )
        
        logger.info(f"✅ Scheduled daily OCR task at Beijing time {OCR_SCHEDULED_TIME} (UTC {scheduled_ocr_time.strftime('%H:%M')})")
    else:
        logger.warning(f"Failed to parse OCR scheduled time: {OCR_SCHEDULED_TIME}")
    
    return app


if __name__ == '__main__':
    # 注册信号处理器
    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)  # 同时处理Ctrl+C
    
    logger.info("Starting bot...")
    
    # 启动 Bot
    logger.info("🤖 机器人启动中...")
    
    # 使用try-except包装polling，确保网络错误时bot能够恢复
    # 关键修复：每次重试时重新创建application，确保连接池被完全重建
    retry_count = 0
    max_retries = 10  # 增加最大重试次数，提高容错能力
    base_retry_interval = 15  # 基础重试间隔（秒）
    application = None
    
    while True:
        try:
            # 每次循环都重新创建 application，确保连接池是全新的
            # 这是解决连接池损坏后无法恢复的关键
            if application is None or retry_count > 0:
                logger.info(f"创建新的Application实例 (重试次数: {retry_count})...")
                application = create_application()
            
            logger.info("开始polling...")
            application.run_polling(allowed_updates=Update.ALL_TYPES, drop_pending_updates=False)
            break  # 如果正常退出，就break
        except KeyboardInterrupt:
            logger.info("收到键盘中断信号，正在优雅关闭...")
            break
        except Exception as e:
            retry_count += 1
            error_name = e.__class__.__name__
            error_msg = str(e)
            logger.error(f"Polling出错 ({retry_count}/{max_retries}): {error_name}: {error_msg}", exc_info=True)
            
            # 重置 application 为 None，强制下次循环重新创建
            # 这确保了损坏的连接池会被丢弃
            application = None
            
            if retry_count >= max_retries:
                logger.error(f"已达到最大重试次数({max_retries})，停止bot")
                break
            
            # 使用指数退避策略，避免频繁重试
            # 重试间隔：15s, 30s, 60s, 120s, ... 最大300s
            retry_interval = min(base_retry_interval * (2 ** (retry_count - 1)), 300)
            logger.info(f"{retry_interval}秒后尝试重新启动polling（指数退避）...")
            import time
            time.sleep(retry_interval)

