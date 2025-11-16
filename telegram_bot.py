import logging
import os
import shutil
import glob
from uuid import uuid4
from datetime import datetime, time
import asyncio

from telegram import Update, InputFile, MessageOriginChannel
from telegram.ext import ApplicationBuilder, ContextTypes, CommandHandler, MessageHandler, filters

from config import BOT_TOKEN, ALLOWED_USER_ID, IMAGE_DOWNLOAD_PATH, DB_PATH, LOG_FILE_PATH, MAX_IMAGES_IN_DOWNLOAD_FOLDER, OCR_SCHEDULED_TIME, OCR_MAX_RETRIES, OCR_BATCH_SIZE
from image_searcher import ImageSimilaritySearcher

from typing import Dict, Optional, List

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


async def handle_photo(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    处理用户发送的图片。
    - 如果图片附带 /search 命令，则执行搜索。
    - 否则，检查图片是否已存在。若不存在，则添加索引；若存在，则根据是否有原消息ID返回相应结果。
    """
    if update.message.from_user.id != ALLOWED_USER_ID:
        logger.warning(f"Unauthorized user {update.message.from_user.id} tried to interact.")
        return

    await update.message.reply_text("处理中...")
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
            logger.error(f"Downloaded file is empty or doesn't exist: {temp_save_path}")
            await update.message.reply_text("下载文件失败，文件为空。", reply_to_message_id=current_message_id)
            return
        
        logger.info(f"Downloaded photo to temporary path {temp_save_path}")

        # Check if the message caption contains the /search command
        if update.message.caption and update.message.caption.strip().lower() == '/search':
            # --- Execute search logic ---
            await search_by_image(update, context, temp_save_path)
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
                    logger.error(f"Failed to rename file {temp_save_path} to {permanent_path}: {e}")
                    await update.message.reply_text("重命名文件失败，请检查日志。", reply_to_message_id=current_message_id)
                    return

                # Add image to index - now returns bool (True/False) instead of OCR text
                # OCR will be processed later by scheduled task
                index_success = searcher.add_image_to_index(permanent_path, telegram_msg_id_for_db)
                if index_success:
                    pending_count = searcher.get_pending_ocr_count()
                    await update.message.reply_text(f"该图片已成功建立索引。\nOCR处理将在定时任务中进行。\n当前待处理OCR图片数: {pending_count}", 
                                                    reply_to_message_id=current_message_id, parse_mode='Markdown')
                else:
                    await update.message.reply_text("图片索引建立失败，请检查日志。", reply_to_message_id=current_message_id)
                
                # After successfully indexing a new image, check for archiving
                await check_and_archive_images(IMAGE_DOWNLOAD_PATH, MAX_IMAGES_IN_DOWNLOAD_FOLDER, searcher, context)

    except Exception as e:
        logger.error(f"Error handling photo with message_id {current_message_id}: {e}", exc_info=True)
        await update.message.reply_text("处理图片时发生错误。", reply_to_message_id=current_message_id)
    finally:
        # Clean up temporary file if it still exists
        if temp_save_path and os.path.exists(temp_save_path):
            try:
                os.remove(temp_save_path)
                logger.info(f"Cleaned up temporary file: {temp_save_path}")
            except OSError as e:
                logger.error(f"Failed to clean up temporary file {temp_save_path}: {e}")


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


async def search_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """处理 /search 命令 (文本或回复)"""
    if update.message.from_user.id != ALLOWED_USER_ID:
        logger.warning(f"Unauthorized user {update.message.from_user.id} tried to interact with /search.")
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
            keywords = " ".join(context.args)
            results = searcher.search_by_text(keywords)
            if not results:
                await update.message.reply_text("未找到文本匹配结果。", reply_to_message_id=update.message.message_id)
                return
            
            # 当只有一个结果时，合并为一句话
            if len(results) == 1:
                result = results[0]
                if result.get('telegram_message_id'):
                    message = f"找到1个文本匹配结果，原消息ID：{result['telegram_message_id']}"
                else:
                    filename = os.path.basename(result['path'])
                    message = f"找到1个文本匹配结果，文件路径：<code>{filename}</code>"
                
                await update.message.reply_text(message, reply_to_message_id=update.message.message_id, parse_mode='HTML')
            else:
                # 当有多个结果时，先回复总数，再合并所有结果到一条消息
                await update.message.reply_text(f"找到 {len(results)} 个文本匹配结果:", reply_to_message_id=update.message.message_id)
                
                result_messages = []
                for idx, result in enumerate(results, 1):
                    if result.get('telegram_message_id'):
                        result_messages.append(f"{idx}. 原消息ID：{result['telegram_message_id']}")
                    else:
                        filename = os.path.basename(result['path'])
                        result_messages.append(f"{idx}. 文件路径：<code>{filename}</code>")
                
                combined_message = "<br>".join(result_messages)
                await context.bot.send_message(
                    chat_id=update.effective_chat.id,
                    text=combined_message,
                    parse_mode='HTML',
                    reply_to_message_id=update.message.message_id
                )
        except Exception as e:
            logger.error(f"Error during text search: {e}", exc_info=True)
            await update.message.reply_text("文本搜索时发生错误。", reply_to_message_id=update.message.message_id)
    
    # Invalid usage of /search command
    else:
        help_text = """使用方法：
1. <code>/search &lt;关键词&gt;</code> (文本搜索)
2. 回复一张图片并发送 <code>/search</code> (图片搜索)"""
        await update.message.reply_text(help_text, parse_mode='HTML', reply_to_message_id=update.message.message_id)


async def force_ocr_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    处理 /forceOCR 命令，立即对所有未OCR的图片进行OCR处理
    
    与定时任务不同的是，/forceOCR 会一次性处理所有待处理的图片，
    不受 OCR_BATCH_SIZE 的限制（但内存允许的情况下）
    """
    if update.message.from_user.id != ALLOWED_USER_ID:
        logger.warning(f"Unauthorized user {update.message.from_user.id} tried to interact with /forceOCR.")
        return
    
    pending_count = searcher.get_pending_ocr_count()
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
        last_update_time = datetime.now()  # 记录上次更新时间，避免过于频繁的 API 调用
        
        while iteration < max_iterations:
            iteration += 1
            remaining = searcher.get_pending_ocr_count()
            if remaining == 0:
                logger.info(f"Force OCR: All images processed after {iteration} iterations.")
                break
            
            logger.info(f"Force OCR iteration {iteration}: Processing {remaining} pending images...")
            stats = searcher.process_ocr_pending_images(batch_size=OCR_BATCH_SIZE, max_retries=OCR_MAX_RETRIES)
            
            # 累计统计
            total_stats['processed'] += stats['processed']
            total_stats['succeeded'] += stats['succeeded']
            total_stats['failed'] += stats['failed']
            total_stats['skipped'] += stats['skipped']
            
            # 每处理完一批后，更新进度条（为避免 API 限流，只在有意义的进度时更新，最多每 0.5 秒更新一次）
            now = datetime.now()
            if (now - last_update_time).total_seconds() >= 0.5 or remaining == 0:
                try:
                    progress_text = (
                        f"⏳ 正在处理 {pending_count} 张待OCR的图片\n\n"
                        f"{create_progress_bar(total_stats['processed'], pending_count)}\n"
                        f"{total_stats['processed']}/{pending_count} 张已处理"
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
        
        # 构建详细的反馈消息
        message = (
            f"✅ OCR处理完成！\n\n"
            f"{create_progress_bar(total_stats['processed'], pending_count)}\n"
            f"总计：{total_stats['processed']}/{pending_count} 张处理\n\n"
            f"📊 处理统计:\n"
            f"  成功: {total_stats['succeeded']}\n"
            f"  失败: {total_stats['failed']}\n"
            f"  跳过: {total_stats['skipped']}\n"
            f"  迭代次数: {iteration}"
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


async def scheduled_ocr_task(context: ContextTypes.DEFAULT_TYPE):
    """
    定时执行OCR任务 - 处理所有待处理的图片
    
    为了避免OCR任务积压，本任务会循环调用process_ocr_pending_images，
    直到所有待处理的图片都被处理完成。
    """
    try:
        pending_count = searcher.get_pending_ocr_count()
        if pending_count == 0:
            logger.info("Scheduled OCR task: No pending images.")
            return
        
        logger.info(f"Starting scheduled OCR task for {pending_count} images...")
        
        # 关键改进：循环处理，直到没有待处理的图片
        total_stats = {'processed': 0, 'succeeded': 0, 'failed': 0, 'skipped': 0}
        iteration = 0
        
        while True:
            iteration += 1
            remaining = searcher.get_pending_ocr_count()
            if remaining == 0:
                logger.info(f"All pending images have been processed after {iteration} iterations.")
                break
            
            logger.info(f"OCR task iteration {iteration}: Processing {remaining} pending images...")
            stats = searcher.process_ocr_pending_images(batch_size=OCR_BATCH_SIZE, max_retries=OCR_MAX_RETRIES)
            
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
        
        # 发送完整的统计信息
        message = (
            f"定时OCR任务已完成\n"
            f"总处理数: {total_stats['processed']}\n"
            f"成功: {total_stats['succeeded']}\n"
            f"失败: {total_stats['failed']}\n"
            f"跳过: {total_stats['skipped']}\n"
            f"迭代次数: {iteration}"
        )
        
        if total_stats['failed'] > 0:
            message += (
                f"\n\n⚠️ 注意：有 {total_stats['failed']} 张图片 OCR 失败。"
                f"这些图片会在后续任务中继续重试（最多 {OCR_MAX_RETRIES} 次）。"
            )
        
        await context.bot.send_message(chat_id=ALLOWED_USER_ID, text=message)
        logger.info(f"Scheduled OCR task completed: {total_stats}, iterations: {iteration}")
    except Exception as e:
        logger.error(f"Error in scheduled OCR task: {e}", exc_info=True)
        try:
            await context.bot.send_message(chat_id=ALLOWED_USER_ID, text=f"定时OCR任务出现错误: {str(e)}")
        except Exception as send_error:
            logger.error(f"Failed to send error message to user: {send_error}")


def parse_scheduled_time(time_str: str) -> Optional[time]:
    """解析时间字符串 (格式: HH:MM) 为 time 对象"""
    try:
        hour, minute = map(int, time_str.split(':'))
        return time(hour=hour, minute=minute)
    except (ValueError, AttributeError):
        logger.error(f"Invalid time format: {time_str}. Expected HH:MM")
        return None


if __name__ == '__main__':
    logger.info("Starting bot...")
    
    application = ApplicationBuilder().token(BOT_TOKEN).build()
    
    # Add handlers
    application.add_handler(CommandHandler('search', search_command))
    application.add_handler(CommandHandler('forceOCR', force_ocr_command))
    # handle_photo processes all photo messages, internal logic decides add or search
    application.add_handler(MessageHandler(filters.PHOTO, handle_photo))
    
    # Add scheduled OCR task
    scheduled_ocr_time = parse_scheduled_time(OCR_SCHEDULED_TIME)
    if scheduled_ocr_time:
        job_queue = application.job_queue
        job_queue.run_daily(scheduled_ocr_task, time=scheduled_ocr_time)
        logger.info(f"Scheduled daily OCR task at {OCR_SCHEDULED_TIME}")
    else:
        logger.warning(f"Failed to parse OCR scheduled time: {OCR_SCHEDULED_TIME}")
    
    # 启动 Bot
    logger.info("🤖 机器人启动中...")
    application.run_polling()

