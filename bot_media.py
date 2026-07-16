import os
import shutil
import asyncio
from datetime import datetime
from uuid import uuid4
from typing import Any, List

from telegram import InputFile, MessageOriginChannel, Update
from telegram.ext import ContextTypes

from config import ALLOWED_USER_ID, IMAGE_DOWNLOAD_PATH, MAX_IMAGES_IN_DOWNLOAD_FOLDER, OCR_MAX_RETRIES, FIND_PAGINATION_ENABLED
from i18n import t

from bot_common import BotDeps, get_effective_language, get_user_language, translate
from bot_formatting import format_result_caption
from bot_ui import get_find_page_size, render_find_page


def get_image_files_in_folder(deps: BotDeps, folder_path: str) -> List[str]:
    """获取下载目录顶层的图片文件，供归档判断使用。"""
    image_extensions = ('.jpg', '.jpeg', '.png', '.gif', '.bmp', '.tiff', '.webp')
    files = []
    try:
        for item in os.listdir(folder_path):
            item_path = os.path.join(folder_path, item)
            if os.path.isfile(item_path) and item_path.lower().endswith(image_extensions):
                files.append(item_path)
    except OSError as e:
        deps.logger.error(f"Error listing files in {folder_path}: {e}")
    except Exception as e:
        deps.logger.error(f"Unexpected error in get_image_files_in_folder: {e}")

    return files


async def check_and_archive_images(deps: BotDeps, download_folder: str, max_count: int, searcher_instance: Any, context: ContextTypes.DEFAULT_TYPE):
    """
    检查下载目录中的图片数量，并在达到阈值时执行归档。
    归档完成后会同步更新数据库中的文件路径。
    """
    if not os.path.exists(download_folder):
        deps.logger.warning(f"Download folder does not exist: {download_folder}")
        return

    deps.logger.info(f"Checking image count in {download_folder}...")
    image_files = get_image_files_in_folder(deps, download_folder)

    if len(image_files) >= max_count:
        deps.logger.info(f"Image count ({len(image_files)}) reached or exceeded {max_count}. Initiating archive process.")

        file_modification_times = []
        valid_image_files = []
        for fpath in image_files:
            try:
                mtime = os.path.getmtime(fpath)
                file_modification_times.append(mtime)
                valid_image_files.append(fpath)
            except OSError as e:
                deps.logger.warning(f"Cannot access file {fpath}: {e}. Skipping.")
                continue
            except Exception as e:
                deps.logger.error(f"Unexpected error getting modification time for {fpath}: {e}. Skipping.")
                continue

        if not valid_image_files or not file_modification_times:
            deps.logger.warning("No valid image files found to archive.")
            return

        min_time = datetime.fromtimestamp(min(file_modification_times))
        max_time = datetime.fromtimestamp(max(file_modification_times))
        folder_name = f"{min_time.strftime('%Y.%m.%d')}_{max_time.strftime('%Y.%m.%d')}"
        archive_path = os.path.join(download_folder, folder_name)

        try:
            os.makedirs(archive_path, exist_ok=True)
            deps.logger.info(f"Created archive folder: {archive_path}")
        except OSError as e:
            deps.logger.error(f"Failed to create archive folder {archive_path}: {e}")
            return

        old_new_paths_for_db = []
        successful_moves_count = 0
        failed_moves = []

        for old_path in valid_image_files:
            try:
                file_name = os.path.basename(old_path)
                new_path = os.path.join(archive_path, file_name)

                if os.path.exists(new_path):
                    name, ext = os.path.splitext(file_name)
                    counter = 1
                    while os.path.exists(new_path):
                        new_path = os.path.join(archive_path, f"{name}_{counter}{ext}")
                        counter += 1
                    deps.logger.info(f"File name conflict, renamed to: {os.path.basename(new_path)}")

                shutil.move(old_path, new_path)
                old_new_paths_for_db.append((old_path, new_path))
                successful_moves_count += 1
                deps.logger.debug(f"Moved {old_path} to {new_path}")
            except OSError as e:
                deps.logger.error(f"Failed to move file {old_path} during archiving: {e}")
                failed_moves.append(old_path)
            except Exception as e:
                deps.logger.error(f"Unexpected error moving file {old_path}: {e}")
                failed_moves.append(old_path)

        if old_new_paths_for_db:
            try:
                deps.logger.info(f"Updating database paths for {len(old_new_paths_for_db)} archived images.")
                searcher_instance.update_archived_file_paths(old_new_paths_for_db)
                deps.logger.info("Database paths updated successfully.")
            except Exception as e:
                deps.logger.error(f"Failed to update database paths: {e}")
        else:
            deps.logger.warning("No files were successfully moved, database not updated.")

        language = get_user_language(deps, context, ALLOWED_USER_ID)
        message = t(language, "find.archive_done", folder_name=folder_name, successful_moves_count=successful_moves_count)
        if failed_moves:
            message += t(language, "find.archive_failed_count", failed_count=len(failed_moves))

        try:
            await context.bot.send_message(chat_id=ALLOWED_USER_ID, text=message, parse_mode='Markdown')
        except Exception as e:
            deps.logger.error(f"Failed to send archive notification: {e}")
    else:
        deps.logger.info(f"Image count ({len(image_files)}) is below {max_count}. No archive needed.")


async def handle_photo_with_retry(deps: BotDeps, update: Update, context: ContextTypes.DEFAULT_TYPE, max_retries: int = OCR_MAX_RETRIES) -> bool:
    """
    处理用户发图入口，带有限次重试。
    该流程同时负责：按图搜索快捷入口、重复图检测、入库和归档触发。
    """
    photo = update.message.photo[-1]
    current_message_id = update.message.message_id
    language = get_effective_language(deps, update, context)

    telegram_msg_id_for_db = ""
    forward_origin = update.message.forward_origin
    if isinstance(forward_origin, MessageOriginChannel):
        if forward_origin.chat.username:
            telegram_msg_id_for_db = f"https://t.me/{forward_origin.chat.username}/{forward_origin.message_id}"
            deps.logger.info(f"Detected forwarded message from channel with original ID: {telegram_msg_id_for_db}")
        else:
            deps.logger.info("Forwarded message from private channel or supergroup, no public link.")
    else:
        deps.logger.info("Message is not a forwarded channel message.")

    file_ext = os.path.splitext(photo.file_unique_id)[1] or '.jpg'
    temp_save_path = None

    for attempt in range(max_retries + 1):
        try:
            if not os.path.exists(IMAGE_DOWNLOAD_PATH):
                os.makedirs(IMAGE_DOWNLOAD_PATH, exist_ok=True)

            temp_save_path = os.path.join(IMAGE_DOWNLOAD_PATH, f"temp_{uuid4()}{file_ext}")
            file = await context.bot.get_file(photo.file_id)
            await file.download_to_drive(temp_save_path)

            if not os.path.exists(temp_save_path) or os.path.getsize(temp_save_path) == 0:
                raise Exception(f"Downloaded file is empty or doesn't exist: {temp_save_path}")

            deps.logger.info(f"Downloaded photo to temporary path {temp_save_path} (attempt {attempt + 1})")

            # 用户给图片直接加 /find caption 时，复用按图搜索流程而不入库。
            if update.message.caption and update.message.caption.strip().lower() == '/find':
                await search_by_image(deps, update, context, temp_save_path)
                return True

            exact_match_results = deps.searcher.search_similar_images(temp_save_path, threshold=0, max_results=1)
            if exact_match_results and exact_match_results[0].get('similarity') == 1.0:
                exact_match_data = exact_match_results[0]
                existing_telegram_message_id_in_db = exact_match_data.get('telegram_message_id')

                if existing_telegram_message_id_in_db:
                    deps.logger.info(f"Duplicate image received, original telegram_message_id: {existing_telegram_message_id_in_db}")
                    await update.message.reply_text(
                        t(language, "photo.duplicate_with_id", message_id=existing_telegram_message_id_in_db),
                        reply_to_message_id=current_message_id,
                    )
                else:
                    try:
                        with open(exact_match_data['path'], 'rb') as photo_file:
                            caption = t(
                                language,
                                "photo.duplicate_details",
                                filename=os.path.basename(exact_match_data['path']),
                                file_hash=exact_match_data['file_hash'],
                                updated_time=datetime.fromtimestamp(exact_match_data['updated_time']).strftime('%Y-%m-%d %H:%M:%S'),
                            )
                            await context.bot.send_photo(
                                chat_id=update.effective_chat.id,
                                photo=InputFile(photo_file),
                                caption=caption,
                                parse_mode='Markdown',
                                reply_to_message_id=current_message_id,
                            )
                            deps.logger.info(f"Duplicate image received with no source message ID, sent details for {exact_match_data['path']}")
                    except FileNotFoundError:
                        await update.message.reply_text(
                            t(language, "photo.duplicate_missing_source"),
                            reply_to_message_id=current_message_id,
                        )
                    except Exception as e:
                        deps.logger.error(f"Error sending existing image details: {e}")
                        await update.message.reply_text(
                            t(language, "photo.duplicate_process_error"),
                            reply_to_message_id=current_message_id,
                        )
            else:
                permanent_path = os.path.join(IMAGE_DOWNLOAD_PATH, f"{current_message_id}_{photo.file_unique_id}{file_ext}")
                os.rename(temp_save_path, permanent_path)
                temp_save_path = None

                index_success = deps.searcher.add_image_to_index(permanent_path, telegram_msg_id_for_db)
                if not index_success:
                    raise Exception("图片索引建立失败")

                deps.logger.info(f"Indexed new image at {permanent_path}")
                pending_count = deps.searcher.get_pending_ocr_count(OCR_MAX_RETRIES)
                await update.message.reply_text(
                    t(language, "photo.index_success", pending_count=pending_count),
                    reply_to_message_id=current_message_id,
                    parse_mode='Markdown',
                )
                await check_and_archive_images(deps, IMAGE_DOWNLOAD_PATH, MAX_IMAGES_IN_DOWNLOAD_FOLDER, deps.searcher, context)

            return True
        except Exception as e:
            deps.logger.error(f"Error handling photo attempt {attempt + 1}/{max_retries + 1} with message_id {current_message_id}: {e}")
            if attempt < max_retries:
                if temp_save_path and os.path.exists(temp_save_path):
                    try:
                        os.remove(temp_save_path)
                        temp_save_path = None
                    except OSError:
                        pass
                await asyncio.sleep(1)
                continue

            await update.message.reply_text(
                t(language, "photo.retry_failed", max_retries=max_retries),
                reply_to_message_id=current_message_id,
            )
            deps.logger.error(f"Failed to handle photo after {max_retries + 1} attempts with message_id {current_message_id}")
            return False
        finally:
            if temp_save_path and os.path.exists(temp_save_path):
                try:
                    os.remove(temp_save_path)
                except OSError as e:
                    deps.logger.error(f"Failed to clean up temporary file {temp_save_path}: {e}")

    return False


async def handle_photo(deps: BotDeps, update: Update, context: ContextTypes.DEFAULT_TYPE):
    """处理普通发图入口。"""
    try:
        deps.logger.info(f"📸 Received photo from user {update.message.from_user.id}, message_id: {update.message.message_id}")
        if update.message.from_user.id != ALLOWED_USER_ID:
            deps.logger.warning(f"❌ Unauthorized user {update.message.from_user.id} tried to interact.")
            return

        await update.message.reply_text(translate(deps, context, update, "common.processing"))
        await handle_photo_with_retry(deps, update, context)
        deps.logger.info(f"✅ Photo processing completed for message_id: {update.message.message_id}")
    except Exception as e:
        deps.logger.error(f"❌ Critical error in handle_photo: {e}", exc_info=True)
        try:
            await update.message.reply_text(translate(deps, context, update, "photo.critical_error", error=str(e)))
        except Exception:
            pass


async def search_by_image(deps: BotDeps, update: Update, context: ContextTypes.DEFAULT_TYPE, query_image_path: str):
    """根据给定图片路径执行相似图搜索，并按结果形态回复。"""
    language = get_effective_language(deps, update, context)
    try:
        results = deps.searcher.search_similar_images(query_image_path)

        if not results:
            await update.message.reply_text(t(language, "search.no_match"), reply_to_message_id=update.message.message_id)
            return

        first_result = results[0]
        if first_result.get('similarity') == 1.0:
            existing_telegram_message_id_in_db = first_result.get('telegram_message_id')
            if existing_telegram_message_id_in_db:
                deps.logger.info(f"Exact image match found with telegram_message_id: {existing_telegram_message_id_in_db}")
                await update.message.reply_text(
                    t(language, "search.exact_match_with_id", message_id=existing_telegram_message_id_in_db),
                    reply_to_message_id=update.message.message_id,
                )
                return

            try:
                if not os.path.exists(first_result['path']):
                    await update.message.reply_text(
                        t(language, "search.exact_match_missing_source"),
                        reply_to_message_id=update.message.message_id,
                    )
                    return

                with open(first_result['path'], 'rb') as photo_file:
                    caption = t(
                        language,
                        "search.exact_match_details",
                        filename=os.path.basename(first_result['path']),
                        file_hash=first_result['file_hash'],
                        updated_time=datetime.fromtimestamp(first_result['updated_time']).strftime('%Y-%m-%d %H:%M:%S'),
                    )
                    await context.bot.send_photo(
                        chat_id=update.effective_chat.id,
                        photo=InputFile(photo_file),
                        caption=caption,
                        parse_mode='Markdown',
                        reply_to_message_id=update.message.message_id,
                    )
                deps.logger.info(f"Sent exact match image details for {first_result['path']}")
                return
            except IOError as e:
                deps.logger.error(f"IO error reading exact match file {first_result['path']}: {e}")
                await update.message.reply_text(t(language, "common.read_file_error"), reply_to_message_id=update.message.message_id)
                return
            except Exception as e:
                deps.logger.error(f"Error sending exact match image details: {e}")
                await update.message.reply_text(
                    t(language, "search.exact_match_process_error"),
                    reply_to_message_id=update.message.message_id,
                )
                return

        if FIND_PAGINATION_ENABLED and len(results) > 1:
            query_id = str(uuid4())
            page_size = get_find_page_size()
            context.user_data.setdefault("find_pagination", {})[query_id] = {
                "results": results,
                "mode": "image",
                "page_size": page_size,
                "summary": t(language, "search.similar_results_summary", count=len(results)),
                "message_ids": [],
                "language": language,
            }
            await render_find_page(deps, update, context, query_id, 1, is_callback=False)
            return

        await update.message.reply_text(
            t(language, "search.similar_results_summary", count=len(results)),
            reply_to_message_id=update.message.message_id,
        )

        for result in results:
            try:
                if not os.path.exists(result['path']):
                    await update.message.reply_text(
                        t(language, "search.result_missing_file", filename=os.path.basename(result['path'])),
                        reply_to_message_id=update.message.message_id,
                        parse_mode='Markdown',
                    )
                    continue

                with open(result['path'], 'rb') as photo_file:
                    caption = format_result_caption(language, result, include_similarity=True)
                    await context.bot.send_photo(
                        chat_id=update.effective_chat.id,
                        photo=InputFile(photo_file),
                        caption=caption,
                        parse_mode='Markdown',
                        reply_to_message_id=update.message.message_id,
                    )
            except IOError as e:
                deps.logger.error(f"IO error reading search result file {result['path']}: {e}")
                await update.message.reply_text(
                    t(language, "search.read_file_error", filename=os.path.basename(result['path'])),
                    reply_to_message_id=update.message.message_id,
                    parse_mode='Markdown',
                )
            except Exception as e:
                deps.logger.error(f"Failed to send search result photo {result['path']}: {e}")
                await update.message.reply_text(
                    t(language, "search.send_result_error", filename=os.path.basename(result['path'])),
                    reply_to_message_id=update.message.message_id,
                    parse_mode='Markdown',
                )
    except Exception as e:
        deps.logger.error(f"Unexpected error in search_by_image: {e}", exc_info=True)
        await update.message.reply_text(t(language, "search.unexpected_error"), reply_to_message_id=update.message.message_id)
