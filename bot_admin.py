import os
import asyncio
import html
from datetime import datetime
from uuid import uuid4

from telegram import Update
from telegram.ext import ContextTypes

from config import (
    ALLOWED_USER_ID,
    DEFAULT_LANGUAGE,
    FAILED_OCR_DEFAULT_LIMIT,
    IMAGE_DOWNLOAD_PATH,
    OCR_MAX_RETRIES,
    SUPPORTED_LANGUAGES,
)
from i18n import get_language_name, is_supported_language, normalize_language, t

from bot_common import BotDeps, get_effective_language


def get_help_document_path(language: str) -> str:
    base_dir = os.path.dirname(os.path.abspath(__file__))
    localized_paths = {
        "zh": os.path.join(base_dir, "COMMANDS_zh.md"),
    }
    return localized_paths.get(language, os.path.join(base_dir, "COMMANDS.md"))


def load_help_document(language: str) -> str:
    fallback_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "COMMANDS.md")
    target_path = get_help_document_path(language)

    try:
        with open(target_path, "r", encoding="utf-8") as file:
            return file.read()
    except OSError:
        if target_path != fallback_path:
            with open(fallback_path, "r", encoding="utf-8") as file:
                return file.read()
        raise


async def tag_command(deps: BotDeps, update: Update, context: ContextTypes.DEFAULT_TYPE):
    """处理 /tag，允许用户为已索引图片手动写入 OCR 文本。"""
    deps.logger.info(f"🏷️ Received /tag command from user {update.message.from_user.id}")
    if update.message.from_user.id != ALLOWED_USER_ID:
        deps.logger.warning(f"❌ Unauthorized user {update.message.from_user.id} tried to interact with /tag.")
        return

    language = get_effective_language(deps, update, context)
    if not update.message.reply_to_message:
        await update.message.reply_text(t(language, "tag.reply_usage"), parse_mode='Markdown', reply_to_message_id=update.message.message_id)
        return

    replied_message = update.message.reply_to_message
    if not replied_message.photo:
        await update.message.reply_text(t(language, "tag.reply_photo_required"), reply_to_message_id=update.message.message_id)
        return

    if not context.args:
        await update.message.reply_text(t(language, "tag.missing_text"), parse_mode='Markdown', reply_to_message_id=update.message.message_id)
        return

    ocr_text = " ".join(context.args)
    try:
        photo = replied_message.photo[-1]
        file_ext = os.path.splitext(photo.file_unique_id)[1] or '.jpg'
        temp_file_path = os.path.join(IMAGE_DOWNLOAD_PATH, f"temp_setocr_{uuid4()}{file_ext}")
        try:
            if not os.path.exists(IMAGE_DOWNLOAD_PATH):
                os.makedirs(IMAGE_DOWNLOAD_PATH, exist_ok=True)
            file = await context.bot.get_file(photo.file_id)
            await file.download_to_drive(temp_file_path)
            if not os.path.exists(temp_file_path) or os.path.getsize(temp_file_path) == 0:
                deps.logger.error(f"Downloaded file is empty or doesn't exist: {temp_file_path}")
                await update.message.reply_text(t(language, "tag.download_failed"), reply_to_message_id=update.message.message_id)
                return

            similar_results = deps.searcher.search_similar_images(temp_file_path, threshold=0, max_results=1)
            # /tag 需要命中数据库中的同一张已索引图片，因此这里只接受 exact match。
            if not similar_results or similar_results[0].get('similarity') != 1.0:
                await update.message.reply_text(t(language, "tag.record_not_found"), reply_to_message_id=update.message.message_id)
                return

            image_record = similar_results[0]
            file_hash = image_record.get('file_hash')
            telegram_message_id_in_db = image_record.get('telegram_message_id')
            success = deps.searcher.set_manual_ocr_result_by_hash(file_hash, ocr_text)
            if success:
                pending_count = deps.searcher.get_pending_ocr_count(OCR_MAX_RETRIES)
                msg_info = t(language, "tag.msg_info_with_id", message_id=telegram_message_id_in_db) if telegram_message_id_in_db else t(language, "tag.msg_info_without_id")
                escaped_ocr_text = html.escape(ocr_text)
                await update.message.reply_text(
                    t(language, "tag.success", ocr_text=escaped_ocr_text, msg_info=msg_info, pending_count=pending_count),
                    parse_mode='HTML',
                    reply_to_message_id=update.message.message_id,
                )
                deps.logger.info(f"User manually set OCR result for file_hash {file_hash}: '{ocr_text}'")
            else:
                await update.message.reply_text(t(language, "tag.failed"), reply_to_message_id=update.message.message_id)
        finally:
            if os.path.exists(temp_file_path):
                try:
                    os.remove(temp_file_path)
                    deps.logger.info(f"Cleaned up temporary file: {temp_file_path}")
                except OSError as e:
                    deps.logger.error(f"Failed to clean up temporary file {temp_file_path}: {e}")
    except Exception as e:
        deps.logger.error(f"Error in tag_command: {e}", exc_info=True)
        await update.message.reply_text(t(language, "tag.error"), reply_to_message_id=update.message.message_id)


async def setmessageid_command(deps: BotDeps, update: Update, context: ContextTypes.DEFAULT_TYPE):
    """处理 /link，为缺少来源消息的已索引图片补写消息 ID 或链接。"""
    if update.message.from_user.id != ALLOWED_USER_ID:
        deps.logger.warning(f"Unauthorized user {update.message.from_user.id} tried to interact with /link.")
        return

    language = get_effective_language(deps, update, context)
    if not update.message.reply_to_message:
        await update.message.reply_text(t(language, "link.reply_usage"), parse_mode='Markdown', reply_to_message_id=update.message.message_id)
        return

    replied_message = update.message.reply_to_message
    if not replied_message.photo:
        await update.message.reply_text(t(language, "link.reply_photo_required"), reply_to_message_id=update.message.message_id)
        return

    if not context.args:
        await update.message.reply_text(t(language, "link.missing_message_id"), parse_mode='Markdown', reply_to_message_id=update.message.message_id)
        return

    message_id = " ".join(context.args)
    try:
        photo = replied_message.photo[-1]
        file_ext = os.path.splitext(photo.file_unique_id)[1] or '.jpg'
        temp_file_path = os.path.join(IMAGE_DOWNLOAD_PATH, f"temp_setmsgid_{uuid4()}{file_ext}")
        try:
            if not os.path.exists(IMAGE_DOWNLOAD_PATH):
                os.makedirs(IMAGE_DOWNLOAD_PATH, exist_ok=True)
            file = await context.bot.get_file(photo.file_id)
            await file.download_to_drive(temp_file_path)
            if not os.path.exists(temp_file_path) or os.path.getsize(temp_file_path) == 0:
                deps.logger.error(f"Downloaded file is empty or doesn't exist: {temp_file_path}")
                await update.message.reply_text(t(language, "link.download_failed"), reply_to_message_id=update.message.message_id)
                return

            similar_results = deps.searcher.search_similar_images(temp_file_path, threshold=0, max_results=1)
            if not similar_results or similar_results[0].get('similarity') != 1.0:
                await update.message.reply_text(t(language, "link.record_not_found"), reply_to_message_id=update.message.message_id)
                return

            image_record = similar_results[0]
            file_hash = image_record.get('file_hash')
            existing_message_id = image_record.get('telegram_message_id')
            if existing_message_id:
                await update.message.reply_text(
                    t(language, "link.already_exists", message_id=existing_message_id),
                    reply_to_message_id=update.message.message_id,
                )
                return

            success = deps.searcher.set_message_id_by_hash(file_hash, message_id)
            if success:
                await update.message.reply_text(
                    t(language, "link.success", message_id=message_id),
                    parse_mode='Markdown',
                    reply_to_message_id=update.message.message_id,
                )
                deps.logger.info(f"User manually set message_id for file_hash {file_hash}: '{message_id}'")
            else:
                await update.message.reply_text(t(language, "link.failed"), reply_to_message_id=update.message.message_id)
        finally:
            if os.path.exists(temp_file_path):
                try:
                    os.remove(temp_file_path)
                    deps.logger.info(f"Cleaned up temporary file: {temp_file_path}")
                except OSError as e:
                    deps.logger.error(f"Failed to clean up temporary file {temp_file_path}: {e}")
    except Exception as e:
        deps.logger.error(f"Error in setmessageid_command: {e}", exc_info=True)
        await update.message.reply_text(t(language, "link.error"), reply_to_message_id=update.message.message_id)


async def untag_command(deps: BotDeps, update: Update, context: ContextTypes.DEFAULT_TYPE):
    """处理 /untag，清空 OCR 文本并把状态重置为 pending。"""
    if update.message.from_user.id != ALLOWED_USER_ID:
        deps.logger.warning(f"Unauthorized user {update.message.from_user.id} tried to interact with /untag.")
        return

    language = get_effective_language(deps, update, context)
    if not update.message.reply_to_message:
        await update.message.reply_text(t(language, "untag.reply_usage"), reply_to_message_id=update.message.message_id)
        return

    replied_message = update.message.reply_to_message
    if not replied_message.photo:
        await update.message.reply_text(t(language, "untag.reply_photo_required"), reply_to_message_id=update.message.message_id)
        return

    try:
        photo = replied_message.photo[-1]
        file_ext = os.path.splitext(photo.file_unique_id)[1] or '.jpg'
        temp_file_path = os.path.join(IMAGE_DOWNLOAD_PATH, f"temp_clearocr_{uuid4()}{file_ext}")
        try:
            if not os.path.exists(IMAGE_DOWNLOAD_PATH):
                os.makedirs(IMAGE_DOWNLOAD_PATH, exist_ok=True)
            file = await context.bot.get_file(photo.file_id)
            await file.download_to_drive(temp_file_path)
            if not os.path.exists(temp_file_path) or os.path.getsize(temp_file_path) == 0:
                deps.logger.error(f"Downloaded file is empty or doesn't exist: {temp_file_path}")
                await update.message.reply_text(t(language, "untag.download_failed"), reply_to_message_id=update.message.message_id)
                return

            similar_results = deps.searcher.search_similar_images(temp_file_path, threshold=0, max_results=1)
            if not similar_results or similar_results[0].get('similarity') != 1.0:
                await update.message.reply_text(t(language, "untag.record_not_found"), reply_to_message_id=update.message.message_id)
                return

            image_record = similar_results[0]
            telegram_message_id_in_db = image_record.get('telegram_message_id')
            if not telegram_message_id_in_db:
                await update.message.reply_text(t(language, "untag.no_message_id"), reply_to_message_id=update.message.message_id)
                return

            success = deps.searcher.clear_ocr_result(telegram_message_id_in_db)
            if success:
                pending_count = deps.searcher.get_pending_ocr_count(OCR_MAX_RETRIES)
                await update.message.reply_text(
                    t(language, "untag.success", pending_count=pending_count),
                    reply_to_message_id=update.message.message_id,
                )
                deps.logger.info(f"User manually cleared OCR result for message_id {telegram_message_id_in_db}")
            else:
                await update.message.reply_text(t(language, "untag.failed"), reply_to_message_id=update.message.message_id)
        finally:
            if os.path.exists(temp_file_path):
                try:
                    os.remove(temp_file_path)
                    deps.logger.info(f"Cleaned up temporary file: {temp_file_path}")
                except OSError as e:
                    deps.logger.error(f"Failed to clean up temporary file {temp_file_path}: {e}")
    except Exception as e:
        deps.logger.error(f"Error in untag_command: {e}", exc_info=True)
        await update.message.reply_text(t(language, "untag.error"), reply_to_message_id=update.message.message_id)


async def getocr_command(deps: BotDeps, update: Update, context: ContextTypes.DEFAULT_TYPE):
    """处理 /getocr：支持按消息 ID 查询，或通过回复图片查询。"""
    if update.message.from_user.id != ALLOWED_USER_ID:
        deps.logger.warning(f"Unauthorized user {update.message.from_user.id} tried to interact with /getocr.")
        return

    language = get_effective_language(deps, update, context)
    message_id_from_arg = None
    if context.args:
        i = 0
        while i < len(context.args):
            arg = context.args[i]
            if arg == '-l' and i + 1 < len(context.args):
                message_id_from_arg = context.args[i + 1]
                break
            i += 1

    if message_id_from_arg:
        try:
            ocr_text = deps.searcher.get_ocr_by_message_id(message_id_from_arg)
            if ocr_text is None:
                await update.message.reply_text(t(language, "getocr.not_found_by_id", message_id=message_id_from_arg), parse_mode='Markdown', reply_to_message_id=update.message.message_id)
            elif not ocr_text or ocr_text.strip() == '':
                await update.message.reply_text(t(language, "getocr.empty_by_id", message_id=message_id_from_arg), parse_mode='Markdown', reply_to_message_id=update.message.message_id)
            else:
                await update.message.reply_text(t(language, "getocr.result", ocr_text=ocr_text), parse_mode='Markdown', reply_to_message_id=update.message.message_id)
                deps.logger.info(f"User queried OCR result by message_id {message_id_from_arg}: '{ocr_text[:50]}...'")
        except Exception as e:
            deps.logger.error(f"Error querying OCR by message_id: {e}", exc_info=True)
            await update.message.reply_text(t(language, "getocr.query_error"), reply_to_message_id=update.message.message_id)
        return

    if not update.message.reply_to_message:
        await update.message.reply_text(t(language, "getocr.usage"), reply_to_message_id=update.message.message_id)
        return

    replied_message = update.message.reply_to_message
    if not replied_message.photo:
        await update.message.reply_text(t(language, "getocr.reply_photo_required"), reply_to_message_id=update.message.message_id)
        return

    try:
        photo = replied_message.photo[-1]
        file_ext = os.path.splitext(photo.file_unique_id)[1] or '.jpg'
        temp_file_path = os.path.join(IMAGE_DOWNLOAD_PATH, f"temp_getocr_{uuid4()}{file_ext}")
        try:
            if not os.path.exists(IMAGE_DOWNLOAD_PATH):
                os.makedirs(IMAGE_DOWNLOAD_PATH, exist_ok=True)
            file = await context.bot.get_file(photo.file_id)
            await file.download_to_drive(temp_file_path)
            if not os.path.exists(temp_file_path) or os.path.getsize(temp_file_path) == 0:
                deps.logger.error(f"Downloaded file is empty or doesn't exist: {temp_file_path}")
                await update.message.reply_text(t(language, "getocr.download_failed"), reply_to_message_id=update.message.message_id)
                return

            similar_results = deps.searcher.search_similar_images(temp_file_path, threshold=0, max_results=1)
            if not similar_results or similar_results[0].get('similarity') != 1.0:
                await update.message.reply_text(t(language, "getocr.record_not_found"), reply_to_message_id=update.message.message_id)
                return

            image_record = similar_results[0]
            ocr_text = image_record.get('ocr_text', '')
            if not ocr_text or ocr_text.strip() == '':
                await update.message.reply_text(t(language, "getocr.empty"), reply_to_message_id=update.message.message_id)
            else:
                await update.message.reply_text(t(language, "getocr.result", ocr_text=ocr_text), parse_mode='Markdown', reply_to_message_id=update.message.message_id)
                deps.logger.info(f"User queried OCR result: '{ocr_text[:50]}...'")
        finally:
            if os.path.exists(temp_file_path):
                try:
                    os.remove(temp_file_path)
                    deps.logger.info(f"Cleaned up temporary file: {temp_file_path}")
                except OSError as e:
                    deps.logger.error(f"Failed to clean up temporary file {temp_file_path}: {e}")
    except Exception as e:
        deps.logger.error(f"Error in getocr_command: {e}", exc_info=True)
        await update.message.reply_text(t(language, "getocr.error"), reply_to_message_id=update.message.message_id)


async def failed_command(deps: BotDeps, update: Update, context: ContextTypes.DEFAULT_TYPE):
    """处理 /failed，列出 OCR 失败记录并尽量引用回原消息。"""
    deps.logger.info(f"📋 Received /failed command from user {update.message.from_user.id}")
    if update.message.from_user.id != ALLOWED_USER_ID:
        deps.logger.warning(f"❌ Unauthorized user {update.message.from_user.id} tried to interact with /failed.")
        return

    language = get_effective_language(deps, update, context)
    limit = FAILED_OCR_DEFAULT_LIMIT
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

    failed_count = deps.searcher.get_failed_ocr_count()
    if failed_count == 0:
        await update.message.reply_text(t(language, "failed.none"), reply_to_message_id=update.message.message_id)
        return

    records = deps.searcher.get_failed_ocr_records(limit=limit if not show_all else None)
    if not records:
        await update.message.reply_text(t(language, "failed.none"), reply_to_message_id=update.message.message_id)
        return

    summary = t(language, "failed.summary_all", count=len(records)) if show_all else t(language, "failed.summary_partial", shown=len(records), total=failed_count)
    await update.message.reply_text(summary, reply_to_message_id=update.message.message_id)

    sent_count = 0
    skipped_count = 0
    for idx, record in enumerate(records, 1):
        file_name = os.path.basename(record['file_path'])
        fail_count = record['ocr_fail_count']
        msg_id_from_filename = None
        if '_' in file_name:
            parts = file_name.split('_')
            if parts[0].isdigit():
                msg_id_from_filename = int(parts[0])

        update_time = datetime.fromtimestamp(record['updated_time']).strftime('%m-%d %H:%M') if record['updated_time'] else ""
        message_text = t(language, "failed.record_reply", index=idx, fail_count=fail_count, update_time=update_time)

        if msg_id_from_filename:
            try:
                await context.bot.send_message(
                    chat_id=update.effective_chat.id,
                    text=message_text,
                    reply_to_message_id=msg_id_from_filename,
                )
                sent_count += 1
                if idx < len(records):
                    await asyncio.sleep(0.3)
            except Exception as e:
                deps.logger.warning(f"Failed to reply to message {msg_id_from_filename}: {e}")
                skipped_count += 1
        else:
            await context.bot.send_message(
                chat_id=update.effective_chat.id,
                text=t(language, "failed.record_no_link", index=idx, file_name=file_name, fail_count=fail_count, update_time=update_time),
                parse_mode='Markdown',
            )
            sent_count += 1
            skipped_count += 1

    complete_msg = t(language, "failed.complete", sent_count=sent_count)
    if skipped_count > 0:
        complete_msg += t(language, "failed.complete_skipped", skipped_count=skipped_count)
    if not show_all and failed_count > len(records):
        complete_msg += t(language, "failed.complete_more", total=failed_count)

    await context.bot.send_message(chat_id=update.effective_chat.id, text=complete_msg)


async def language_command(deps: BotDeps, update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.message.from_user.id != ALLOWED_USER_ID:
        deps.logger.warning(f"Unauthorized user {update.message.from_user.id} tried to interact with /language.")
        return

    current_language = get_effective_language(deps, update, context)
    if not context.args:
        await update.message.reply_text(
            t(current_language, "language.current", language_name=get_language_name(current_language)),
            parse_mode='Markdown',
            reply_to_message_id=update.message.message_id,
        )
        return

    raw_value = context.args[0]
    if not is_supported_language(raw_value):
        await update.message.reply_text(
            t(current_language, "language.unsupported", value=raw_value),
            reply_to_message_id=update.message.message_id,
        )
        return

    new_language = normalize_language(raw_value, DEFAULT_LANGUAGE)
    if new_language not in SUPPORTED_LANGUAGES:
        await update.message.reply_text(
            t(current_language, "language.unsupported", value=raw_value),
            reply_to_message_id=update.message.message_id,
        )
        return

    deps.searcher.set_user_language(update.message.from_user.id, new_language)
    context.user_data["language"] = new_language
    await update.message.reply_text(
        t(new_language, "language.switched", language_name=get_language_name(new_language)),
        reply_to_message_id=update.message.message_id,
    )


async def help_command(deps: BotDeps, update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.message.from_user.id != ALLOWED_USER_ID:
        deps.logger.warning(f"Unauthorized user {update.message.from_user.id} tried to interact with /help.")
        return

    language = get_effective_language(deps, update, context)
    try:
        help_text = load_help_document(language)
    except OSError as e:
        deps.logger.error(f"Failed to load help document: {e}")
        await update.message.reply_text(
            t(language, "help.unavailable"),
            reply_to_message_id=update.message.message_id,
        )
        return

    await update.message.reply_text(
        help_text,
        parse_mode='Markdown',
        reply_to_message_id=update.message.message_id,
    )
