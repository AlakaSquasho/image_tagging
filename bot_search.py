import os
from typing import Dict, Optional
from uuid import uuid4

from telegram import InputFile, Update
from telegram.ext import ContextTypes

from config import ALLOWED_USER_ID, FIND_PAGINATION_ENABLED, IMAGE_DOWNLOAD_PATH, MAX_RESULTS, RANDOM_DEFAULT_COUNT
from i18n import t

from bot_common import BotDeps, get_effective_language
from bot_formatting import format_result_caption
from bot_media import search_by_image
from bot_ui import get_find_page_size, render_find_page


async def find_command(deps: BotDeps, update: Update, context: ContextTypes.DEFAULT_TYPE):
    """处理 /find：支持文本搜索和回复图片的按图搜索。"""
    deps.logger.info(f"🔍 Received /find command from user {update.message.from_user.id}")

    if update.message.from_user.id != ALLOWED_USER_ID:
        deps.logger.warning(f"❌ Unauthorized user {update.message.from_user.id} tried to interact with /find.")
        return

    language = get_effective_language(deps, update, context)

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
                deps.logger.error(f"Downloaded file is empty or doesn't exist: {temp_file_path}")
                await update.message.reply_text(t(language, "find.download_empty"), reply_to_message_id=update.message.message_id)
                return

            await search_by_image(deps, update, context, temp_file_path)
        except IOError as e:
            deps.logger.error(f"IO error processing search via replied photo: {e}", exc_info=True)
            await update.message.reply_text(t(language, "find.download_read_error"), reply_to_message_id=update.message.message_id)
        except Exception as e:
            deps.logger.error(f"Error processing search via replied photo: {e}", exc_info=True)
            await update.message.reply_text(t(language, "find.reply_photo_search_error"), reply_to_message_id=update.message.message_id)
        finally:
            if os.path.exists(temp_file_path):
                try:
                    os.remove(temp_file_path)
                except OSError as e:
                    deps.logger.error(f"Failed to clean up temporary search file {temp_file_path}: {e}")
        return

    if context.args:
        try:
            search_mode = 'exact'
            max_results = MAX_RESULTS
            keywords_args = list(context.args)

            i = 0
            while i < len(keywords_args):
                arg = keywords_args[i]
                # 支持从命令参数中解析搜索模式和结果数上限。
                if arg.startswith('--'):
                    param = arg[2:]
                    if param == 'com':
                        param = 'comprehensive'
                    if param in ['exact', 'comprehensive', 'contains']:
                        search_mode = param
                        keywords_args.pop(i)
                        continue
                    await update.message.reply_text(
                        t(language, "find.invalid_mode", arg=arg),
                        reply_to_message_id=update.message.message_id,
                    )
                    return
                elif arg.startswith('-n=') or arg.startswith('--max=') or (arg.startswith('-') and arg[1:].isdigit()):
                    try:
                        if arg.startswith('-n='):
                            max_results = int(arg[3:])
                        elif arg.startswith('--max='):
                            max_results = int(arg[6:])
                        else:
                            max_results = int(arg[1:])

                        if max_results <= 0:
                            raise ValueError("max_results must be greater than 0")
                        keywords_args.pop(i)
                        continue
                    except ValueError:
                        await update.message.reply_text(
                            t(language, "find.invalid_max_results", arg=arg),
                            reply_to_message_id=update.message.message_id,
                        )
                        return
                i += 1

            keywords = " ".join(keywords_args)
            if not keywords.strip():
                await update.message.reply_text(
                    t(language, "find.missing_keywords"),
                    parse_mode='Markdown',
                    reply_to_message_id=update.message.message_id,
                )
                return

            results = deps.searcher.search_by_text(keywords, max_results=max_results, search_mode=search_mode)
            deps.logger.info(f"Text search for '{keywords}' using mode '{search_mode}' returned {len(results)} results")
            if not results:
                await update.message.reply_text(
                    t(language, "find.no_text_results", search_mode=search_mode),
                    reply_to_message_id=update.message.message_id,
                )
                return

            mode_desc = t(language, f"find.mode.{search_mode}")

            if len(results) == 1:
                result = results[0]
                if result.get('telegram_message_id'):
                    message = t(language, "find.single_result_with_id", mode_desc=mode_desc, message_id=result['telegram_message_id'])
                    await update.message.reply_text(message, reply_to_message_id=update.message.message_id, parse_mode='HTML')
                else:
                    filename = os.path.basename(result['path'])
                    message = t(language, "find.single_result_file", mode_desc=mode_desc, filename=filename)
                    await update.message.reply_text(message, reply_to_message_id=update.message.message_id, parse_mode='HTML')
                    try:
                        if os.path.exists(result['path']):
                            with open(result['path'], 'rb') as photo:
                                await context.bot.send_photo(
                                    chat_id=update.effective_chat.id,
                                    photo=InputFile(photo, filename=filename),
                                    caption=t(language, "find.photo_caption", filename=filename),
                                    reply_to_message_id=update.message.message_id,
                                )
                    except Exception as e:
                        deps.logger.error(f"Failed to send search result image: {e}")
                        await update.message.reply_text(t(language, "common.send_image_failed", filename=filename))
                return

            if FIND_PAGINATION_ENABLED and len(results) > 1:
                query_id = str(uuid4())
                page_size = get_find_page_size()
                context.user_data.setdefault("find_pagination", {})[query_id] = {
                    "results": results,
                    "mode": "text",
                    "search_mode": search_mode,
                    "page_size": page_size,
                    "summary": t(language, "find.multi_results_summary", count=len(results), mode_desc=mode_desc),
                    "message_ids": [],
                    "language": language,
                }
                await render_find_page(deps, update, context, query_id, 1, is_callback=False)
                return

            await update.message.reply_text(
                t(language, "find.multi_results_summary", count=len(results), mode_desc=mode_desc),
                reply_to_message_id=update.message.message_id,
            )

            with_message_id = []
            without_message_id = []
            for result in results:
                if result.get('telegram_message_id'):
                    with_message_id.append(result)
                else:
                    without_message_id.append(result)

            if with_message_id:
                message_lines = [
                    t(language, "find.result_message_id_line", index=idx, message_id=result['telegram_message_id'])
                    for idx, result in enumerate(with_message_id, 1)
                ]
                await context.bot.send_message(chat_id=update.effective_chat.id, text="\n".join(message_lines), parse_mode='HTML')

            for idx, result in enumerate(without_message_id, len(with_message_id) + 1):
                filename = os.path.basename(result['path'])
                await context.bot.send_message(
                    chat_id=update.effective_chat.id,
                    text=t(language, "find.result_file_line", index=idx, filename=filename),
                    parse_mode='HTML',
                )
                try:
                    if os.path.exists(result['path']):
                        with open(result['path'], 'rb') as photo:
                            await context.bot.send_photo(
                                chat_id=update.effective_chat.id,
                                photo=InputFile(photo, filename=filename),
                                caption=t(language, "find.photo_caption", filename=filename),
                            )
                except Exception as e:
                    deps.logger.error(f"Failed to send search result image: {e}")
                    await context.bot.send_message(
                        chat_id=update.effective_chat.id,
                        text=t(language, "common.send_image_failed", filename=filename),
                    )
        except Exception as e:
            deps.logger.error(f"Error during text search: {e}", exc_info=True)
            await update.message.reply_text(t(language, "find.text_search_error"), reply_to_message_id=update.message.message_id)
        return

    await update.message.reply_text(t(language, "find.usage"), parse_mode='HTML', reply_to_message_id=update.message.message_id)


async def random_command(deps: BotDeps, update: Update, context: ContextTypes.DEFAULT_TYPE):
    """处理 /r：随机返回若干已索引图片。"""
    deps.logger.info(f"🎲 Received /r command from user {update.message.from_user.id}")

    if update.message.from_user.id != ALLOWED_USER_ID:
        deps.logger.warning(f"❌ Unauthorized user {update.message.from_user.id} tried to interact with /r.")
        return

    language = get_effective_language(deps, update, context)
    requested_count = RANDOM_DEFAULT_COUNT
    if context.args:
        if len(context.args) != 1:
            await update.message.reply_text(t(language, "random.usage"), reply_to_message_id=update.message.message_id)
            return

        try:
            requested_count = int(context.args[0])
        except ValueError:
            await update.message.reply_text(t(language, "random.invalid_integer"), reply_to_message_id=update.message.message_id)
            return

    if requested_count <= 0:
        await update.message.reply_text(t(language, "random.invalid_positive"), reply_to_message_id=update.message.message_id)
        return

    results = deps.searcher.get_random_images(requested_count)
    if not results:
        await update.message.reply_text(t(language, "random.no_images"), reply_to_message_id=update.message.message_id)
        return

    query_id = str(uuid4())
    page_size = get_find_page_size()
    context.user_data.setdefault("find_pagination", {})[query_id] = {
        "results": results,
        "mode": "random",
        "page_size": page_size,
        "requested_count": requested_count,
        "summary": t(language, "random.summary", count=len(results)),
        "message_ids": [],
        "language": language,
    }
    await render_find_page(deps, update, context, query_id, 1, is_callback=False)
