import os
from typing import Dict, List, Optional, Tuple

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, InputMediaPhoto, Update
from telegram.ext import ContextTypes

from config import DEFAULT_LANGUAGE, FIND_PAGE_SIZE, RANDOM_DEFAULT_COUNT
from i18n import t

from bot_common import BotDeps, get_effective_language, translate


def create_progress_bar(current: int, total: int, bar_length: int = 20) -> str:
    """创建 OCR 进度展示使用的 ASCII 进度条。"""
    if total == 0:
        return "■" * bar_length + " 0%"

    percentage = current / total
    filled = int(bar_length * percentage)
    bar = "█" * filled + "░" * (bar_length - filled)
    percent_str = f"{percentage * 100:.1f}%"
    return f"{bar} {percent_str}"


def get_find_page_size() -> int:
    """读取 /find 分页大小，并限制在 Telegram 允许的安全范围内。"""
    try:
        page_size = int(FIND_PAGE_SIZE)
    except (TypeError, ValueError):
        page_size = 9

    if page_size < 1 or page_size > 9:
        return 9
    return page_size


def paginate_results(results: List[Dict], page: int, page_size: int) -> Tuple[List[Dict], int]:
    total = len(results)
    total_pages = max(1, (total + page_size - 1) // page_size)
    safe_page = min(max(page, 1), total_pages)
    start = (safe_page - 1) * page_size
    end = start + page_size
    return results[start:end], total_pages


def build_find_keyboard(page: int, total_pages: int, query_id: str, state: Optional[Dict] = None) -> Optional[InlineKeyboardMarkup]:
    keyboard_rows = []
    language = (state or {}).get("language", DEFAULT_LANGUAGE)

    if total_pages > 1:
        prev_page = max(1, page - 1)
        next_page = min(total_pages, page + 1)

        keyboard_rows.append([
            InlineKeyboardButton(t(language, "common.page_prev"), callback_data=f"find_page:{query_id}:{prev_page}"),
            InlineKeyboardButton(f"{page}/{total_pages}", callback_data="find_page:noop:0"),
            InlineKeyboardButton(t(language, "common.page_next"), callback_data=f"find_page:{query_id}:{next_page}"),
        ])

    if state and state.get("mode") == "random":
        keyboard_rows.append([
            InlineKeyboardButton(t(language, "common.reroll"), callback_data=f"find_random:{query_id}:reroll")
        ])

    if not keyboard_rows:
        return None
    return InlineKeyboardMarkup(keyboard_rows)


def get_find_summary_text(state: Dict, page: int, total_pages: int) -> str:
    summary = state.get("summary", "")
    total = len(state.get("results", []))
    language = state.get("language", DEFAULT_LANGUAGE)
    return f"{summary}\n{t(language, 'common.page_summary', page=page, total_pages=total_pages, total=total)}"


def build_find_summary_text(state: Dict, page: int, total_pages: int, page_results: List[Dict]) -> str:
    summary_text = get_find_summary_text(state, page, total_pages)
    page_size = state.get("page_size", 9)
    start_index = (page - 1) * page_size
    language = state.get("language", DEFAULT_LANGUAGE)
    link_lines = []

    for index, result in enumerate(page_results, start=start_index + 1):
        if result.get("telegram_message_id"):
            link_lines.append(t(language, "common.original_link", index=index, message_id=result["telegram_message_id"]))
        else:
            link_lines.append(t(language, "common.no_original_link", index=index))

    if link_lines:
        summary_text = f"{summary_text}\n\n" + "\n".join(link_lines)
    return summary_text


async def render_find_page(
    deps: BotDeps,
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
            await update.callback_query.answer(translate(deps, context, update, "find.pagination_expired"), show_alert=False)
        return

    results = state.get("results", [])
    page_size = state.get("page_size", 9)
    page_results, total_pages = paginate_results(results, page, page_size)
    page = min(max(page, 1), total_pages)

    keyboard = build_find_keyboard(page, total_pages, query_id, state)
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

    # 翻页时删除上一页的摘要和媒体消息，避免结果不断堆积在聊天里。
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
        if not os.path.exists(result["path"]):
            deps.logger.warning(f"Search result file not found: {result['path']}")
            continue
        try:
            with open(result["path"], "rb") as photo_file:
                media_group.append(InputMediaPhoto(media=photo_file.read()))
        except Exception as e:
            deps.logger.error(f"Failed to prepare search result image: {e}")

    if media_group:
        try:
            media_messages = await context.bot.send_media_group(chat_id=chat_id, media=media_group)
            message_ids.extend([m.message_id for m in media_messages])
        except Exception as e:
            deps.logger.error(f"Failed to send media group: {e}")

    if summary_message_id:
        message_ids.append(summary_message_id)
    else:
        summary_message = await context.bot.send_message(
            chat_id=chat_id,
            text=summary_text,
            reply_markup=keyboard,
            reply_to_message_id=update.message.message_id if update.message else None,
        )
        summary_message_id = summary_message.message_id
        message_ids.append(summary_message_id)

    state["message_ids"] = message_ids
    state["current_page"] = page
    state["summary_message_id"] = summary_message_id


def create_handle_find_page_callback(deps: BotDeps):
    """创建分页 callback handler，避免在模块级持有 bot 运行时依赖。"""
    async def handle_find_page_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
        query = update.callback_query
        if not query or not query.data:
            return

        parts = query.data.split(":")
        if len(parts) != 3:
            await query.answer()
            return

        action, query_id, value = parts
        if query_id == "noop":
            await query.answer()
            return

        if action == "find_random" and value == "reroll":
            state = context.user_data.get("find_pagination", {}).get(query_id)
            language = (state or {}).get("language", get_effective_language(deps, update, context))
            if not state or state.get("mode") != "random":
                await query.answer(t(language, "find.random_expired"), show_alert=False)
                return

            requested_count = state.get("requested_count", RANDOM_DEFAULT_COUNT)
            results = deps.searcher.get_random_images(requested_count)
            if not results:
                await query.answer(t(language, "find.no_random_images"), show_alert=False)
                return

            state["results"] = results
            state["summary"] = t(language, "random.summary", count=len(results))
            await query.answer(t(language, "find.rerolled"))
            await render_find_page(deps, update, context, query_id, 1, is_callback=True)
            return

        try:
            page = int(value)
        except ValueError:
            await query.answer()
            return

        await query.answer()
        await render_find_page(deps, update, context, query_id, page, is_callback=True)

    return handle_find_page_callback
