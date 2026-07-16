import asyncio
from datetime import datetime

from telegram import Update
from telegram.ext import ContextTypes

from config import ALLOWED_USER_ID, OCR_BATCH_SIZE, OCR_MAX_RETRIES
from i18n import t

from bot_common import BotDeps, get_effective_language, get_user_language
from bot_ui import create_progress_bar


async def ocr_command(deps: BotDeps, update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    处理 /ocr 命令，立即对所有未 OCR 的图片进行处理。
    bot 侧负责进度展示与消息更新，OCR 实际执行仍由 searcher 承担。
    """
    import gc

    deps.logger.info(f"🔤 Received /ocr command from user {update.message.from_user.id}")

    if update.message.from_user.id != ALLOWED_USER_ID:
        deps.logger.warning(f"❌ Unauthorized user {update.message.from_user.id} tried to interact with /ocr.")
        return

    language = get_effective_language(deps, update, context)
    pending_count = deps.searcher.get_pending_ocr_count(OCR_MAX_RETRIES)
    if pending_count == 0:
        await update.message.reply_text(t(language, "ocr.none_pending"))
        return

    status_message = await update.message.reply_text(
        t(
            language,
            "ocr.start",
            pending_count=pending_count,
            progress_bar=create_progress_bar(0, pending_count),
        )
    )

    try:
        total_stats = {'processed': 0, 'succeeded': 0, 'failed': 0, 'skipped': 0}
        iteration = 0
        max_iterations = 100
        start_time = datetime.now()
        last_update_time = start_time

        while iteration < max_iterations:
            iteration += 1
            remaining = deps.searcher.get_pending_ocr_count(OCR_MAX_RETRIES)
            if remaining == 0:
                deps.logger.info(f"Force OCR: All images processed after {iteration} iterations.")
                break

            deps.logger.info(f"Force OCR iteration {iteration}: Processing {remaining} pending images...")
            loop = asyncio.get_running_loop()
            # OCR 批处理仍在同步 searcher 中执行，这里通过 executor 避免阻塞 bot 事件循环。
            stats = await loop.run_in_executor(
                None,
                lambda: deps.searcher.process_ocr_pending_images(batch_size=OCR_BATCH_SIZE, max_retries=OCR_MAX_RETRIES)
            )

            total_stats['processed'] += stats['processed']
            total_stats['succeeded'] += stats['succeeded']
            total_stats['failed'] += stats['failed']
            total_stats['skipped'] += stats['skipped']

            now = datetime.now()
            if (now - last_update_time).total_seconds() >= 0.5 or remaining == 0:
                try:
                    elapsed_str = f"{int((now - start_time).total_seconds())}s"
                    progress_text = t(
                        language,
                        "ocr.progress",
                        pending_count=pending_count,
                        progress_bar=create_progress_bar(total_stats['processed'], pending_count),
                        processed=total_stats['processed'],
                        elapsed_str=elapsed_str,
                    )
                    await context.bot.edit_message_text(
                        chat_id=update.effective_chat.id,
                        message_id=status_message.message_id,
                        text=progress_text,
                    )
                    last_update_time = now
                except Exception as e:
                    deps.logger.debug(f"Failed to update progress message: {e}")

            if stats['processed'] == 0:
                deps.logger.warning(f"No images were processed in iteration {iteration}, stopping.")
                break

            gc.collect()

        end_time = datetime.now()
        total_elapsed = end_time - start_time
        elapsed_minutes = int(total_elapsed.total_seconds() // 60)
        elapsed_seconds = int(total_elapsed.total_seconds() % 60)
        total_time_str = f"{elapsed_minutes}m {elapsed_seconds}s" if elapsed_minutes > 0 else f"{elapsed_seconds}s"

        message = t(
            language,
            "ocr.done",
            progress_bar=create_progress_bar(total_stats['processed'], pending_count),
            processed=total_stats['processed'],
            pending_count=pending_count,
            succeeded=total_stats['succeeded'],
            failed=total_stats['failed'],
            skipped=total_stats['skipped'],
            iteration=iteration,
            total_time_str=total_time_str,
        )
        if total_stats['failed'] > 0:
            message += t(language, "ocr.failed_details", failed=total_stats['failed'], max_retries=OCR_MAX_RETRIES)
        if total_stats['succeeded'] > 0:
            message += t(language, "ocr.success_hint", succeeded=total_stats['succeeded'])

        await context.bot.edit_message_text(
            chat_id=update.effective_chat.id,
            message_id=status_message.message_id,
            text=message,
        )
        deps.logger.info(f"Force OCR completed: {total_stats}, iterations: {iteration}")
        gc.collect()
    except Exception as e:
        deps.logger.error(f"Error during force OCR: {e}", exc_info=True)
        error_message = t(language, "ocr.error", error=str(e))
        try:
            await context.bot.edit_message_text(
                chat_id=update.effective_chat.id,
                message_id=status_message.message_id,
                text=error_message,
            )
        except Exception:
            await update.message.reply_text(error_message)


async def scheduled_ocr_task(deps: BotDeps, context: ContextTypes.DEFAULT_TYPE):
    """
    定时执行 OCR 任务，处理所有待处理的图片。
    与手动 /ocr 共享同一批处理入口，但这里额外负责定时通知和超时保护。
    """
    import gc
    from concurrent.futures import ThreadPoolExecutor

    task_start_time = datetime.now()
    deps.logger.info(f"Scheduled OCR task started at: {task_start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="ocr_scheduled")
    network_timeout = 30.0
    language = get_user_language(deps, context, ALLOWED_USER_ID)

    async def safe_send_message(text: str) -> bool:
        try:
            await asyncio.wait_for(
                context.bot.send_message(chat_id=ALLOWED_USER_ID, text=text),
                timeout=network_timeout,
            )
            return True
        except asyncio.TimeoutError:
            deps.logger.error(f"Timeout sending message: {text[:50]}...")
            return False
        except Exception as e:
            deps.logger.error(f"Error sending message: {e}")
            return False

    try:
        pending_count = deps.searcher.get_pending_ocr_count(OCR_MAX_RETRIES)
        if pending_count == 0:
            deps.logger.info("Scheduled OCR task: No pending images.")
            await safe_send_message(
                t(language, "scheduled.none_pending", task_time=task_start_time.strftime('%Y-%m-%d %H:%M:%S'))
            )
            return

        deps.logger.info(f"Starting scheduled OCR task for {pending_count} images...")
        total_stats = {'processed': 0, 'succeeded': 0, 'failed': 0, 'skipped': 0}
        iteration = 0
        max_iterations = 100

        while iteration < max_iterations:
            iteration += 1
            remaining = deps.searcher.get_pending_ocr_count(OCR_MAX_RETRIES)
            if remaining == 0:
                deps.logger.info(f"All pending images have been processed after {iteration} iterations.")
                break

            deps.logger.info(f"OCR task iteration {iteration}: Processing {remaining} pending images...")
            loop = asyncio.get_running_loop()
            try:
                stats = await asyncio.wait_for(
                    loop.run_in_executor(
                        executor,
                        lambda: deps.searcher.process_ocr_pending_images(batch_size=OCR_BATCH_SIZE, max_retries=OCR_MAX_RETRIES)
                    ),
                    timeout=600.0,
                )
            except asyncio.TimeoutError:
                deps.logger.error(f"OCR batch processing timeout in iteration {iteration}")
                total_stats['failed'] += OCR_BATCH_SIZE
                break

            total_stats['processed'] += stats['processed']
            total_stats['succeeded'] += stats['succeeded']
            total_stats['failed'] += stats['failed']
            total_stats['skipped'] += stats['skipped']

            if stats['processed'] == 0:
                deps.logger.warning(f"No images were processed in iteration {iteration}, stopping to avoid infinite loop.")
                break

            deps.logger.info(f"Iteration {iteration} completed: {stats}")
            gc.collect()
            deps.logger.info(f"💓 Heartbeat: OCR task still running after iteration {iteration}")

        task_end_time = datetime.now()
        duration_str = f"{int((task_end_time - task_start_time).total_seconds())}s"
        message = t(
            language,
            "scheduled.done",
            processed=total_stats['processed'],
            succeeded=total_stats['succeeded'],
            failed=total_stats['failed'],
            skipped=total_stats['skipped'],
            iteration=iteration,
            start_time=task_start_time.strftime('%H:%M:%S'),
            end_time=task_end_time.strftime('%H:%M:%S'),
            duration_str=duration_str,
        )
        if total_stats['failed'] > 0:
            message += t(language, "scheduled.failed_note", failed=total_stats['failed'], max_retries=OCR_MAX_RETRIES)

        await safe_send_message(message)
        deps.logger.info(f"Scheduled OCR task completed successfully: {total_stats}, iterations: {iteration}, duration: {duration_str}")
    except Exception as e:
        duration_str = f"{int((datetime.now() - task_start_time).total_seconds())}s"
        deps.logger.error(f"Error in scheduled OCR task: {e}", exc_info=True)
        await safe_send_message(
            t(
                language,
                "scheduled.error",
                error=str(e),
                task_time=task_start_time.strftime('%Y-%m-%d %H:%M:%S'),
                duration_str=duration_str,
            )
        )
    finally:
        try:
            executor.shutdown(wait=False, cancel_futures=True)
            deps.logger.info("OCR executor shutdown completed")
        except Exception as e:
            deps.logger.error(f"Error shutting down executor: {e}")
        gc.collect()
        total_duration = (datetime.now() - task_start_time).total_seconds()
        deps.logger.info(f"🏁 Scheduled OCR task cleanup completed. Total duration: {total_duration:.1f}s")
