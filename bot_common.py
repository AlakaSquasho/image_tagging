from dataclasses import dataclass
import logging
from typing import Any, Optional

from telegram import Update
from telegram.ext import ContextTypes

from config import ALLOWED_USER_ID, DEFAULT_LANGUAGE
from i18n import normalize_language, t


@dataclass(frozen=True)
class BotDeps:
    searcher: Any
    logger: logging.Logger


def get_user_language(deps: BotDeps, context: ContextTypes.DEFAULT_TYPE, user_id: Optional[int] = None) -> str:
    user_data = getattr(context, "user_data", None)
    if user_data and user_data.get("language"):
        return normalize_language(user_data["language"], DEFAULT_LANGUAGE)

    if user_id is not None:
        stored_language = deps.searcher.get_user_language(user_id)
        if stored_language:
            language = normalize_language(stored_language, DEFAULT_LANGUAGE)
            if user_data is not None:
                user_data["language"] = language
            return language

    return DEFAULT_LANGUAGE


def get_effective_language(deps: BotDeps, update: Optional[Update], context: ContextTypes.DEFAULT_TYPE) -> str:
    if update and update.effective_user:
        user_id = update.effective_user.id
    else:
        user_id = ALLOWED_USER_ID
    return get_user_language(deps, context, user_id)


def translate(deps: BotDeps, context: ContextTypes.DEFAULT_TYPE, update: Optional[Update], key: str, **kwargs) -> str:
    language = get_effective_language(deps, update, context)
    return t(language, key, **kwargs)
