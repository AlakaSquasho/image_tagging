import os
from datetime import datetime
from typing import Dict, Optional

from i18n import t


def format_result_caption(language: str, result: Dict, *, include_similarity: bool = False, no_message_id_key: Optional[str] = None) -> str:
    parts = []
    if result.get('telegram_message_id'):
        parts.append(t(language, 'find.result_message_id_line', index=1, message_id=result['telegram_message_id']).split('. ', 1)[1])
    elif no_message_id_key:
        parts.append(t(language, no_message_id_key))

    parts.append(f"File: `{os.path.basename(result['path'])}`" if language == 'en' else f"文件路径: `{os.path.basename(result['path'])}`")
    parts.append(f"File hash: `{result['file_hash']}`" if language == 'en' else f"文件哈希: `{result['file_hash']}`")
    parts.append(
        f"Updated at: {datetime.fromtimestamp(result['updated_time']).strftime('%Y-%m-%d %H:%M:%S')}"
        if language == 'en'
        else f"更新时间: {datetime.fromtimestamp(result['updated_time']).strftime('%Y-%m-%d %H:%M:%S')}"
    )
    if include_similarity and 'similarity' in result:
        parts.append(f"Similarity: {result['similarity']:.2%}" if language == 'en' else f"相似度: {result['similarity']:.2%}")
    if 'ocr_text' in result and result['ocr_text']:
        display_ocr_text = result['ocr_text'][:100] + "..." if len(result['ocr_text']) > 100 else result['ocr_text']
        parts.append(f"OCR text: `{display_ocr_text}`" if language == 'en' else f"OCR文本: `{display_ocr_text}`")

    return "\n".join(parts)
