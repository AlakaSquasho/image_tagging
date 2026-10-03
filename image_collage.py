"""把一页搜索结果拼接成单张网格图（纯图像处理，无 Telegram 依赖）。"""
import io
import logging
import math
import os
from typing import Optional, Sequence, Tuple

from PIL import Image, ImageOps

from config import FIND_MERGE_CELL_SIZE

DEFAULT_JPEG_QUALITY = 85
MIN_CELL_SIZE = 64
MAX_CELL_SIZE = 1024
DEFAULT_BACKGROUND = (255, 255, 255)


def compute_grid(count: int) -> Tuple[int, int]:
    """返回 (columns, rows)：ceil(sqrt(n)) 列，尽量接近正方形。9 -> (3, 3)。"""
    if count <= 0:
        return (0, 0)
    columns = max(1, math.ceil(math.sqrt(count)))
    rows = math.ceil(count / columns)
    return columns, rows


def _load_rgb(path: str, background: Tuple[int, int, int], logger: Optional[logging.Logger]) -> Optional[Image.Image]:
    """打开图片并统一为 RGB（含 EXIF 方向与透明通道处理）；失败返回 None。"""
    try:
        with Image.open(path) as source:
            # exif_transpose 始终返回脱离文件句柄的副本，因此可安全关闭 source。
            image = ImageOps.exif_transpose(source)
            if image.mode == "P" and "transparency" in image.info:
                image = image.convert("RGBA")
            if image.mode in ("RGBA", "LA"):
                rgba = image.convert("RGBA")
                base = Image.new("RGBA", rgba.size, background + (255,))
                image = Image.alpha_composite(base, rgba).convert("RGB")
            elif image.mode != "RGB":
                image = image.convert("RGB")
            return image
    except Exception as e:
        if logger:
            logger.warning(f"Skipping unreadable image for collage {path}: {e}")
        return None


def build_collage_image(
    paths: Sequence[str],
    *,
    cell_size: int = FIND_MERGE_CELL_SIZE,
    columns: int = 0,
    background: Tuple[int, int, int] = DEFAULT_BACKGROUND,
    quality: int = DEFAULT_JPEG_QUALITY,
    logger: Optional[logging.Logger] = None,
) -> Optional[bytes]:
    """
    将多张图片拼成网格 JPEG，返回编码后的字节；无可用图片时返回 None。

    每张图等比缩放后完整放入单元格（contain），居中留白，不裁切、不放大。
    columns 为 0 时按 compute_grid 自动排版。
    """
    valid_paths = [path for path in paths if os.path.exists(path)]
    if not valid_paths:
        return None

    cell = max(MIN_CELL_SIZE, min(int(cell_size), MAX_CELL_SIZE))
    jpeg_quality = max(1, min(int(quality), 95))

    auto_columns, _ = compute_grid(len(valid_paths))
    cols = columns if columns > 0 else auto_columns
    rows = math.ceil(len(valid_paths) / cols)

    canvas = Image.new("RGB", (cols * cell, rows * cell), background)
    pasted = 0
    try:
        for index, path in enumerate(valid_paths):
            image = _load_rgb(path, background, logger)
            if image is None:
                continue
            try:
                image.thumbnail((cell, cell), Image.LANCZOS)
                width, height = image.size
                row, col = divmod(index, cols)
                x = col * cell + (cell - width) // 2
                y = row * cell + (cell - height) // 2
                canvas.paste(image, (x, y))
                pasted += 1
            finally:
                image.close()

        if pasted == 0:
            return None

        buffer = io.BytesIO()
        canvas.save(buffer, format="JPEG", quality=jpeg_quality, optimize=True)
        return buffer.getvalue()
    finally:
        canvas.close()
