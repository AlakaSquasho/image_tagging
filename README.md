# Image Tagging

A Telegram bot for indexing, searching, and OCR-tagging images.

[中文说明 / Chinese README](./README_zh.md)

## Features

- Automatically indexes images sent to the bot
- Detects exact duplicates and visually similar images with MD5 and perceptual hash
- Searches images by OCR text
- Supports manual OCR text update and reset
- Supports scheduled OCR and retry for failed records
- Automatically archives images when the download folder grows too large

## Quick Start

### 1. Install dependencies

```bash
pip install -r requirements.txt
```

### 2. Configure

Edit `config.py` and set at least these values:

```python
BOT_TOKEN = "your_bot_token"
ALLOWED_USER_ID = 123456789
OCR_SCHEDULED_TIME = "04:00"
```

### 3. Run

```bash
python telegram_bot.py
```

## Docker

Make sure `config.py` is configured first, then build and run:

```bash
docker build -t image_tagging:latest .
docker run -d --name image_tagging_bot \
  -v $(pwd)/downloads:/app/downloads \
  image_tagging:latest
```

Or use Docker Compose:

```bash
docker-compose up -d
docker-compose logs -f
docker-compose down
```

## Common Commands

- `/find <keyword>`: search by OCR text
- `reply to an image with /find`: search by image
- `/r`: send random images
- `/ocr`: process pending OCR images
- `reply to an image with /tag <text>`: set OCR text manually
- `reply to an image with /untag`: clear OCR text and reset it to pending
- `reply to an image with /link <message id or link>`: attach a message ID
- `/getocr`: view OCR text for an image
- `/failed`: view failed OCR records
- `/help`: output the localized command reference in Markdown
- `/language <zh|en>` or `/lang <zh|en>`: switch the bot output language between Chinese and English

See `COMMANDS.md` for detailed command usage.

## Useful Checks

```bash
python -m py_compile telegram_bot.py image_searcher.py config.py i18n.py
docker-compose logs -f
```

## Main Files

- `telegram_bot.py`: Telegram bot entrypoint and command handlers
- `image_searcher.py`: indexing, OCR, search, and archive logic
- `config.py`: runtime configuration
- `COMMANDS.md`: command reference

## Runtime Files

- `downloads/`
- `image_index.db`
- `bot.log`
