# Image Tagging

一个用于管理、检索和 OCR 标记图片的 Telegram Bot。

## 功能

- 自动索引上传到 Telegram 的图片
- 基于 MD5 和感知哈希进行重复/相似图片搜索
- 基于 OCR 文本进行搜索
- 支持手动补充、清除 OCR 文本
- 支持定时 OCR 和失败重试
- 图片数量过多时自动归档

## 快速开始

### 1. 安装依赖

```bash
pip install -r requirements.txt
```

### 2. 配置

编辑 `config.py`，至少设置以下字段：

```python
BOT_TOKEN = "your_bot_token"
ALLOWED_USER_ID = 123456789
OCR_SCHEDULED_TIME = "04:00"
```

### 3. 启动

```bash
python telegram_bot.py
```

## Docker

先确认 `config.py` 中的配置已改好，再构建和启动：

```bash
docker build -t image_tagging:latest .
docker run -d --name image_tagging_bot \
  -v $(pwd)/downloads:/app/downloads \
  image_tagging:latest
```

或使用 Docker Compose：

```bash
docker-compose up -d
docker-compose logs -f
docker-compose down
```

## 常用命令

- `/find 关键词`：按 OCR 文本搜索图片
- `回复图片 /find`：以图搜图
- `/r`：随机发送图片
- `/ocr`：处理待 OCR 图片
- `回复图片 /tag 文本`：手动设置 OCR 文本
- `回复图片 /untag`：清除 OCR 文本并重置为待处理
- `回复图片 /link 消息ID或链接`：补充消息 ID
- `/getocr`：查看图片 OCR 文本
- `/failed`：查看 OCR 失败记录
- `/help`：以 Markdown 形式输出当前语言对应的命令说明
- `/language <zh|en>` 或 `/lang <zh|en>`：在中文和英文之间切换 bot 输出语言

详细命令说明见 `COMMANDS.md`。

## 常用检查命令

```bash
python -m py_compile telegram_bot.py image_searcher.py config.py i18n.py
docker-compose logs -f
```

## 主要文件

- `telegram_bot.py`：Telegram Bot 入口与命令处理
- `image_searcher.py`：索引、OCR、搜索、归档逻辑
- `config.py`：运行配置
- `COMMANDS.md`：命令详细说明

## 运行时生成文件

- `downloads/`
- `image_index.db`
- `bot.log`
