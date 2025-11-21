# Image Tagging - Telegram Bot 图像标记系统

> 一个功能强大的 Telegram Bot，用于管理、搜索和标记图像。基于感知哈希和 OCR 技术的智能图像索引系统。

---

## 🎯 功能特性

- ✅ **图像索引** - 快速计算图像哈希建立索引（<1 秒）
- ✅ **重复检测** - 精确检测完全相同的图像
- ✅ **相似搜索** - 查找视觉相似的图像
- ✅ **OCR 识别** - 提取和搜索图像中的文本（懒加载模式，零内存泄漏）
- ✅ **定时处理** - 在指定时间自动执行 OCR
- ✅ **自动归档** - 图像达到阈值时自动整理
- ✅ **异步处理** - 支持高并发上传和操作
- ✅ **命令优化** - 所有命令首字母即可区分，快速输入

---

## 🚀 快速开始

### 1. 安装依赖

```bash
pip install -r requirements.txt
```

### 2. 配置 Bot

编辑 `config.py`：

```python
BOT_TOKEN = "your_bot_token_here"      # 从 BotFather 获取
ALLOWED_USER_ID = 123456789             # 你的 Telegram 用户 ID
OCR_SCHEDULED_TIME = "04:00"            # 定时 OCR 执行时间（北京时间UTC+8）
```

### 3. 启动 Bot

```bash
python telegram_bot.py
```

---

## 🐳 Docker 部署

### 方法 1：Docker 命令行

```bash
docker build -t image_tagging:latest .
docker run -d \
  --name image_tagging_bot \
  -e BOT_TOKEN="your_token" \
  -e ALLOWED_USER_ID="your_id" \
  -v $(pwd)/downloads:/app/downloads \
  image_tagging:latest
```

### 方法 2：Docker Compose（推荐）

```bash
cp .env.example .env  # 填入 BOT_TOKEN 和 ALLOWED_USER_ID
docker-compose up -d
```

---

## 💬 命令用法详解

### 📌 新命令体系（v3.0）

**所有命令首字母即可区分，快速输入！**

| 命令 | 快捷输入 | 功能 |
|------|---------|------|
| `/find` | `/f` | 搜索图片 |
| `/ocr` | `/o` | OCR处理 |
| `/tag` | `/t` | 设置标签 |
| `/untag` | `/u` | 清除标签 |
| `/link` | `/l` | 设置消息ID |

---

### 📸 发送图片 - 自动索引

直接发送图片到Bot即可自动建立索引，支持：
- ✅ **快速索引** - 图片发送后立即计算哈希值
- ✅ **重复检测** - 自动识别完全相同的图片
- ✅ **批量处理** - 支持同时发送多张图片
- ✅ **智能搜索** - 发送图片时附带 `/find` 可立即执行图片搜索

**示例：**
```
直接发送图片                    # 自动索引
发送图片，caption填写 /find    # 以图搜图
```

---

### 🔍 文本搜索 - `/find`

**基础用法：**
```
/find 关键词                    # 智能搜索模式
/find 手机 截图                 # 多关键词搜索
```

**搜索模式：**
```
/find --smart 关键词            # 智能模式（默认）：优先FTS5，失败时回退LIKE
/find --comprehensive 关键词    # 全面模式：FTS5+LIKE结果合并去重
/find --com 关键词              # 全面模式简写（与 --comprehensive 等效）
/find --fts 关键词              # 仅FTS5：快速精确搜索
/find --like 关键词             # 仅模糊：包含性搜索，结果更全
```

**结果数量控制：**
```
/find -5 关键词                 # 限制返回5个结果（简化格式）
/find -n=5 关键词               # 限制返回5个结果（完整格式）
/find --max=10 关键词           # 限制返回10个结果（完整格式）
/find -3 --com 关键词           # 全面搜索，限制3个结果
```

**实际示例：**
```
/f 发票                         # 快捷搜索：搜索包含"发票"的图片
/f --fts 身份证 照片             # 快速搜索身份证照片
/f -2 --like 截图                # 模糊搜索截图，最多2个结果
/f -n=3 --com 二维码             # 全面搜索二维码，最多3个结果
```

---

### 🖼️ 以图搜图 - 回复图片+`/find`

**操作步骤：**
1. 回复任意图片
2. 输入 `/find` 命令（或简写 `/f`）
3. 系统自动查找相似或相同图片

**示例：**
```
[回复图片] /find               # 查找相似图片
[回复图片] /f                  # 快捷方式
```

---

### ⚡ OCR 处理 - `/ocr`

**批量OCR：**
```
/ocr                           # 处理所有待OCR的图片
/o                             # 快捷方式
```

**特性：**
- 实时进度显示
- 自动批次处理
- 内存优化（懒加载模式）
- 失败自动重试

---

### 🏷️ 标签管理

**设置标签 - `/tag`**
```
[回复图片] /tag OCR文本内容     # 手动为图片设置OCR结果
[回复图片] /t 这是一张发票 金额500元  # 快捷方式
```

**特性：**
- 支持无消息ID的图片
- 自动文本清理和规范化
- 实时更新待处理计数

**清除标签 - `/untag`**
```
[回复图片] /untag              # 清除图片的OCR结果
[回复图片] /u                  # 快捷方式
```

---

### 🔗 消息ID管理 - `/link`

**设置消息ID：**
```
[回复图片] /link 消息ID或链接   # 为图片设置Telegram消息ID
[回复图片] /l https://t.me/channel/123  # 快捷方式
```

**限制：**
- 仅适用于没有消息ID的图片
- 已有消息ID的图片无法覆盖

---

## ⚙️ 配置说明

```python
# config.py
BOT_TOKEN = "123:abc"                      # Telegram Bot Token
ALLOWED_USER_ID = 123456                   # 允许的用户 ID
IMAGE_DOWNLOAD_PATH = "./downloads"        # 图像存储目录
DB_PATH = "image_index.db"                 # 数据库路径
LOG_FILE_PATH = "bot.log"                  # 日志文件路径
OCR_SCHEDULED_TIME = "04:00"               # 定时 OCR 时间（北京时间UTC+8）
OCR_MAX_RETRIES = 3                        # OCR 失败重试次数
OCR_BATCH_SIZE = 5                         # 单次处理的最大图像数
MAX_IMAGES_IN_DOWNLOAD_FOLDER = 300        # 自动归档阈值
MAX_RESULTS = 5                            # 搜索返回的最大结果数
```

**调度器配置：**
```python
SCHEDULER_MISFIRE_GRACE_TIME = 300         # 任务延迟容忍时间（秒）
SCHEDULER_MAX_INSTANCES = 1                # 同一任务的最大并发实例数
SCHEDULER_COALESCE = True                  # 合并延迟的任务执行
```

---

## 📊 系统要求

| 项目 | 要求 |
|------|------|
| **Python** | 3.8+ |
| **内存** | 1GB+（OCR懒加载模式，内存占用大幅降低） |
| **磁盘** | 10GB+（根据图像数量） |
| **依赖** | 见 `requirements.txt` |

---

## 📂 项目结构

```
image_tagging/
├── telegram_bot.py          # 主 Bot 文件
├── image_searcher.py        # 图像搜索引擎（懒加载OCR）
├── config.py                # 配置文件
├── requirements.txt         # 依赖列表
├── Dockerfile               # Docker 配置
├── docker-compose.yml       # Docker Compose 配置
├── .dockerignore            # Docker 忽略文件
├── .env.example             # 环境变量示例
├── README.md                # 本文件
├── COMMANDS.md              # 命令参考手册（详细）
├── bot.log                  # 运行日志（自动生成）
├── image_index.db           # 数据库（自动生成）
└── downloads/               # 图像存储目录（自动生成）
```

---

## 🔧 故障排查

### Q: 短时间内上传多张图片失败？
**A:** 已通过 v2.0 解决，图像索引已优化到 <1 秒，支持高并发上传。

### Q: OCR 识别失败率高？
**A:** 检查以下几点：
- 图片质量是否清晰
- 服务器内存是否充足（可减小 `OCR_BATCH_SIZE`）
- PaddleOCR 模型是否完整

### Q: /ocr 命令执行后内存占用过高？
**A:** v3.0 已解决！采用懒加载模式：
- OCR引擎仅在需要时加载
- 每批处理完成后立即释放
- 执行前后内存占用基本一致

### Q: macOS Docker 构建失败？
**A:** 重启 Docker Desktop：
```bash
pkill -9 Docker && sleep 10 && open /Applications/Docker.app
```
然后重新构建。

---

## 📈 性能指标

| 操作 | 耗时 |
|------|------|
| 单张图像索引 | <300ms |
| 完全匹配搜索 | <10ms |
| 相似度搜索 | <100ms |
| 文本搜索（FTS5） | <50ms |
| OCR（单张） | 3-6s |
| OCR（批量10张） | 30-60s |

**内存优化（v3.0）：**
- 启动时：~0.8GB（无OCR引擎）
- 处理中：~1.5GB（临时加载）
- 处理后：~0.8GB（自动释放） ✅

---

## 🔒 安全建议

- 仅允许特定用户使用：`ALLOWED_USER_ID`
- 定期备份数据库：`cp image_index.db image_index.db.backup`
- 定期清理日志：`bot.log`（日志会自动轮转）

---

## 📝 更新日志

### v3.0 (2025-11-22)
- ✅ **命令优化**：所有命令首字母即可区分（/f, /o, /t, /u, /l）
- ✅ **内存优化**：OCR引擎懒加载模式，零内存泄漏
- ✅ **功能增强**：/tag 支持无消息ID的图片
- ✅ **新增命令**：/link 设置消息ID
- ✅ **快捷别名**：/find --com 作为 --comprehensive 的简写

### v2.1 (2025-11-16)
- ✅ 优化搜索模式（智能、全面、FTS5、模糊）
- ✅ 支持搜索结果数量控制
- ✅ 增强文本搜索性能

### v2.0 (2025-11)
- ✅ 图像索引性能优化（<1秒）
- ✅ 支持高并发上传
- ✅ 定时OCR任务增强

---

## 📄 许可证

MIT License

---

## 🎉 致谢

- [python-telegram-bot](https://github.com/python-telegram-bot/python-telegram-bot)
- [PaddleOCR](https://github.com/PaddlePaddle/PaddleOCR)
- [ImageHash](https://github.com/JohannesBuchner/imagehash)
- [jieba](https://github.com/fxsjy/jieba) - 中文分词
- [opencc-python](https://github.com/yichen0831/opencc-python) - 简繁转换

---

**版本**: v3.0 | **状态**: ✅ 生产就绪 | **最后更新**: 2025-11-22
