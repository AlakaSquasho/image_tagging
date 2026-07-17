# Bot 命令参考

## 命令列表

- `/find <关键词>`：按 OCR 文本搜索图片
- `回复图片 /find`：以图搜图
- `/r [数量]`：随机发送图片
- `/ocr`：处理待 OCR 图片
- `回复图片 /ocr`：如果该图片处于 pending 或 failed 状态，仅对这张图片执行一次 OCR
- `回复图片 /tag <文本>`：手动设置 OCR 文本
- `回复图片 /untag`：清除 OCR 文本并重置为待处理
- `回复图片 /link <消息ID或链接>`：为没有消息 ID 的图片补充消息 ID
- `回复图片 /getocr`：查看图片当前 OCR 文本
- `/failed`：查看 OCR 失败记录
- `/help`：以 Markdown 形式输出当前语言对应的命令说明（中文优先 `COMMANDS_zh.md`，否则回退 `COMMANDS.md`）
- `/language <zh|en>` 或 `/lang <zh|en>`：在中文和英文之间切换 bot 输出语言

## `/find`

文本搜索：

```text
/find 关键词
/find --comprehensive 关键词
/find --com 关键词
/find --contains 关键词
/find -5 关键词
/find -n=5 关键词
/find --max=10 关键词
```

说明：

- 默认模式：精确匹配
- `--comprehensive` / `--com`：完整关键词 + 分词搜索
- `--contains`：按包含关系搜索 OCR 文本
- `-5`、`-n=5`、`--max=10`：限制返回数量

图片搜索：

```text
[回复图片] /find
```

## `/r`

```text
/r
/r 10
```

- 不传数量时使用 `config.py` 中的默认值
- 结果过多时会分页显示

## `/ocr`

```text
/ocr
[回复图片] /ocr
```

- 不回复图片时：处理所有待 OCR 和可重试的失败图片
- 回复图片时：如果该图片处于 pending 或 failed 状态，仅对这张图片执行一次 OCR；已完成 OCR 的图片会跳过

## `/tag`

```text
[回复图片] /tag OCR文本内容
```

- 手动设置图片 OCR 文本

## `/untag`

```text
[回复图片] /untag
```

- 清除 OCR 文本
- 将状态重置为待处理

## `/link`

```text
[回复图片] /link 消息ID或链接
```

- 仅适用于当前没有消息 ID 的图片

## `/getocr`

```text
[回复图片] /getocr
```

- 查看图片当前保存的 OCR 文本

## `/failed`

```text
/failed
/failed -5
/failed -a
/failed -all
```

- 查看 OCR 失败记录
- `-a` / `-all` 表示显示全部

## `/help`

```text
/help
```

- 以 Markdown 形式发送命令说明
- 当前 bot 语言为中文（`zh`）时使用 `COMMANDS_zh.md`
- 其他语言或没有对应本地化文件时回退到 `COMMANDS.md`

## `/language` 和 `/lang`

```text
/language
/language zh
/language en
/lang
/lang zh
/lang en
```

- 不带参数时显示当前 bot 输出语言
- 目前支持 `zh` 和 `en`
- `/lang` 是 `/language` 的简写
