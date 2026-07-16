# Bot Command Reference

[中文说明 / Chinese Version](./COMMANDS_zh.md)

## Command List

- `/find <keyword>`: search images by OCR text
- `reply to an image with /find`: search by image
- `/r [count]`: send random images
- `/ocr`: process pending OCR images
- `reply to an image with /tag <text>`: set OCR text manually
- `reply to an image with /untag`: clear OCR text and reset it to pending
- `reply to an image with /link <message id or link>`: attach a message ID to an image without one
- `reply to an image with /getocr`: view the current OCR text of an image
- `/failed`: view failed OCR records
- `/help`: output the localized command reference in Markdown (`COMMANDS_zh.md` for Chinese, otherwise `COMMANDS.md`)
- `/language <zh|en>` or `/lang <zh|en>`: switch the bot output language between Chinese and English

## `/find`

Text search:

```text
/find keyword
/find --comprehensive keyword
/find --com keyword
/find --contains keyword
/find -5 keyword
/find -n=5 keyword
/find --max=10 keyword
```

Notes:

- Default mode: exact match
- `--comprehensive` / `--com`: full keyword + tokenized search
- `--contains`: search OCR text by substring match
- `-5`, `-n=5`, `--max=10`: limit the number of results

Image search:

```text
[reply to an image] /find
```

## `/r`

```text
/r
/r 10
```

- Uses the default count from `config.py` when no count is provided
- Results may be paginated when there are too many images

## `/ocr`

```text
/ocr
```

- Processes all pending OCR images and retryable failed images

## `/tag`

```text
[reply to an image] /tag OCR text content
```

- Manually sets the OCR text for an image

## `/untag`

```text
[reply to an image] /untag
```

- Clears OCR text
- Resets the image status to pending

## `/link`

```text
[reply to an image] /link message_id_or_link
```

- Only works for images that do not already have a message ID

## `/getocr`

```text
[reply to an image] /getocr
```

- Shows the currently stored OCR text for the image

## `/failed`

```text
/failed
/failed -5
/failed -a
/failed -all
```

- Shows failed OCR records
- `-a` / `-all` shows all records

## `/help`

```text
/help
```

- Sends the command reference as Markdown
- Uses `COMMANDS_zh.md` when the current bot language is Chinese (`zh`)
- Falls back to `COMMANDS.md` for other languages or when no localized file exists

## `/language` and `/lang`

```text
/language
/language zh
/language en
/lang
/lang zh
/lang en
```

- Shows the current bot output language when used without arguments
- Supports `zh` and `en`
- `/lang` is a short alias for `/language`
