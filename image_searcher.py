import os
import sqlite3
import hashlib
from typing import List, Dict, Optional, Tuple
import time
import logging
import re
import gc
import threading
import subprocess
import platform

from PIL import Image
import imagehash
import jieba
import opencc

# 尝试导入配置
try:
    import config
    MAC_SHORTCUTS = getattr(config, 'MAC_SHORTCUTS', None)
    OCR_POST_FILTER_PATTERNS = getattr(config, 'OCR_POST_FILTER_PATTERNS', [])
except ImportError:
    MAC_SHORTCUTS = None
    OCR_POST_FILTER_PATTERNS = []

# 根据配置决定是否导入 PaddleOCR
# 只有在非 Mac 或未配置快捷指令时才需要 PaddleOCR
if not MAC_SHORTCUTS or platform.system() != 'Darwin':
    from paddleocr import PaddleOCR
else:
    PaddleOCR = None

class ImageSimilaritySearcher:
    """图像相似性搜索器 - Bot专用版"""

    def __init__(self, db_path: str):
        self.db_path = db_path
        # 使用线程锁保护数据库操作，避免并发问题
        self._db_lock = threading.RLock()
        # 每个线程使用独立的数据库连接
        self.conn = sqlite3.connect(self.db_path, check_same_thread=False, timeout=30.0) 
        self.logger = logging.getLogger(__name__)
        self._init_database()
        
        # OCR引擎采用懒加载模式，不在初始化时加载
        self.ocr_engine = None
        self.logger.info("ImageSimilaritySearcher initialized with lazy-loading OCR engine.")
        
        # 初始化中文分词器
        try:
            jieba.initialize()
            self.logger.info("Jieba Chinese tokenizer initialized.")
        except Exception as e:
            self.logger.warning(f"Failed to initialize Jieba: {e}. Chinese text segmentation may not work optimally.")
        
        # 初始化简繁转换器
        try:
            self.cc_s2t = opencc.OpenCC('s2t')  # 简体转繁体
            self.cc_t2s = opencc.OpenCC('t2s')  # 繁体转简体
            self.logger.info("OpenCC simplified-traditional Chinese converters initialized.")
        except Exception as e:
            self.logger.warning(f"Failed to initialize OpenCC: {e}. Simplified-traditional conversion may not work.")

    def _ensure_ocr_engine(self):
        """确保OCR引擎已加载（懒加载模式）"""
        # 如果使用 Mac 快捷指令，则不需要 PaddleOCR
        if self._use_mac_shortcuts():
            return
        
        if self.ocr_engine is None:
            try:
                self.logger.info("Loading OCR engine on demand...")
                if PaddleOCR is None:
                    raise RuntimeError("PaddleOCR is not available")
                self.ocr_engine = PaddleOCR()
                self.logger.info("OCR engine loaded successfully")
            except Exception as e:
                self.logger.error(f"Failed to load OCR engine: {e}")
                raise
    
    def _use_mac_shortcuts(self) -> bool:
        """判断是否使用 Mac 快捷指令进行 OCR"""
        return bool(MAC_SHORTCUTS) and platform.system() == 'Darwin'
    
    def _get_clipboard_content(self) -> str:
        """
        获取 Mac 剪切板内容
        
        Returns:
            剪切板内容字符串
        """
        try:
            result = subprocess.run(
                ['pbpaste'],
                capture_output=True,
                text=True,
                timeout=5
            )
            return result.stdout
        except Exception as e:
            self.logger.warning(f"Failed to get clipboard content: {e}")
            return ""
    
    def _post_process_ocr_text(self, text_lines: List[str]) -> List[str]:
        """
        OCR后处理：去重、过滤空行、只有数字的行、只有符号的行等
        
        Args:
            text_lines: 文本行列表
        
        Returns:
            处理后的文本行列表
        """
        if not text_lines:
            return []
        
        # 编译正则表达式
        compiled_patterns = [re.compile(pattern) for pattern in OCR_POST_FILTER_PATTERNS]
        
        seen = set()
        result = []
        
        for line in text_lines:
            # 去除首尾空白
            line = line.strip()
            
            # 跳过空行
            if not line:
                continue
            
            # 去重
            if line in seen:
                continue
            
            # 检查是否匹配任何过滤正则
            should_filter = False
            for pattern in compiled_patterns:
                if pattern.match(line):
                    should_filter = True
                    break
            
            if should_filter:
                continue
            
            seen.add(line)
            result.append(line)
        
        return result
    
    def _extract_text_mac_shortcuts(self, image_path: str, timeout: int = 30) -> str:
        """
        使用 Mac 快捷指令进行 OCR 识别
        
        Args:
            image_path: 图片路径
            timeout: 超时时间（秒）
        
        Returns:
            识别结果文本
        """
        if not os.path.exists(image_path):
            self.logger.error(f"Image file not found: {image_path}")
            return ""
        
        # 获取绝对路径
        abs_path = os.path.abspath(image_path)
        
        # 获取执行前的剪切板内容
        old_clipboard = self._get_clipboard_content()
        
        try:
            # 调用快捷指令
            self.logger.info(f"Running Mac shortcut '{MAC_SHORTCUTS}' for {abs_path}")
            subprocess.run(
                ['shortcuts', 'run', MAC_SHORTCUTS, '-i', abs_path],
                capture_output=True,
                text=True,
                timeout=timeout
            )
            
            # 监听剪切板变化
            start_time = time.time()
            poll_interval = 0.3  # 轮询间隔
            
            while time.time() - start_time < timeout:
                current_clipboard = self._get_clipboard_content()
                if current_clipboard != old_clipboard:
                    # 剪切板内容变化，说明 OCR 完成
                    text_lines = current_clipboard.split('\n')
                    processed_lines = self._post_process_ocr_text(text_lines)
                    result_text = ' '.join(processed_lines)
                    # 清理文本
                    cleaned_text = self._clean_text(result_text)
                    self.logger.info(f"Mac shortcuts OCR completed for {abs_path}: {len(cleaned_text)} chars")
                    return cleaned_text
                time.sleep(poll_interval)
            
            self.logger.warning(f"Mac shortcuts OCR timeout for {abs_path}")
            return ""
            
        except subprocess.TimeoutExpired:
            self.logger.error(f"Mac shortcut execution timeout ({timeout}s) for {abs_path}")
            return ""
        except FileNotFoundError:
            self.logger.error("'shortcuts' command not found. Make sure running on macOS.")
            return ""
        except Exception as e:
            self.logger.error(f"Mac shortcuts OCR failed for {abs_path}: {e}")
            return ""

    def _clean_text(self, text: str) -> str:
        """
        清理和规范化文本，移除噪声字符。
        用于OCR结果的标准化处理，提高搜索准确性。
        """
        if not text or not isinstance(text, str):
            return ""
        
        # 1. 先移除换行符和制表符，替换为空格
        text = re.sub(r'[\r\n\t]+', ' ', text)
        
        # 2. 移除多余的空白字符
        text = re.sub(r'\s+', ' ', text.strip())
        
        # 3. 移除常见的OCR噪声字符
        # 移除特殊符号、标点（保留中英文字符、数字）
        text = re.sub(r'[^\u4e00-\u9fff\w\s]', '', text)
        
        # 4. 再次清理空格
        text = re.sub(r'\s+', ' ', text.strip())
        
        return text
    
    def _normalize_query_text(self, text: str) -> str:
        """
        规范化查询文本，确保与数据库中的清理后文本格式一致。
        """
        return self._clean_text(text)

    def _init_database(self):
        """初始化数据库"""
        cursor = self.conn.cursor()
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS image_features (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                file_path TEXT UNIQUE NOT NULL,
                file_hash TEXT,
                phash TEXT,
                ocr_text TEXT,
                telegram_message_id TEXT,
                updated_time REAL,
                ocr_status TEXT DEFAULT 'pending',
                ocr_fail_count INTEGER DEFAULT 0
            )
        ''')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_file_hash ON image_features(file_hash)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_phash ON image_features(phash)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_ocr_status ON image_features(ocr_status)')
        
        # FTS5 for full-text search on OCR text
        cursor.execute('CREATE VIRTUAL TABLE IF NOT EXISTS image_text_search USING fts5(file_path, ocr_text, content="image_features", content_rowid="id")')
        
        # Triggers to keep FTS table synchronized with image_features table
        cursor.executescript('''
            CREATE TRIGGER IF NOT EXISTS image_features_ai AFTER INSERT ON image_features BEGIN
              INSERT INTO image_text_search(rowid, file_path, ocr_text) 
              VALUES (new.id, new.file_path, new.ocr_text);
            END;
            CREATE TRIGGER IF NOT EXISTS image_features_ad AFTER DELETE ON image_features BEGIN
              INSERT INTO image_text_search(image_text_search, rowid, file_path, ocr_text) 
              VALUES('delete', old.id, old.file_path, old.ocr_text);
            END;
            CREATE TRIGGER IF NOT EXISTS image_features_au AFTER UPDATE ON image_features BEGIN
              INSERT INTO image_text_search(image_text_search, rowid, file_path, ocr_text) 
              VALUES('delete', old.id, old.file_path, old.ocr_text);
              INSERT INTO image_text_search(rowid, file_path, ocr_text) 
              VALUES (new.id, new.file_path, new.ocr_text);
            END;
        ''')
        self.conn.commit()
        self.logger.info("Database initialized successfully.")

    def _get_file_hash(self, file_path: str) -> str:
        """计算文件的MD5哈希值"""
        hash_md5 = hashlib.md5()
        try:
            with open(file_path, "rb") as f:
                for chunk in iter(lambda: f.read(4096), b""):
                    hash_md5.update(chunk)
            return hash_md5.hexdigest()
        except IOError as e:
            self.logger.error(f"Failed to read file {file_path} for hashing: {e}")
            raise

    def _extract_text_from_image(self, image_path: str) -> str:
        """从图片中提取文本，根据配置选择 Mac 快捷指令或 PaddleOCR"""
        # 检查是否使用 Mac 快捷指令
        if self._use_mac_shortcuts():
            return self._extract_text_mac_shortcuts(image_path)
        
        # 懒加载：在实际需要OCR时才加载引擎
        self._ensure_ocr_engine()
        
        if self.ocr_engine is None:
            self.logger.error(f"OCR engine failed to load")
            return ""
        
        result = None
        texts = []
        
        try:
            # PaddleOCR API - 直接调用，不使用任何特殊参数
            result = self.ocr_engine.ocr(image_path)
            
            if not result or not result[0]:
                return ""
            
            for line in result:
                # 处理新格式：result 是字典，包含 'rec_texts' 和 'rec_scores'
                if isinstance(line, dict):
                    rec_texts = line.get('rec_texts', [])
                    rec_scores = line.get('rec_scores', [])
                    
                    for text, score in zip(rec_texts, rec_scores):
                        # 确保 score 是浮点数，处理可能的字符串格式
                        try:
                            score_float = float(score) if isinstance(score, str) else score
                        except (ValueError, TypeError):
                            score_float = 0.0
                        
                        # 过滤掉空文本和低置信度的结果
                        if text and text.strip() and score_float > 0.6:
                            texts.append(text.strip())
                # 处理旧格式：result 是列表的列表
                elif isinstance(line, list):
                    for subline in line:
                        if isinstance(subline, list):
                            for word_info in subline:
                                # word_info 结构: ((x1, y1), (x2, y2), (x3, y3), (x4, y4)), (text, confidence)
                                if isinstance(word_info, (list, tuple)) and len(word_info) >= 2:
                                    text = word_info[1][0]
                                    score = word_info[1][1]
                                    if score > 0.6:
                                        texts.append(text)
            
            # 合并文本并清理
            raw_text = ' '.join(texts)
            cleaned_text = self._clean_text(raw_text)
            return cleaned_text
        except Exception as e:
            self.logger.error(f"OCR failed for {image_path}: {e}")
            return ""
        finally:
            # 显式释放OCR结果占用的内存
            del result
            del texts
            # 触发垃圾回收，清理OCR产生的临时对象
            gc.collect()

    def _tokenize_text(self, text: str) -> List[str]:
        """
        使用 jieba 对中文文本进行分词，同时保留英文单词。
        返回分词后的关键词列表，并去除长度过短的词汇。
        """
        try:
            if not text or not isinstance(text, str):
                return []
            
            # 使用 jieba 进行分词
            tokens = jieba.cut(text.strip())
            # 过滤：移除长度 < 2 的词汇，这些通常是无意义的字符
            keywords = [token for token in tokens if len(token.strip()) >= 2 and token.strip()]
            return keywords
        except Exception as e:
            self.logger.warning(f"Tokenization failed: {e}, returning empty list")
            return []

    def _get_all_variants(self, keywords: List[str]) -> List[str]:
        """
        获取关键词的所有变体（简体和繁体）。
        如果输入是简体，生成繁体版本；如果是繁体，生成简体版本。
        同时保留原文本，实现双向搜索。
        
        例：输入 ['搜索', '文本'] 会返回 ['搜索', '文本', '搜索', '文本']
        """
        if not hasattr(self, 'cc_s2t') or not hasattr(self, 'cc_t2s'):
            return keywords
        
        try:
            all_variants = []
            for keyword in keywords:
                all_variants.append(keyword)  # 保留原文本
                try:
                    # 转换为繁体，再转换回简体，如果有变化说明是简体，生成繁体
                    simplified = self.cc_t2s.convert(keyword)
                    traditional = self.cc_s2t.convert(keyword)
                    
                    # 添加转换后的版本（避免重复）
                    if traditional != keyword and traditional not in all_variants:
                        all_variants.append(traditional)
                    if simplified != keyword and simplified not in all_variants:
                        all_variants.append(simplified)
                except Exception as e:
                    self.logger.debug(f"Failed to convert '{keyword}': {e}")
            
            return all_variants
        except Exception as e:
            self.logger.warning(f"Failed to get variants: {e}, returning original keywords")
            return keywords

    def _extract_features(self, image_path: str, ocr_needed: bool = False) -> Optional[Dict]:
        """
        从图片中提取文件哈希、感知哈希和（可选）OCR文本。
        """
        img = None
        try:
            img = Image.open(image_path)
            features = {
                'file_hash': self._get_file_hash(image_path),
                'phash': str(imagehash.phash(img)),
                'ocr_text': ""
            }
            if ocr_needed:
                features['ocr_text'] = self._extract_text_from_image(image_path)
            return features
        except FileNotFoundError:
            self.logger.error(f"Image file not found for feature extraction: {image_path}")
            return None
        except Exception as e:
            self.logger.error(f"Feature extraction failed for {image_path}: {e}")
            return None
        finally:
            # 确保图像对象被正确关闭，释放内存
            if img is not None:
                try:
                    img.close()
                except Exception as e:
                    self.logger.debug(f"Failed to close image: {e}")

    def add_image_to_index(self, file_path: str, telegram_message_id: str) -> bool:
        """
        添加单个文件的索引，包含Telegram消息ID。
        仅计算哈希和感知哈希，OCR标记为pending状态，留待后续定时处理。
        成功返回True，失败返回False。
        """
        features = self._extract_features(file_path, ocr_needed=False)
        if not features:
            return False
        
        with self._db_lock:
            cursor = self.conn.cursor()
            try:
                cursor.execute(
                    "INSERT OR REPLACE INTO image_features (file_path, file_hash, phash, ocr_text, telegram_message_id, updated_time, ocr_status, ocr_fail_count) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                    (file_path, features['file_hash'], features['phash'], "", telegram_message_id, time.time(), 'pending', 0)
                )
                self.conn.commit()
                self.logger.info(f"Indexed image: {file_path} with telegram_message_id: '{telegram_message_id}'. OCR status: pending")
                return True
            except sqlite3.IntegrityError as e:
                self.logger.error(f"Integrity error when adding image {file_path}: {e}")
                self.conn.rollback()
                return False
            except Exception as e:
                self.logger.error(f"Failed to add image {file_path} to index: {e}")
                self.conn.rollback()
                return False

    def process_ocr_pending_images(self, batch_size: int = 10, max_retries: int = 3) -> Dict[str, int]:
        """
        处理所有OCR状态为pending或failed的图片。
        batch_size: 单次处理的最大图片数量
        max_retries: 最大重试次数（超过此次数的失败图片将被跳过）
        返回处理统计信息：{'processed': 5, 'succeeded': 4, 'failed': 1, 'skipped': 0}
        """
        stats = {'processed': 0, 'succeeded': 0, 'failed': 0, 'skipped': 0}
        
        try:
            # 获取待处理的图片（使用锁保护）
            with self._db_lock:
                cursor = self.conn.cursor()
                cursor.execute('''
                    SELECT id, file_path FROM image_features 
                    WHERE (ocr_status = 'pending' OR (ocr_status = 'failed' AND ocr_fail_count < ?))
                    LIMIT ?
                ''', (max_retries, batch_size))
                
                pending_images = cursor.fetchall()
                cursor.close()
            
            if not pending_images:
                self.logger.info("No pending images for OCR processing.")
                return stats
            
            self.logger.info(f"Processing {len(pending_images)} images for OCR...")
            
            # 如果不使用 Mac 快捷指令，在处理前确保OCR引擎已加载
            if not self._use_mac_shortcuts():
                self._ensure_ocr_engine()
            
            for img_id, file_path in pending_images:
                if not os.path.exists(file_path):
                    self.logger.warning(f"Image file not found: {file_path}. Marking as skipped.")
                    self._mark_ocr_skipped(img_id)
                    stats['skipped'] += 1
                    continue
                
                stats['processed'] += 1
                try:
                    # 检查文件大小
                    file_size = os.path.getsize(file_path)
                    if file_size == 0:
                        self.logger.warning(f"Image file is empty: {file_path}. Marking as skipped.")
                        self._mark_ocr_skipped(img_id)
                        stats['skipped'] += 1
                        continue
                    
                    self.logger.debug(f"Processing OCR for {file_path} (id: {img_id})...")
                    ocr_text = self._extract_text_from_image(file_path)
                    self._update_ocr_result(img_id, ocr_text, 'completed', 0)
                    stats['succeeded'] += 1
                    self.logger.info(f"Successfully processed OCR for {file_path}")
                except Exception as e:
                    self.logger.error(f"OCR failed for {file_path}: {e}", exc_info=True)
                    self._increment_ocr_fail_count(img_id)
                    stats['failed'] += 1
            
            return stats
            
        except Exception as e:
            self.logger.error(f"Error during batch OCR processing: {e}", exc_info=True)
            return stats
        finally:
            # 处理完成后立即清理OCR资源，释放内存
            self.cleanup_ocr_resources()
            # 每批处理完成后显式触发垃圾回收
            gc.collect()

    def _update_ocr_result(self, img_id: int, ocr_text: str, status: str, fail_count: int):
        """更新OCR结果"""
        with self._db_lock:
            cursor = self.conn.cursor()
            try:
                cursor.execute(
                    "UPDATE image_features SET ocr_text = ?, ocr_status = ?, ocr_fail_count = ? WHERE id = ?",
                    (ocr_text, status, fail_count, img_id)
                )
                self.conn.commit()
            except Exception as e:
                self.logger.error(f"Failed to update OCR result for image id {img_id}: {e}")
                self.conn.rollback()

    def _increment_ocr_fail_count(self, img_id: int):
        """增加OCR失败次数"""
        with self._db_lock:
            cursor = self.conn.cursor()
            try:
                cursor.execute(
                    "UPDATE image_features SET ocr_status = 'failed', ocr_fail_count = ocr_fail_count + 1 WHERE id = ?",
                    (img_id,)
                )
                self.conn.commit()
            except Exception as e:
                self.logger.error(f"Failed to increment OCR fail count for image id {img_id}: {e}")
                self.conn.rollback()

    def _mark_ocr_skipped(self, img_id: int):
        """标记OCR为已跳过（文件不存在）"""
        with self._db_lock:
            cursor = self.conn.cursor()
            try:
                cursor.execute(
                    "UPDATE image_features SET ocr_status = 'skipped' WHERE id = ?",
                    (img_id,)
                )
                self.conn.commit()
            except Exception as e:
                self.logger.error(f"Failed to mark image as skipped for id {img_id}: {e}")
                self.conn.rollback()

    def get_pending_ocr_count(self) -> int:
        """获取待处理的OCR图片数量"""
        with self._db_lock:
            cursor = self.conn.cursor()
            try:
                cursor.execute("SELECT COUNT(*) FROM image_features WHERE ocr_status = 'pending'")
                count = cursor.fetchone()[0]
                return count
            except Exception as e:
                self.logger.error(f"Failed to get pending OCR count: {e}")
                return 0

    def set_manual_ocr_result(self, telegram_message_id: str, ocr_text: str) -> bool:
        """
        手动设置指定消息ID的图片的OCR结果。
        将OCR文本设置为指定内容，状态修改为'processed'，失败计数重置为0。
        
        Args:
            telegram_message_id: Telegram消息ID
            ocr_text: 要设置的OCR文本内容
            
        Returns:
            bool: 成功返回True，失败返回False
        """
        if not telegram_message_id or not isinstance(ocr_text, str):
            self.logger.warning("Invalid parameters for set_manual_ocr_result")
            return False
        
        with self._db_lock:
            cursor = self.conn.cursor()
            try:
                # 查找对应的图片记录
                cursor.execute(
                    "SELECT id, file_path FROM image_features WHERE telegram_message_id = ?",
                    (telegram_message_id,)
                )
                result = cursor.fetchone()
                
                if not result:
                    self.logger.warning(f"No image found with telegram_message_id: {telegram_message_id}")
                    return False
                
                img_id, file_path = result
                
                # 清理OCR文本
                cleaned_ocr_text = self._clean_text(ocr_text)
                
                # 更新OCR结果
                cursor.execute(
                    "UPDATE image_features SET ocr_text = ?, ocr_status = 'completed', ocr_fail_count = 0, updated_time = ? WHERE id = ?",
                    (cleaned_ocr_text, time.time(), img_id)
                )
                self.conn.commit()
                
                self.logger.info(f"Manually set OCR result for {file_path} (message_id: {telegram_message_id}): '{ocr_text}'")
                return True
                
            except Exception as e:
                self.logger.error(f"Failed to set manual OCR result for message_id {telegram_message_id}: {e}")
                self.conn.rollback()
                return False
    
    def set_manual_ocr_result_by_hash(self, file_hash: str, ocr_text: str) -> bool:
        """
        通过file_hash手动设置图片的OCR结果（支持没有message_id的图片）。
        将OCR文本设置为指定内容，状态修改为'completed'，失败计数重置为0。
        
        Args:
            file_hash: 文件哈希值
            ocr_text: 要设置的OCR文本内容
            
        Returns:
            bool: 成功返回True，失败返回False
        """
        if not file_hash or not isinstance(ocr_text, str):
            self.logger.warning("Invalid parameters for set_manual_ocr_result_by_hash")
            return False
        
        with self._db_lock:
            cursor = self.conn.cursor()
            try:
                # 查找对应的图片记录
                cursor.execute(
                    "SELECT id, file_path FROM image_features WHERE file_hash = ?",
                    (file_hash,)
                )
                result = cursor.fetchone()
                
                if not result:
                    self.logger.warning(f"No image found with file_hash: {file_hash}")
                    return False
                
                img_id, file_path = result
                
                # 清理OCR文本
                cleaned_ocr_text = self._clean_text(ocr_text)
                
                # 更新OCR结果
                cursor.execute(
                    "UPDATE image_features SET ocr_text = ?, ocr_status = 'completed', ocr_fail_count = 0, updated_time = ? WHERE id = ?",
                    (cleaned_ocr_text, time.time(), img_id)
                )
                self.conn.commit()
                
                self.logger.info(f"Manually set OCR result for {file_path} (file_hash: {file_hash}): '{ocr_text}'")
                return True
                
            except Exception as e:
                self.logger.error(f"Failed to set manual OCR result for file_hash {file_hash}: {e}")
                self.conn.rollback()
                return False
    
    def set_message_id_by_hash(self, file_hash: str, message_id: str) -> bool:
        """
        通过file_hash设置图片的Telegram消息ID（仅限不存在message_id的记录）。
        
        Args:
            file_hash: 文件哈希值
            message_id: Telegram消息ID
            
        Returns:
            bool: 成功返回True，失败返回False
        """
        if not file_hash or not message_id:
            self.logger.warning("Invalid parameters for set_message_id_by_hash")
            return False
        
        with self._db_lock:
            cursor = self.conn.cursor()
            try:
                # 查找对应的图片记录，确保没有message_id
                cursor.execute(
                    "SELECT id, file_path, telegram_message_id FROM image_features WHERE file_hash = ?",
                    (file_hash,)
                )
                result = cursor.fetchone()
                
                if not result:
                    self.logger.warning(f"No image found with file_hash: {file_hash}")
                    return False
                
                img_id, file_path, existing_message_id = result
                
                # 检查是否已有message_id
                if existing_message_id:
                    self.logger.warning(f"Image already has message_id: {existing_message_id}")
                    return False
                
                # 更新message_id
                cursor.execute(
                    "UPDATE image_features SET telegram_message_id = ?, updated_time = ? WHERE id = ?",
                    (message_id, time.time(), img_id)
                )
                self.conn.commit()
                
                self.logger.info(f"Set message_id for {file_path} (file_hash: {file_hash}): '{message_id}'")
                return True
                
            except Exception as e:
                self.logger.error(f"Failed to set message_id for file_hash {file_hash}: {e}")
                self.conn.rollback()
                return False

    def clear_ocr_result(self, telegram_message_id: str) -> bool:
        """
        清除指定消息ID的图片的OCR结果。
        将OCR文本清空，状态修改为'pending'，失败计数重置为0。
        
        Args:
            telegram_message_id: Telegram消息ID
            
        Returns:
            bool: 成功返回True，失败返回False
        """
        if not telegram_message_id:
            self.logger.warning("Invalid telegram_message_id for clear_ocr_result")
            return False
        
        with self._db_lock:
            cursor = self.conn.cursor()
            try:
                # 查找对应的图片记录
                cursor.execute(
                    "SELECT id, file_path FROM image_features WHERE telegram_message_id = ?",
                    (telegram_message_id,)
                )
                result = cursor.fetchone()
                
                if not result:
                    self.logger.warning(f"No image found with telegram_message_id: {telegram_message_id}")
                    return False
                
                img_id, file_path = result
                
                # 清除OCR结果并重置状态
                cursor.execute(
                    "UPDATE image_features SET ocr_text = '', ocr_status = 'pending', ocr_fail_count = 0, updated_time = ? WHERE id = ?",
                    (time.time(), img_id)
                )
                self.conn.commit()
                
                self.logger.info(f"Cleared OCR result for {file_path} (message_id: {telegram_message_id})")
                return True
                
            except Exception as e:
                self.logger.error(f"Failed to clear OCR result for message_id {telegram_message_id}: {e}")
                self.conn.rollback()
                return False

    def get_ocr_by_message_id(self, telegram_message_id: str) -> Optional[str]:
        """
        通过消息ID获取图片的OCR结果。
        
        Args:
            telegram_message_id: Telegram消息ID
            
        Returns:
            Optional[str]: OCR文本，如果没有结果或未找到则返回None
        """
        if not telegram_message_id:
            self.logger.warning("Invalid telegram_message_id for get_ocr_by_message_id")
            return None
        
        with self._db_lock:
            cursor = self.conn.cursor()
            try:
                cursor.execute(
                    "SELECT ocr_text FROM image_features WHERE telegram_message_id = ?",
                    (telegram_message_id,)
                )
                result = cursor.fetchone()
                
                if not result:
                    self.logger.warning(f"No image found with telegram_message_id: {telegram_message_id}")
                    return None
                
                ocr_text = result[0]
                return ocr_text if ocr_text else None
                
            except Exception as e:
                self.logger.error(f"Failed to get OCR result for message_id {telegram_message_id}: {e}")
                return None

    def _hamming_distance(self, hash1: str, hash2: str) -> int:
        """计算两个哈希字符串之间的汉明距离"""
        # Ensure hashes are of the same length, phash is typically 64-bit (16 hex chars)
        if len(hash1) != len(hash2):
            self.logger.warning(f"Comparing hashes of different lengths: {hash1} ({len(hash1)}) vs {hash2} ({len(hash2)})")
            return 64
        return sum(c1 != c2 for c1, c2 in zip(hash1, hash2))

    def search_similar_images(self, query_image_path: str, threshold: int = 5, max_results: int = 3) -> List[Dict]:
        """
        搜索相似图像，返回包含文件路径和相关信息的字典列表。
        threshold: 感知哈希的最大汉明距离，低于此距离视为相似。
        max_results: 返回的最大结果数量。
        """
        query_features = self._extract_features(query_image_path, ocr_needed=False)
        if not query_features:
            self.logger.warning(f"Could not extract features from query image: {query_image_path}")
            return []
        
        with self._db_lock:
            cursor = self.conn.cursor()
            
            try:
                # 1. Check for exact file hash match first (most performant check)
                cursor.execute('SELECT file_path, telegram_message_id, file_hash, updated_time, ocr_text FROM image_features WHERE file_hash = ?', (query_features['file_hash'],))
                exact_match = cursor.fetchone()
                if exact_match:
                    self.logger.info(f"Exact match found for {query_image_path}: {exact_match[0]}")
                    return [{
                        'path': exact_match[0],
                        'telegram_message_id': exact_match[1],
                        'file_hash': exact_match[2],
                        'updated_time': exact_match[3],
                        'ocr_text': exact_match[4],
                        'similarity': 1.0
                    }]

                # 2. If no exact file hash match, search for similar phash matches
                cursor.execute('SELECT file_path, phash, telegram_message_id, file_hash, updated_time, ocr_text FROM image_features WHERE phash IS NOT NULL')
                results = []
                for file_path, phash, msg_id, file_hash, updated_time, ocr_text in cursor.fetchall():
                    if not phash:
                        continue
                    distance = self._hamming_distance(query_features['phash'], phash)
                    if distance <= threshold:
                        similarity = 1.0 - (distance / 64.0)
                        results.append({
                            'path': file_path,
                            'telegram_message_id': msg_id,
                            'file_hash': file_hash,
                            'updated_time': updated_time,
                            'ocr_text': ocr_text,
                            'similarity': similarity
                        })
                
                results.sort(key=lambda x: x['similarity'], reverse=True)
                self.logger.info(f"Found {len(results)} similar images for {query_image_path} (threshold={threshold}).")
                return results[:max_results]
            
            except Exception as e:
                self.logger.error(f"Error during similarity search: {e}")
                return []

    def search_by_text(self, keywords: str, max_results: int = 3, search_mode: str = 'smart') -> List[Dict]:
        """
        根据关键字搜索，支持模糊匹配、分词和简繁互转。
        使用 jieba 对查询关键字进行分词，支持简体和繁体互查。
        返回包含文件路径和消息ID的字典列表。
        
        Args:
            keywords: 搜索关键词
            max_results: 最大返回结果数
            search_mode: 搜索模式
                - 'smart': 智能搜索，优先FTS5，回退到LIKE
                - 'comprehensive': 全面搜索，FTS5 + LIKE 结果合并去重
                - 'fts': 仅使用FTS5搜索
                - 'like': 仅使用LIKE搜索
        """
        with self._db_lock:
            cursor = self.conn.cursor()
            try:
                # 清理和规范化查询文本
                cleaned_keywords = self._normalize_query_text(keywords)
                if not cleaned_keywords:
                    self.logger.warning(f"Query keywords empty after cleaning: '{keywords}'")
                    return []
                
                # 对清理后的关键字进行分词
                query_tokens = self._tokenize_text(cleaned_keywords)
                
                if search_mode == 'comprehensive':
                    # 全面搜索模式：合并FTS5和LIKE结果
                    return self._comprehensive_search(cursor, query_tokens, cleaned_keywords, max_results)
                elif search_mode == 'fts':
                    # 仅FTS5搜索
                    return self._fts_search_only(cursor, query_tokens, max_results)
                elif search_mode == 'like':
                    # 仅LIKE搜索
                    return self._like_search_only(cursor, query_tokens, cleaned_keywords, max_results)
                else:
                    # 默认智能搜索模式
                    return self._smart_search(cursor, query_tokens, cleaned_keywords, max_results)
                    
            except Exception as e:
                self.logger.error(f"Error during text search: {e}")
                return []
    
    def _comprehensive_search(self, cursor, query_tokens: List[str], cleaned_keywords: str, max_results: int) -> List[Dict]:
        """全面搜索：合并FTS5和LIKE结果"""
        results_map = {}  # 使用字典去重，key为file_path
        
        # 1. 先尝试FTS5搜索
        fts_results = self._fts_search_only(cursor, query_tokens, max_results * 2)  # 获取更多FTS结果
        for result in fts_results:
            results_map[result['path']] = result
        
        # 2. 如果FTS5结果不足，补充LIKE搜索
        if len(results_map) < max_results:
            like_results = self._like_search_only(cursor, query_tokens, cleaned_keywords, max_results * 2)
            for result in like_results:
                if result['path'] not in results_map:
                    results_map[result['path']] = result
        
        # 3. 返回结果，按更新时间排序
        final_results = list(results_map.values())
        final_results.sort(key=lambda x: x.get('updated_time', 0), reverse=True)
        
        self.logger.info(f"Comprehensive search for '{cleaned_keywords}' found {len(final_results)} unique results")
        return final_results[:max_results]
    
    def _fts_search_only(self, cursor, query_tokens: List[str], max_results: int) -> List[Dict]:
        """仅使用FTS5搜索"""
        if not query_tokens:
            return []
        
        # 获取所有变体（简体和繁体）
        all_variants = self._get_all_variants(query_tokens)
        
        # 构建 FTS5 查询：使用 OR 逻辑（任何一个词匹配即可）
        fts_query = ' OR '.join([f'"{variant}"' for variant in all_variants])  # 使用短语查询提高准确性
        
        try:
            cursor.execute('''
                SELECT f.file_path, f.telegram_message_id, f.updated_time, bm25(image_text_search) as score
                FROM image_text_search
                JOIN image_features f ON image_text_search.rowid = f.id
                WHERE image_text_search MATCH ? ORDER BY score DESC LIMIT ?
            ''', (fts_query, max_results))
            
            results = [{
                'path': row[0], 
                'telegram_message_id': row[1],
                'updated_time': row[2],
                'search_method': 'FTS5',
                'score': row[3]
            } for row in cursor.fetchall()]
            
            if results:
                self.logger.info(f"FTS5 search found {len(results)} results")
                return results
                
        except sqlite3.OperationalError as e:
            self.logger.debug(f"FTS5 search failed: {e}")
        
        return []
    
    def _like_search_only(self, cursor, query_tokens: List[str], cleaned_keywords: str, max_results: int) -> List[Dict]:
        """仅使用LIKE搜索"""
        if query_tokens:
            # 使用分词结果
            all_variants = self._get_all_variants(query_tokens)
        else:
            # 回退到原始关键字
            all_variants = [cleaned_keywords]
        
        # 构建 LIKE 查询：ANY 条件满足就返回
        where_clauses = [f"ocr_text LIKE ?" for _ in all_variants]
        where_sql = ' OR '.join(where_clauses)
        query_params = [f"%{variant}%" for variant in all_variants] + [max_results]
        
        cursor.execute(f'''
            SELECT file_path, telegram_message_id, updated_time FROM image_features 
            WHERE {where_sql}
            ORDER BY updated_time DESC LIMIT ?
        ''', query_params)
        
        results = [{
            'path': row[0], 
            'telegram_message_id': row[1],
            'updated_time': row[2],
            'search_method': 'LIKE'
        } for row in cursor.fetchall()]
        
        self.logger.info(f"LIKE search found {len(results)} results")
        return results
    
    def _smart_search(self, cursor, query_tokens: List[str], cleaned_keywords: str, max_results: int) -> List[Dict]:
        """智能搜索：优先FTS5，不足时回退到LIKE"""
        # 先尝试FTS5
        results = self._fts_search_only(cursor, query_tokens, max_results)
        
        if results:
            return results
        
        # FTS5无结果时回退到LIKE
        self.logger.info("FTS5 search returned no results, falling back to LIKE search")
        return self._like_search_only(cursor, query_tokens, cleaned_keywords, max_results)

    def update_archived_file_paths(self, path_mappings: List[Tuple[str, str]]):
        """
        批量更新数据库中图片的file_path。
        path_mappings: 列表，每个元素为 (old_path, new_path)
        """
        if not path_mappings:
            return

        with self._db_lock:
            cursor = self.conn.cursor()
            try:
                # Use executemany for efficient bulk updates
                cursor.executemany(
                    "UPDATE image_features SET file_path = ? WHERE file_path = ?",
                    [(new_path, old_path) for old_path, new_path in path_mappings]
                )
                self.conn.commit()
                self.logger.info(f"Successfully updated {len(path_mappings)} file paths in database after archiving.")
            except Exception as e:
                self.logger.error(f"Failed to update file paths in database during archiving: {e}")
                self.conn.rollback()

    def clean_all_ocr_texts(self) -> Dict[str, int]:
        """
        批量清理数据库中所有已完成OCR的文本，移除噪声字符。
        返回处理统计信息。
        """
        with self._db_lock:
            cursor = self.conn.cursor()
            stats = {'total': 0, 'cleaned': 0, 'unchanged': 0, 'errors': 0}
            
            try:
                # 获取所有已完成OCR的记录
                cursor.execute(
                    "SELECT id, file_path, ocr_text FROM image_features WHERE ocr_status = 'completed' AND ocr_text IS NOT NULL AND ocr_text != ''"
                )
                records = cursor.fetchall()
                stats['total'] = len(records)
                
                self.logger.info(f"Starting OCR text cleaning for {stats['total']} records...")
                
                for img_id, file_path, original_text in records:
                    try:
                        if not original_text:
                            stats['unchanged'] += 1
                            continue
                        
                        cleaned_text = self._clean_text(original_text)
                        
                        if cleaned_text != original_text:
                            # 更新清理后的文本
                            cursor.execute(
                                "UPDATE image_features SET ocr_text = ?, updated_time = ? WHERE id = ?",
                                (cleaned_text, time.time(), img_id)
                            )
                            stats['cleaned'] += 1
                            self.logger.debug(f"Cleaned OCR text for {file_path}: '{original_text}' -> '{cleaned_text}'")
                        else:
                            stats['unchanged'] += 1
                            
                    except Exception as e:
                        self.logger.error(f"Error cleaning OCR text for image id {img_id}: {e}")
                        stats['errors'] += 1
                
                # 提交所有更改
                self.conn.commit()
                
                self.logger.info(f"OCR text cleaning completed: {stats}")
                return stats
                
            except Exception as e:
                self.logger.error(f"Error during batch OCR text cleaning: {e}")
                self.conn.rollback()
                stats['errors'] = stats['total']
                return stats

    def cleanup_ocr_resources(self):
        """清理OCR引擎占用的内存资源"""
        # 如果使用 Mac 快捷指令，不需要清理 PaddleOCR 资源
        if self._use_mac_shortcuts():
            return
        
        try:
            if hasattr(self, 'ocr_engine') and self.ocr_engine is not None:
                # 尝试清理PaddleOCR的内部资源
                if hasattr(self.ocr_engine, 'text_detector'):
                    del self.ocr_engine.text_detector
                if hasattr(self.ocr_engine, 'text_recognizer'):
                    del self.ocr_engine.text_recognizer
                if hasattr(self.ocr_engine, 'text_classifier'):
                    del self.ocr_engine.text_classifier
                
                # 清理主引擎对象
                del self.ocr_engine
                self.ocr_engine = None
                
                # 触发垃圾回收
                gc.collect()
                self.logger.info("OCR resources cleaned up successfully")
        except Exception as e:
            self.logger.warning(f"Failed to cleanup OCR resources: {e}")
    
    def reinitialize_ocr(self):
        """重新初始化OCR引擎（在清理后使用）- 懒加载模式下不需要主动调用"""
        # 懒加载模式下，OCR引擎会在下次需要时自动加载
        # 这个方法保留是为了兼容性，实际上什么都不做
        self.logger.info("OCR engine will be lazy-loaded when needed")
        pass

    def close(self):
        """关闭数据库连接并清理资源"""
        # 清理OCR资源
        self.cleanup_ocr_resources()
        
        # 关闭数据库连接
        if self.conn:
            self.conn.close()
            self.logger.info("Database connection closed.")
