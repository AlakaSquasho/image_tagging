import os
import sqlite3
import hashlib
from typing import List, Dict, Optional, Tuple
import time
import logging

from PIL import Image
import imagehash
import jieba

from paddleocr import PaddleOCR

class ImageSimilaritySearcher:
    """图像相似性搜索器 - Bot专用版"""

    def __init__(self, db_path: str):
        self.db_path = db_path
        # Allow multiple threads to access the same connection, for a bot this is often needed
        # but ensure proper synchronization if concurrent writes are possible.
        self.conn = sqlite3.connect(self.db_path, check_same_thread=False) 
        self.logger = logging.getLogger(__name__)
        self._init_database()
        
        # Initialize PaddleOCR once to avoid repeated loading overhead
        try:
            # 使用默认参数初始化，兼容所有版本
            self.ocr_engine = PaddleOCR()
            self.logger.info("ImageSimilaritySearcher initialized with PaddleOCR.")
        except Exception as e:
            self.logger.error(f"Failed to initialize PaddleOCR: {e}")
            self.ocr_engine = None
        
        # 初始化中文分词器
        try:
            jieba.initialize()
            self.logger.info("Jieba Chinese tokenizer initialized.")
        except Exception as e:
            self.logger.warning(f"Failed to initialize Jieba: {e}. Chinese text segmentation may not work optimally.")

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
        """使用PaddleOCR从图片中提取文本"""
        if self.ocr_engine is None:
            self.logger.error(f"OCR engine not initialized")
            return ""
        
        try:
            # PaddleOCR API - 直接调用，不使用任何特殊参数
            result = self.ocr_engine.ocr(image_path)
            
            if not result or not result[0]:
                return ""
            
            texts = []
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
            
            return ' '.join(texts)
        except Exception as e:
            self.logger.error(f"OCR failed for {image_path}: {e}")
            return ""

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

    def _extract_features(self, image_path: str, ocr_needed: bool = False) -> Optional[Dict]:
        """
        从图片中提取文件哈希、感知哈希和（可选）OCR文本。
        """
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

    def add_image_to_index(self, file_path: str, telegram_message_id: str) -> bool:
        """
        添加单个文件的索引，包含Telegram消息ID。
        仅计算哈希和感知哈希，OCR标记为pending状态，留待后续定时处理。
        成功返回True，失败返回False。
        """
        features = self._extract_features(file_path, ocr_needed=False)
        if not features:
            return False
        
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
        cursor = self.conn.cursor()
        stats = {'processed': 0, 'succeeded': 0, 'failed': 0, 'skipped': 0}
        
        try:
            # 获取待处理的图片（pending状态或失败次数<max_retries的failed状态）
            cursor.execute('''
                SELECT id, file_path FROM image_features 
                WHERE (ocr_status = 'pending' OR (ocr_status = 'failed' AND ocr_fail_count < ?))
                LIMIT ?
            ''', (max_retries, batch_size))
            
            pending_images = cursor.fetchall()
            
            if not pending_images:
                self.logger.info("No pending images for OCR processing.")
                return stats
            
            self.logger.info(f"Processing {len(pending_images)} images for OCR...")
            
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

    def _update_ocr_result(self, img_id: int, ocr_text: str, status: str, fail_count: int):
        """更新OCR结果"""
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
        cursor = self.conn.cursor()
        try:
            cursor.execute("SELECT COUNT(*) FROM image_features WHERE ocr_status = 'pending'")
            count = cursor.fetchone()[0]
            return count
        except Exception as e:
            self.logger.error(f"Failed to get pending OCR count: {e}")
            return 0

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

    def search_by_text(self, keywords: str, max_results: int = 3) -> List[Dict]:
        """
        根据关键字搜索，支持模糊匹配和分词。
        使用 jieba 对查询关键字进行分词，提高中文搜索准确性。
        返回包含文件路径和消息ID的字典列表。
        """
        cursor = self.conn.cursor()
        try:
            # 对输入关键字进行分词
            query_tokens = self._tokenize_text(keywords)
            
            if query_tokens:
                # 构建 FTS5 查询：使用 OR 逻辑（任何一个词匹配即可）
                # 格式: "token1 OR token2 OR token3"
                fts_query = ' OR '.join(query_tokens)
                
                try:
                    # 尝试使用 FTS5 进行全文搜索
                    cursor.execute('''
                        SELECT f.file_path, f.telegram_message_id, bm25(image_text_search) as score
                        FROM image_text_search
                        JOIN image_features f ON image_text_search.rowid = f.id
                        WHERE image_text_search MATCH ? ORDER BY score DESC LIMIT ?
                    ''', (fts_query, max_results))
                    results = [{'path': row[0], 'telegram_message_id': row[1]} for row in cursor.fetchall()]
                    
                    if results:
                        self.logger.info(f"FTS5 search for '{keywords}' found {len(results)} results")
                        return results
                except sqlite3.OperationalError as e:
                    self.logger.debug(f"FTS5 search failed: {e}, falling back to LIKE search")
                
                # 回退方案：使用 LIKE 进行多关键词模糊匹配
                # 构建 LIKE 查询：ANY 条件满足就返回
                where_clauses = [f"ocr_text LIKE ?" for _ in query_tokens]
                where_sql = ' OR '.join(where_clauses)
                query_params = [f"%{token}%" for token in query_tokens] + [max_results]
                
                cursor.execute(f'''
                    SELECT file_path, telegram_message_id FROM image_features 
                    WHERE {where_sql}
                    ORDER BY updated_time DESC LIMIT ?
                ''', query_params)
                
                results = [{'path': row[0], 'telegram_message_id': row[1]} for row in cursor.fetchall()]
                self.logger.info(f"LIKE search for '{keywords}' found {len(results)} results")
                return results
            else:
                # 如果分词失败或关键字为空，使用原始关键字进行搜索
                self.logger.debug(f"No tokens extracted from '{keywords}', using raw keyword")
                cursor.execute('''
                    SELECT file_path, telegram_message_id FROM image_features 
                    WHERE ocr_text LIKE ? ORDER BY updated_time DESC LIMIT ?
                ''', (f'%{keywords}%', max_results))
                
                return [{'path': row[0], 'telegram_message_id': row[1]} for row in cursor.fetchall()]
        except Exception as e:
            self.logger.error(f"Error during text search: {e}")
            return []

    def update_archived_file_paths(self, path_mappings: List[Tuple[str, str]]):
        """
        批量更新数据库中图片的file_path。
        path_mappings: 列表，每个元素为 (old_path, new_path)
        """
        if not path_mappings:
            return

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

    def close(self):
        """关闭数据库连接"""
        if self.conn:
            self.conn.close()
            self.logger.info("Database connection closed.")
