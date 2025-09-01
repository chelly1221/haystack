import asyncio
import uuid
from typing import Dict, List, Optional, Set
from datetime import datetime, timedelta
import threading
import logging
from enum import Enum
import sqlite3
import json
import os
from contextlib import contextmanager

class TaskStatus(Enum):
    UPLOADING = "uploading"
    QUEUED = "queued"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"

class TaskManager:
    def __init__(self, db_path: str = "./data/tasks.db"):
        self.db_path = db_path
        self.processing_queue = asyncio.Queue()
        self.worker_task = None
        
        # Create data directory if not exists
        os.makedirs(os.path.dirname(self.db_path), exist_ok=True)
        
        # Initialize database
        self._init_db()
        
        # Start cleanup task
        asyncio.create_task(self._periodic_cleanup())
        
        # Start stale task checker
        asyncio.create_task(self._check_stale_tasks())
    
    def _init_db(self):
        """Initialize SQLite database"""
        with self._get_db() as conn:
            conn.execute('''
                CREATE TABLE IF NOT EXISTS tasks (
                    id TEXT PRIMARY KEY,
                    filename TEXT NOT NULL,
                    sosok TEXT NOT NULL,
                    site TEXT NOT NULL,
                    status TEXT NOT NULL,
                    progress INTEGER DEFAULT 0,
                    message TEXT,
                    created_at TIMESTAMP NOT NULL,
                    updated_at TIMESTAMP NOT NULL,
                    completed_at TIMESTAMP,
                    result TEXT,
                    error TEXT,
                    file_path TEXT,
                    total_pages INTEGER DEFAULT 0,
                    processed_pages INTEGER DEFAULT 0,
                    is_dismissed BOOLEAN DEFAULT 0
                )
            ''')
            
            # Create indexes for better performance
            conn.execute('CREATE INDEX IF NOT EXISTS idx_sosok_site ON tasks(sosok, site)')
            conn.execute('CREATE INDEX IF NOT EXISTS idx_created_at ON tasks(created_at)')
            conn.execute('CREATE INDEX IF NOT EXISTS idx_status ON tasks(status)')
            conn.execute('CREATE INDEX IF NOT EXISTS idx_dismissed ON tasks(is_dismissed)')
            conn.execute('CREATE INDEX IF NOT EXISTS idx_updated_at ON tasks(updated_at)')
            
            conn.commit()
    
    @contextmanager
    def _get_db(self):
        """Get database connection with proper handling"""
        conn = sqlite3.connect(self.db_path, timeout=30.0)
        conn.row_factory = sqlite3.Row
        try:
            yield conn
        finally:
            conn.close()
    
    def create_task(self, filename: str, sosok: str, site: str) -> str:
        """새 작업 생성"""
        task_id = str(uuid.uuid4())
        now = datetime.now()
        
        with self._get_db() as conn:
            conn.execute('''
                INSERT INTO tasks (
                    id, filename, sosok, site, status, progress, message,
                    created_at, updated_at, is_dismissed
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                task_id, filename, sosok, site, TaskStatus.UPLOADING.value,
                0, "파일 전송 중...", now, now, 0
            ))
            conn.commit()
        
        logging.info(f"📋 Created task {task_id} for {filename}")
        return task_id
    
    def update_task_status(self, task_id: str, status: TaskStatus, 
                          progress: int = None, message: str = None,
                          total_pages: int = None, processed_pages: int = None):
        """작업 상태 업데이트"""
        with self._get_db() as conn:
            # Build update query dynamically
            updates = ['status = ?', 'updated_at = ?']
            params = [status.value, datetime.now()]
            
            if progress is not None:
                updates.append('progress = ?')
                params.append(progress)
            
            if message is not None:
                updates.append('message = ?')
                params.append(message)
            
            if total_pages is not None:
                updates.append('total_pages = ?')
                params.append(total_pages)
            
            if processed_pages is not None:
                updates.append('processed_pages = ?')
                params.append(processed_pages)
            
            params.append(task_id)
            
            query = f"UPDATE tasks SET {', '.join(updates)} WHERE id = ?"
            conn.execute(query, params)
            conn.commit()
        
        logging.info(f"📊 Updated task {task_id}: {status.value} - {progress}%")
    
    def set_task_file_path(self, task_id: str, file_path: str):
        """작업에 파일 경로 설정"""
        with self._get_db() as conn:
            conn.execute(
                "UPDATE tasks SET file_path = ?, updated_at = ? WHERE id = ?",
                (file_path, datetime.now(), task_id)
            )
            conn.commit()
    
    def complete_task(self, task_id: str, result: dict):
        """작업 완료 처리"""
        now = datetime.now()
        with self._get_db() as conn:
            conn.execute('''
                UPDATE tasks 
                SET status = ?, progress = ?, message = ?, result = ?, 
                    updated_at = ?, completed_at = ?
                WHERE id = ?
            ''', (
                TaskStatus.COMPLETED.value, 100, "처리 완료",
                json.dumps(result), now, now, task_id
            ))
            conn.commit()
        
        logging.info(f"✅ Completed task {task_id}")
    
    def fail_task(self, task_id: str, error: str):
        """작업 실패 처리"""
        now = datetime.now()
        with self._get_db() as conn:
            conn.execute('''
                UPDATE tasks 
                SET status = ?, message = ?, error = ?, 
                    updated_at = ?, completed_at = ?
                WHERE id = ?
            ''', (
                TaskStatus.FAILED.value, "처리 실패", error,
                now, now, task_id
            ))
            conn.commit()
        
        logging.error(f"❌ Failed task {task_id}: {error}")
    
    def dismiss_task(self, task_id: str):
        """클라이언트에서 작업 제거 (UI에서만 숨김)"""
        with self._get_db() as conn:
            result = conn.execute(
                "UPDATE tasks SET is_dismissed = 1, updated_at = ? WHERE id = ?",
                (datetime.now(), task_id)
            )
            conn.commit()
            updated_count = result.rowcount
        
        logging.info(f"🗑️ Dismissed task {task_id} (updated: {updated_count})")
    
    def dismiss_completed_tasks(self, sosok: str, site: str):
        """특정 현장의 완료/실패 작업 모두 제거"""
        with self._get_db() as conn:
            # 로그를 위해 먼저 카운트
            cursor = conn.execute('''
                SELECT COUNT(*) as count FROM tasks 
                WHERE sosok = ? AND site = ? 
                AND status IN (?, ?)
                AND is_dismissed = 0
            ''', (
                sosok, site,
                TaskStatus.COMPLETED.value, TaskStatus.FAILED.value
            ))
            count_before = cursor.fetchone()['count']
            
            # 업데이트 실행
            result = conn.execute('''
                UPDATE tasks 
                SET is_dismissed = 1, updated_at = ?
                WHERE sosok = ? AND site = ? 
                AND status IN (?, ?)
                AND is_dismissed = 0
            ''', (
                datetime.now(), sosok, site,
                TaskStatus.COMPLETED.value, TaskStatus.FAILED.value
            ))
            conn.commit()
            dismissed_count = result.rowcount
        
        logging.info(f"🗑️ Dismissed {dismissed_count} completed tasks for {sosok}/{site} (found {count_before} before update)")
    
    def get_tasks_by_site(self, sosok: str, site: str) -> List[Dict]:
        """특정 현장의 작업 목록 조회 (제거된 작업 제외)"""
        cutoff_time = datetime.now() - timedelta(hours=24)
        
        # 먼저 오래된 진행 중인 작업을 정리
        self._cleanup_stale_tasks()
        
        with self._get_db() as conn:
            cursor = conn.execute('''
                SELECT * FROM tasks
                WHERE sosok = ? AND site = ?
                AND created_at > ?
                AND is_dismissed = 0
                ORDER BY created_at DESC
            ''', (sosok, site, cutoff_time))
            
            tasks = []
            for row in cursor:
                task_dict = dict(row)
                
                # Parse JSON fields
                if task_dict.get('result'):
                    try:
                        task_dict['result'] = json.loads(task_dict['result'])
                    except:
                        pass
                
                # Convert timestamps to ISO format
                for field in ['created_at', 'updated_at', 'completed_at']:
                    if task_dict.get(field):
                        try:
                            # SQLite stores as string, parse and format
                            if isinstance(task_dict[field], str):
                                dt = datetime.fromisoformat(task_dict[field])
                            else:
                                dt = task_dict[field]
                            task_dict[field] = dt.isoformat()
                        except:
                            pass
                
                # Ensure is_dismissed is boolean
                task_dict['is_dismissed'] = bool(task_dict.get('is_dismissed', 0))
                
                tasks.append(task_dict)
        
        logging.info(f"📋 Retrieved {len(tasks)} non-dismissed tasks for {sosok}/{site}")
        return tasks
    
    def get_task(self, task_id: str) -> Optional[Dict]:
        """특정 작업 조회"""
        with self._get_db() as conn:
            cursor = conn.execute(
                "SELECT * FROM tasks WHERE id = ?",
                (task_id,)
            )
            row = cursor.fetchone()
            
            if row:
                task_dict = dict(row)
                
                # Parse JSON fields
                if task_dict.get('result'):
                    try:
                        task_dict['result'] = json.loads(task_dict['result'])
                    except:
                        pass
                
                # Convert timestamps to ISO format
                for field in ['created_at', 'updated_at', 'completed_at']:
                    if task_dict.get(field):
                        try:
                            if isinstance(task_dict[field], str):
                                dt = datetime.fromisoformat(task_dict[field])
                            else:
                                dt = task_dict[field]
                            task_dict[field] = dt.isoformat()
                        except:
                            pass
                
                # Ensure is_dismissed is boolean
                task_dict['is_dismissed'] = bool(task_dict.get('is_dismissed', 0))
                
                return task_dict
        
        return None
    
    def get_task_file_path(self, task_id: str) -> Optional[str]:
        """작업의 파일 경로 조회"""
        with self._get_db() as conn:
            cursor = conn.execute(
                "SELECT file_path FROM tasks WHERE id = ?",
                (task_id,)
            )
            row = cursor.fetchone()
            return row['file_path'] if row else None
    
    def _cleanup_stale_tasks(self):
        """오래된 진행 중인 작업을 실패로 처리"""
        # 10분 이상 업데이트되지 않은 진행 중인 작업을 찾음
        stale_threshold = datetime.now() - timedelta(minutes=10)
        
        with self._get_db() as conn:
            # 오래된 진행 중인 작업 찾기
            cursor = conn.execute('''
                SELECT id, filename, status, updated_at FROM tasks
                WHERE status IN (?, ?, ?)
                AND updated_at < ?
                AND is_dismissed = 0
            ''', (
                TaskStatus.UPLOADING.value,
                TaskStatus.QUEUED.value,
                TaskStatus.PROCESSING.value,
                stale_threshold
            ))
            
            stale_tasks = cursor.fetchall()
            
            if stale_tasks:
                logging.warning(f"⚠️ Found {len(stale_tasks)} stale tasks")
                
                for task in stale_tasks:
                    task_id = task['id']
                    filename = task['filename']
                    status = task['status']
                    
                    logging.warning(f"⚠️ Marking stale task as failed: {task_id} ({filename}) - was {status}")
                    
                    # 작업을 실패로 표시
                    now = datetime.now()
                    conn.execute('''
                        UPDATE tasks 
                        SET status = ?, message = ?, error = ?, 
                            updated_at = ?, completed_at = ?
                        WHERE id = ?
                    ''', (
                        TaskStatus.FAILED.value, 
                        "처리 시간 초과",
                        f"작업이 10분 이상 진행되지 않아 자동으로 취소되었습니다. (마지막 상태: {status})",
                        now, now, task_id
                    ))
                    
                    # 관련 파일 정리
                    file_path = self.get_task_file_path(task_id)
                    if file_path and os.path.exists(file_path):
                        try:
                            os.remove(file_path)
                            meta_path = file_path + ".meta.json"
                            if os.path.exists(meta_path):
                                os.remove(meta_path)
                            logging.info(f"🗑️ Cleaned up files for stale task {task_id}")
                        except Exception as e:
                            logging.warning(f"Failed to clean up files for stale task {task_id}: {e}")
                
                conn.commit()
                logging.info(f"✅ Marked {len(stale_tasks)} stale tasks as failed")
    
    def cleanup_old_tasks(self):
        """오래된 작업 정리 (24시간 이상)"""
        cutoff_time = datetime.now() - timedelta(hours=24)
        
        with self._get_db() as conn:
            # 먼저 파일 경로를 가져와서 파일 삭제
            cursor = conn.execute(
                "SELECT file_path FROM tasks WHERE created_at < ? AND file_path IS NOT NULL",
                (cutoff_time,)
            )
            
            file_paths = []
            for row in cursor:
                if row['file_path']:
                    file_paths.append(row['file_path'])
            
            # 데이터베이스에서 삭제
            result = conn.execute(
                "DELETE FROM tasks WHERE created_at < ?",
                (cutoff_time,)
            )
            conn.commit()
            deleted_count = result.rowcount
        
        # 파일 삭제
        for file_path in file_paths:
            try:
                if os.path.exists(file_path):
                    os.remove(file_path)
                meta_path = file_path + ".meta.json"
                if os.path.exists(meta_path):
                    os.remove(meta_path)
            except Exception as e:
                logging.warning(f"Failed to delete file {file_path}: {e}")
        
        if deleted_count > 0:
            logging.info(f"🧹 Cleaned up {deleted_count} old tasks")
    
    async def enqueue_for_processing(self, task_id: str):
        """처리 큐에 작업 추가"""
        await self.processing_queue.put(task_id)
        self.update_task_status(task_id, TaskStatus.QUEUED, message="처리 대기 중...")
    
    async def _check_stale_tasks(self):
        """주기적으로 오래된 작업 확인 및 정리"""
        while True:
            await asyncio.sleep(60)  # 1분마다 확인
            try:
                self._cleanup_stale_tasks()
            except Exception as e:
                logging.error(f"Error checking stale tasks: {e}")
    
    async def _periodic_cleanup(self):
        """주기적인 정리 작업"""
        while True:
            await asyncio.sleep(3600)  # 1시간마다
            try:
                self.cleanup_old_tasks()
            except Exception as e:
                logging.error(f"Error during cleanup: {e}")
    
    def cancel_task(self, task_id: str) -> bool:
        """작업 취소"""
        with self._get_db() as conn:
            # 작업 상태 확인
            cursor = conn.execute(
                "SELECT status, file_path FROM tasks WHERE id = ?",
                (task_id,)
            )
            row = cursor.fetchone()
            
            if not row:
                return False
            
            status = row['status']
            file_path = row['file_path']
            
            # uploading 또는 queued 상태만 취소 가능
            if status not in [TaskStatus.UPLOADING.value, TaskStatus.QUEUED.value]:
                logging.warning(f"Cannot cancel task {task_id} with status {status}")
                return False
            
            # 작업을 실패로 표시
            now = datetime.now()
            conn.execute('''
                UPDATE tasks 
                SET status = ?, message = ?, error = ?, 
                    updated_at = ?, completed_at = ?
                WHERE id = ?
            ''', (
                TaskStatus.FAILED.value, 
                "사용자가 취소함",
                "사용자 요청으로 작업이 취소되었습니다.",
                now, now, task_id
            ))
            conn.commit()
            
            # 관련 파일 정리
            if file_path and os.path.exists(file_path):
                try:
                    os.remove(file_path)
                    meta_path = file_path + ".meta.json"
                    if os.path.exists(meta_path):
                        os.remove(meta_path)
                    logging.info(f"🗑️ Cleaned up files for cancelled task {task_id}")
                except Exception as e:
                    logging.warning(f"Failed to clean up files for cancelled task {task_id}: {e}")
            
            logging.info(f"🚫 Cancelled task {task_id}")
            return True

# 전역 TaskManager 인스턴스
task_manager = TaskManager()