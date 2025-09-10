#!/usr/bin/env python3
"""
Standalone File Processor Service
Independent background service that monitors and processes uploaded files
"""

import os
import time
import logging
import json
import asyncio
from pathlib import Path
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
import sqlite3
from typing import Optional
from dataclasses import dataclass
from enum import Enum
import uuid
import unicodedata

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('/tmp/file_processor.log'),
        logging.StreamHandler()
    ]
)

class TaskStatus(Enum):
    PENDING = "pending"
    PROCESSING = "processing" 
    COMPLETED = "completed"
    FAILED = "failed"

@dataclass
class FileTask:
    file_id: str
    original_filename: str
    file_path: str
    status: TaskStatus
    progress: int = 0
    message: str = ""
    created_at: float = None
    updated_at: float = None

class SimpleTaskManager:
    """Lightweight task management for the file processor"""
    
    def __init__(self, db_path: str = "./data/processor_tasks.db"):
        self.db_path = db_path
        os.makedirs(os.path.dirname(db_path), exist_ok=True)
        self.init_db()
    
    def init_db(self):
        """Initialize the task database"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS file_tasks (
                file_id TEXT PRIMARY KEY,
                original_filename TEXT NOT NULL,
                file_path TEXT NOT NULL,
                status TEXT NOT NULL,
                progress INTEGER DEFAULT 0,
                message TEXT DEFAULT '',
                created_at REAL NOT NULL,
                updated_at REAL NOT NULL
            )
        ''')
        
        conn.commit()
        conn.close()
    
    def create_task(self, file_id: str, original_filename: str, file_path: str) -> FileTask:
        """Create a new file processing task"""
        now = time.time()
        task = FileTask(
            file_id=file_id,
            original_filename=original_filename,
            file_path=file_path,
            status=TaskStatus.PENDING,
            created_at=now,
            updated_at=now
        )
        
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
            INSERT INTO file_tasks (file_id, original_filename, file_path, status, progress, message, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ''', (task.file_id, task.original_filename, task.file_path, task.status.value, 
              task.progress, task.message, task.created_at, task.updated_at))
        
        conn.commit()
        conn.close()
        
        logging.info(f"📋 Created task: {file_id} - {original_filename}")
        return task
    
    def update_task(self, file_id: str, status: TaskStatus, progress: int = None, message: str = None):
        """Update task status and progress"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        updates = ["status = ?", "updated_at = ?"]
        params = [status.value, time.time()]
        
        if progress is not None:
            updates.append("progress = ?")
            params.append(progress)
        
        if message is not None:
            updates.append("message = ?")
            params.append(message)
        
        params.append(file_id)
        
        cursor.execute(f'''
            UPDATE file_tasks SET {", ".join(updates)}
            WHERE file_id = ?
        ''', params)
        
        conn.commit()
        conn.close()
        
        logging.info(f"📋 Updated task {file_id}: {status.value} ({progress}%) - {message}")

class FileProcessorHandler(FileSystemEventHandler):
    """File system event handler for processing new uploads"""
    
    def __init__(self, processor):
        self.processor = processor
        super().__init__()
    
    def on_created(self, event):
        """Handle new file creation"""
        if not event.is_directory:
            # Give file time to be fully written
            time.sleep(0.5)
            asyncio.create_task(self.processor.process_new_file(event.src_path))

class StandaloneFileProcessor:
    """Completely independent file processing service"""
    
    def __init__(self):
        self.upload_dir = "./uploads/simple"
        self.processed_dir = "./uploads/processed"
        self.task_manager = SimpleTaskManager()
        self.is_running = False
        
        # Ensure directories exist
        os.makedirs(self.upload_dir, exist_ok=True)
        os.makedirs(self.processed_dir, exist_ok=True)
        
        logging.info("🎯 Standalone File Processor initialized")
    
    def start_watching(self):
        """Start watching the upload directory"""
        self.is_running = True
        
        # Setup file system watcher
        event_handler = FileProcessorHandler(self)
        observer = Observer()
        observer.schedule(event_handler, self.upload_dir, recursive=False)
        observer.start()
        
        logging.info(f"👀 Started watching directory: {self.upload_dir}")
        
        try:
            # Process any existing files first
            self.process_existing_files()
            
            # Keep the service running
            while self.is_running:
                time.sleep(1)
        except KeyboardInterrupt:
            logging.info("🛑 Received shutdown signal")
        finally:
            observer.stop()
            observer.join()
            logging.info("✅ File processor service stopped")
    
    def process_existing_files(self):
        """Process any files that were uploaded while service was down"""
        for file_path in Path(self.upload_dir).glob("*"):
            if file_path.is_file() and not file_path.name.startswith('.'):
                asyncio.create_task(self.process_new_file(str(file_path)))
    
    async def process_new_file(self, file_path: str):
        """Process a newly uploaded file"""
        try:
            filename = os.path.basename(file_path)
            
            # Extract file_id and original filename from the naming convention
            # Format: {file_id}_{original_filename}
            if '_' not in filename:
                logging.warning(f"⚠️ Skipping file with invalid naming: {filename}")
                return
            
            file_id, original_filename = filename.split('_', 1)
            
            logging.info(f"🔄 Processing new file: {original_filename} (ID: {file_id})")
            
            # Create task
            task = self.task_manager.create_task(file_id, original_filename, file_path)
            
            # Start processing
            self.task_manager.update_task(file_id, TaskStatus.PROCESSING, 10, "파일 분석 시작...")
            
            # TODO: Add actual file processing logic here
            # For now, just simulate processing
            await self.simulate_processing(file_id, file_path, original_filename)
            
            # Move to processed directory
            processed_path = os.path.join(self.processed_dir, filename)
            os.rename(file_path, processed_path)
            
            # Mark as completed
            self.task_manager.update_task(file_id, TaskStatus.COMPLETED, 100, "처리 완료")
            
            logging.info(f"✅ Completed processing: {original_filename}")
            
        except Exception as e:
            logging.error(f"❌ Error processing file {file_path}: {e}")
            if 'file_id' in locals():
                self.task_manager.update_task(file_id, TaskStatus.FAILED, 0, f"처리 실패: {str(e)}")
    
    async def simulate_processing(self, file_id: str, file_path: str, original_filename: str):
        """Process file - simplified version for standalone service"""
        
        try:
            # File validation
            self.task_manager.update_task(file_id, TaskStatus.PROCESSING, 20, "파일 유효성 검사...")
            
            ext = os.path.splitext(original_filename.lower())[-1]
            if ext not in [".pdf", ".docx", ".pptx", ".hwpx"]:
                raise Exception(f"지원하지 않는 파일 형식: {ext}")
            
            await asyncio.sleep(1)
            
            # Basic file processing
            if ext == ".pdf":
                self.task_manager.update_task(file_id, TaskStatus.PROCESSING, 40, "PDF 분석 중...")
                await asyncio.sleep(2)  # Simulate PDF processing
                
                self.task_manager.update_task(file_id, TaskStatus.PROCESSING, 60, "텍스트 추출 중...")
                await asyncio.sleep(2)  # Simulate text extraction
                
            else:
                self.task_manager.update_task(file_id, TaskStatus.PROCESSING, 50, f"{ext.upper()} 처리 중...")
                await asyncio.sleep(1)
            
            # Simulate embedding and storage
            self.task_manager.update_task(file_id, TaskStatus.PROCESSING, 80, "임베딩 생성 중...")
            await asyncio.sleep(2)
            
            self.task_manager.update_task(file_id, TaskStatus.PROCESSING, 90, "데이터베이스 저장 중...")
            await asyncio.sleep(1)
            
            logging.info(f"✅ Successfully processed: {original_filename}")
            
        except Exception as e:
            logging.error(f"❌ Processing error for {original_filename}: {e}")
            raise

def main():
    """Main entry point for the standalone service"""
    logging.info("🚀 Starting Standalone File Processor Service")
    
    processor = StandaloneFileProcessor()
    processor.start_watching()

if __name__ == "__main__":
    main()