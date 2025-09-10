"""
Processor Task API - Query status from the standalone file processor
"""

from fastapi import APIRouter, HTTPException
import sqlite3
import logging
from typing import List, Optional

def get_processor_task_router():
    router = APIRouter()
    
    @router.get("/processor-tasks/")
    async def list_processor_tasks(sosok: Optional[str] = None, site: Optional[str] = None):
        """Get all processing tasks from standalone processor"""
        try:
            db_path = "./data/processor_tasks.db"
            conn = sqlite3.connect(db_path)
            conn.row_factory = sqlite3.Row  # Enable dict-like access
            cursor = conn.cursor()
            
            cursor.execute('''
                SELECT file_id, original_filename, status, progress, message, created_at, updated_at
                FROM file_tasks
                ORDER BY created_at DESC
            ''')
            
            tasks = []
            for row in cursor.fetchall():
                tasks.append({
                    "file_id": row["file_id"],
                    "filename": row["original_filename"],
                    "status": row["status"],
                    "progress": row["progress"],
                    "message": row["message"],
                    "created_at": row["created_at"],
                    "updated_at": row["updated_at"]
                })
            
            conn.close()
            
            return {"tasks": tasks}
            
        except Exception as e:
            logging.error(f"❌ Error getting processor tasks: {e}")
            raise HTTPException(status_code=500, detail=str(e))
    
    @router.get("/processor-task/{task_id}")
    async def get_processor_task(task_id: str):
        """Get specific task status"""
        try:
            db_path = "./data/processor_tasks.db"
            conn = sqlite3.connect(db_path)
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            
            cursor.execute('''
                SELECT file_id, original_filename, status, progress, message, created_at, updated_at
                FROM file_tasks
                WHERE file_id = ?
            ''', (task_id,))
            
            row = cursor.fetchone()
            conn.close()
            
            if not row:
                raise HTTPException(status_code=404, detail="Task not found")
            
            return {
                "file_id": row["file_id"],
                "filename": row["original_filename"],
                "status": row["status"],
                "progress": row["progress"],
                "message": row["message"],
                "created_at": row["created_at"],
                "updated_at": row["updated_at"]
            }
            
        except HTTPException:
            raise
        except Exception as e:
            logging.error(f"❌ Error getting processor task {task_id}: {e}")
            raise HTTPException(status_code=500, detail=str(e))
    
    @router.delete("/processor-task/{task_id}")
    async def delete_processor_task(task_id: str):
        """Delete completed/failed task"""
        try:
            db_path = "./data/processor_tasks.db"
            conn = sqlite3.connect(db_path)
            cursor = conn.cursor()
            
            # Check if task exists and is completed/failed
            cursor.execute('SELECT status FROM file_tasks WHERE file_id = ?', (task_id,))
            row = cursor.fetchone()
            
            if not row:
                conn.close()
                raise HTTPException(status_code=404, detail="Task not found")
            
            if row[0] not in ['completed', 'failed']:
                conn.close()
                raise HTTPException(status_code=400, detail="Can only delete completed or failed tasks")
            
            # Delete the task
            cursor.execute('DELETE FROM file_tasks WHERE file_id = ?', (task_id,))
            conn.commit()
            conn.close()
            
            return {"status": "success", "message": "Task deleted"}
            
        except HTTPException:
            raise
        except Exception as e:
            logging.error(f"❌ Error deleting processor task {task_id}: {e}")
            raise HTTPException(status_code=500, detail=str(e))
    
    return router