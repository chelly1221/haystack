from fastapi import APIRouter, UploadFile, File, Form, HTTPException
from typing import List, Optional
import os
import uuid
import unicodedata
import logging
import json
import time
from task_manager_sqlite import task_manager

def get_background_upload_router(document_store, embedder):
    router = APIRouter()
    
    # 백그라운드 워커 시작
    async def start_background_worker():
        """백그라운드 작업 처리 워커"""
        while True:
            try:
                # 큐에서 작업 가져오기 (타임아웃 설정으로 주기적 체크)
                try:
                    task_id = await asyncio.wait_for(
                        task_manager.processing_queue.get(), 
                        timeout=1.0
                    )
                except asyncio.TimeoutError:
                    continue
                
                task = task_manager.get_task(task_id)
                
                if not task:
                    continue
                
                # 실제 처리 함수 호출
                await process_file_task(task_id, document_store, embedder)
                
            except Exception as e:
                logging.error(f"❌ Background worker error: {e}")
                await asyncio.sleep(1)
    
    # 애플리케이션 시작 시 워커 실행
    asyncio.create_task(start_background_worker())
    
    # 주기적인 정리 작업
    async def cleanup_worker():
        while True:
            await asyncio.sleep(3600)  # 1시간마다
            task_manager.cleanup_old_tasks()
    
    asyncio.create_task(cleanup_worker())
    
    @router.post("/upload-async/")
    async def upload_async(
        files: List[UploadFile] = File(...),
        tags: Optional[str] = Form(None),
        sosok: Optional[str] = Form(None),
        site: Optional[str] = Form(None),
        type: Optional[str] = Form(None),
        top_margin: Optional[float] = Form(0.1),
        bottom_margin: Optional[float] = Form(0.1),
        margin_settings: Optional[str] = Form(None),
        overwrite_decisions: Optional[str] = Form(None)
    ):
        """파일 업로드만 처리 - 즉시 응답, 처리는 백그라운드 서비스가 담당"""
        
        sosok = sosok.strip() if sosok else ""
        site = site.strip() if site else ""
        
        uploaded_files = []
        
        # 임시 디렉토리 확인 및 생성
        temp_dir = "./uploads/temp"
        os.makedirs(temp_dir, exist_ok=True)
        
        # 파일 저장만 수행 - 처리 로직 없음
        for file in files:
            # 파일 확장자 검증
            ext = os.path.splitext(file.filename.lower())[-1]
            if ext not in [".pdf", ".hwpx", ".docx", ".pptx"]:
                continue
            
            # UUID 생성
            file_uuid = str(uuid.uuid4())
            
            # 파일명 정규화
            normalized_filename = unicodedata.normalize("NFC", file.filename.strip())
            temp_filename = f"{file_uuid}_{normalized_filename}"
            temp_file_path = os.path.join(temp_dir, temp_filename)
            meta_file_path = temp_file_path + ".meta"
            
            # 파일 저장
            content = await file.read()
            with open(temp_file_path, "wb") as f:
                f.write(content)
            
            # 메타데이터 저장
            metadata = {
                "uuid": file_uuid,
                "original_filename": file.filename,
                "content_type": file.content_type,
                "tags": tags,
                "sosok": sosok,
                "site": site,
                "top_margin": top_margin,
                "bottom_margin": bottom_margin,
                "margin_settings": margin_settings,
                "overwrite_decisions": overwrite_decisions,
                "upload_timestamp": time.time()
            }
            
            with open(meta_file_path, "w", encoding="utf-8") as f:
                json.dump(metadata, f, ensure_ascii=False, indent=2)
            
            uploaded_files.append({
                "uuid": file_uuid,
                "filename": file.filename,
                "size": len(content)
            })
            
            logging.info(f"📁 File uploaded: {temp_filename}")
        
        logging.info(f"📤 Uploaded {len(uploaded_files)} files, responding immediately")
        
        # 즉시 응답 반환 - 백그라운드 프로세서가 처리 담당
        return {
            "status": "uploaded",
            "files": uploaded_files,
            "message": f"{len(uploaded_files)}개 파일이 업로드되었습니다."
        }
    
    @router.get("/tasks/")
    async def get_tasks(sosok: str, site: str):
        """현장별 작업 목록 조회"""
        tasks = task_manager.get_tasks_by_site(sosok, site)
        return {"tasks": tasks}
    
    @router.get("/task/{task_id}")
    async def get_task_status(task_id: str):
        """특정 작업 상태 조회"""
        task = task_manager.get_task(task_id)
        if not task:
            raise HTTPException(status_code=404, detail="Task not found")
        return task
    
    @router.post("/dismiss-task/{task_id}")
    async def dismiss_task(task_id: str):
        """작업 UI에서 제거 (완료/실패 작업)"""
        task = task_manager.get_task(task_id)
        if not task:
            raise HTTPException(status_code=404, detail="Task not found")
        
        # 실패한 작업도 dismiss 가능
        if task['status'] not in ['completed', 'failed']:
            raise HTTPException(status_code=400, detail="Only completed or failed tasks can be dismissed")
        
        task_manager.dismiss_task(task_id)
        
        # 실패한 작업의 임시 파일 정리
        if task['status'] == 'failed':
            try:
                file_path = task_manager.get_task_file_path(task_id)
                if file_path and os.path.exists(file_path):
                    os.remove(file_path)
                meta_path = file_path + ".meta.json"
                if os.path.exists(meta_path):
                    os.remove(meta_path)
            except Exception as e:
                logging.warning(f"⚠️ Failed to clean up files for failed task: {e}")
        
        return {"status": "success", "message": "Task dismissed"}
    
    @router.post("/dismiss-completed-tasks")
    async def dismiss_completed_tasks(
        sosok: Optional[str] = Form(None),
        site: Optional[str] = Form(None)
    ):
        """현장의 모든 완료/실패 작업 제거"""
        sosok = sosok.strip() if sosok else ""
        site = site.strip() if site else ""
        
        logging.info(f"📡 Dismissing completed tasks for sosok='{sosok}', site='{site}'")
        
        task_manager.dismiss_completed_tasks(sosok, site)
        
        # 디버깅을 위해 현재 상태 확인
        remaining_tasks = task_manager.get_tasks_by_site(sosok, site)
        logging.info(f"📊 Remaining tasks after dismiss: {len(remaining_tasks)}")
        
        return {"status": "success", "message": "All completed tasks dismissed"}
    
    @router.post("/cancel-task/{task_id}")
    async def cancel_task(task_id: str):
        """진행 중인 작업 취소"""
        task = task_manager.get_task(task_id)
        if not task:
            raise HTTPException(status_code=404, detail="Task not found")
        
        # 작업 취소 (task_manager의 cancel_task 메서드 사용)
        success = task_manager.cancel_task(task_id)
        
        if success:
            return {"status": "success", "message": "Task cancelled"}
        else:
            raise HTTPException(status_code=400, detail="Cannot cancel this task")
    
    return router
