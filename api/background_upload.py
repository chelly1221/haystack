from fastapi import APIRouter, UploadFile, File, Form, HTTPException, BackgroundTasks
from typing import List, Optional
import os
import shutil
import uuid
import unicodedata
import logging
import json
import asyncio
from task_manager_sqlite import task_manager, TaskStatus

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
        """비동기 파일 업로드 - 즉시 응답 반환, 파일 처리는 백그라운드"""
        
        sosok = sosok.strip() if sosok else ""
        site = site.strip() if site else ""
        
        task_ids = []
        file_metadata_list = []  # 파일 메타데이터만 저장 (파일 내용은 읽지 않음)
        
        # 파일 검증만 수행 (파일 내용은 읽지 않음)
        for file in files:
            # 파일 확장자 검증
            ext = os.path.splitext(file.filename.lower())[-1]
            if ext not in [".pdf", ".hwpx", ".docx", ".pptx"]:
                continue
            
            # 작업 ID 생성
            task_id = str(uuid.uuid4())
            task_ids.append(task_id)
            
            # 파일 메타데이터만 저장 (파일 내용은 백그라운드에서 읽음)
            file_metadata_list.append({
                "task_id": task_id,
                "filename": file.filename,
                "file_obj": file,  # 파일 객체 저장 (내용은 아직 읽지 않음)
                "content_type": file.content_type,
                "tags": tags,
                "sosok": sosok,
                "site": site,
                "top_margin": top_margin,
                "bottom_margin": bottom_margin,
                "margin_settings": margin_settings,
                "overwrite_decisions": overwrite_decisions
            })
        
        # IMMEDIATE RESPONSE - 파일 내용을 읽지 않고 즉시 응답
        logging.info(f"📤 Accepting {len(task_ids)} files for background processing (no file reading)")
        
        # 백그라운드에서 파일 처리 스케줄링 (응답 후 파일 읽기)
        asyncio.create_task(process_uploaded_files_from_metadata(file_metadata_list, task_manager))
        
        # 즉시 응답 반환 - 파일 처리를 기다리지 않음
        return {
            "status": "accepted",
            "task_ids": task_ids,
            "message": f"{len(task_ids)}개 파일이 접수되었습니다. 처리가 시작됩니다..."
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


async def process_uploaded_files_from_metadata(file_metadata_list, task_manager):
    """응답 후 백그라운드에서 파일 메타데이터로부터 파일을 처리"""
    import unicodedata
    
    # 업로드 디렉토리 확인
    os.makedirs("./uploads", exist_ok=True)
    
    for file_metadata in file_metadata_list:
        task_id = file_metadata["task_id"]
        filename = file_metadata["filename"]
        file_obj = file_metadata["file_obj"]
        
        try:
            # 1. 데이터베이스에 작업 등록
            task_manager.create_task_with_id(task_id, filename, file_metadata["sosok"], file_metadata["site"])
            
            # 2. 파일 내용을 이제 읽기 (백그라운드에서)
            task_manager.update_task_status(
                task_id, 
                TaskStatus.UPLOADING,
                progress=25,
                message="파일 읽기 중..."
            )
            
            # 파일 내용 읽기 (응답 후이므로 블로킹되어도 문제없음)
            content = await file_obj.read()
            
            # 3. 파일명 정규화 및 파일 저장
            normalized_filename = unicodedata.normalize("NFC", filename.strip())
            unique_filename = f"{uuid.uuid4().hex}_{normalized_filename}"
            file_path = os.path.join("./uploads", unique_filename)
            
            # 파일 저장 상태로 업데이트
            task_manager.update_task_status(
                task_id, 
                TaskStatus.UPLOADING,
                progress=50,
                message="파일 저장 중..."
            )
            
            # 파일을 디스크에 저장
            with open(file_path, "wb") as f:
                f.write(content)
            
            # 파일 경로 저장
            task_manager.set_task_file_path(task_id, file_path)
            
            # 메타데이터 파일 생성
            meta_path = file_path + ".meta.json"
            metadata = {
                "task_id": task_id,
                "file_path": file_path,
                "tags": file_metadata["tags"],
                "sosok": file_metadata["sosok"],
                "site": file_metadata["site"],
                "top_margin": file_metadata["top_margin"],
                "bottom_margin": file_metadata["bottom_margin"],
                "margin_settings": file_metadata["margin_settings"],
                "overwrite_decisions": file_metadata["overwrite_decisions"],
                "original_filename": filename
            }
            with open(meta_path, "w") as f:
                json.dump(metadata, f)
            
            # 처리 대기 상태로 변경
            task_manager.update_task_status(
                task_id, 
                TaskStatus.QUEUED,
                progress=100,
                message="처리 대기 중..."
            )
            
            # 처리 큐에 추가
            await task_manager.processing_queue.put(task_id)
            
            logging.info(f"✅ Background file processing queued: {filename} (task_id: {task_id})")
            
            # 메모리에서 파일 콘텐츠 제거
            del content
            
        except Exception as e:
            logging.error(f"❌ Background processing error for {filename}: {e}")
            # 실패한 작업 처리
            try:
                task_manager.fail_task(task_id, str(e))
            except:
                # 작업이 아직 생성되지 않은 경우 무시
                pass


# Keep old function for backward compatibility
async def process_uploaded_files(file_data_list, task_manager):
    """Legacy function - kept for backward compatibility"""
    pass


async def process_file_task(task_id: str, document_store, embedder):
    """실제 파일 처리 로직"""
    from util.pdf import split_pdf_by_section_headings, split_pdf_by_token_window
    from util.embedding import embed_document_sections
    from decimal import Decimal
    import pdfplumber
    import re
    import gc
    
    try:
        task = task_manager.get_task(task_id)
        if not task:
            return
        
        # 상태를 처리 중으로 변경
        task_manager.update_task_status(task_id, TaskStatus.PROCESSING, 
                                      progress=10, message="파일 분석 중...")
        
        # 파일 경로 가져오기
        file_path = task_manager.get_task_file_path(task_id)
        
        if not file_path or not os.path.exists(file_path):
            raise Exception("파일을 찾을 수 없습니다")
        
        # 메타데이터 로드
        meta_path = file_path + ".meta.json"
        if not os.path.exists(meta_path):
            raise Exception("메타데이터 파일을 찾을 수 없습니다")
            
        with open(meta_path, "r") as f:
            metadata = json.load(f)
        
        ext = os.path.splitext(file_path.lower())[-1]
        sections = []
        total_pages = 0
        
        # 파일 처리
        if ext == ".pdf":
            task_manager.update_task_status(task_id, TaskStatus.PROCESSING,
                                          progress=20, message="PDF 분석 중...")
            
            # 유지보수 문서 확인
            def detect_maintenance_doc(path):
                try:
                    with pdfplumber.open(path) as pdf:
                        for page in pdf.pages[:3]:  # 처음 3페이지만 확인
                            text = page.extract_text()
                            if text:
                                first_line = text.strip().splitlines()[0] if text.strip().splitlines() else ""
                                cleaned = re.sub(r"\s+", "", first_line)
                                return cleaned.startswith("유지보수교범")
                except Exception as e:
                    logging.warning(f"⚠️ Error detecting maintenance doc: {e}")
                return False
            
            is_maintenance = detect_maintenance_doc(file_path)
            
            # 마진 설정
            margin_map = {}
            if metadata.get("margin_settings"):
                try:
                    margin_map = json.loads(metadata["margin_settings"])
                except:
                    pass
            
            file_margins = margin_map.get(metadata["original_filename"], {})
            top_margin = Decimal(str(file_margins.get("top_margin", metadata.get("top_margin", 0.1))))
            bottom_margin = Decimal(str(file_margins.get("bottom_margin", metadata.get("bottom_margin", 0.1))))
            
            # PDF 분할
            try:
                if is_maintenance:
                    sections = split_pdf_by_section_headings(
                        file_path, None, top_margin, bottom_margin,
                        doc_id=None, extract_text_tables=True,
                        auto_detect_header_footer=True,
                        document_title=metadata["original_filename"]
                    )
                else:
                    sections = split_pdf_by_token_window(
                        file_path, top_margin, bottom_margin,
                        700, 100, "./models/KURE-v1",
                        None, True, True
                    )
            except Exception as e:
                logging.error(f"❌ PDF 처리 중 오류: {e}")
                raise Exception(f"PDF 처리 실패: {str(e)}")
            
            # 총 페이지 수
            with pdfplumber.open(file_path) as pdf:
                total_pages = len(pdf.pages)
            
            task_manager.update_task_status(task_id, TaskStatus.PROCESSING,
                                          total_pages=total_pages)
        
        elif ext == ".hwpx":
            from util.hwpx import split_hwpx_by_pages
            task_manager.update_task_status(task_id, TaskStatus.PROCESSING,
                                          progress=20, message="HWPX 분석 중...")
            try:
                sections = split_hwpx_by_pages(file_path, os.path.basename(file_path))
                total_pages = max([s.get("page_number", 0) for s in sections]) if sections else 1
            except Exception as e:
                logging.error(f"❌ HWPX 처리 중 오류: {e}")
                raise Exception(f"HWPX 처리 실패: {str(e)}")
        
        elif ext == ".docx":
            from util.docx import split_docx_by_token_window
            from transformers import AutoTokenizer
            task_manager.update_task_status(task_id, TaskStatus.PROCESSING,
                                          progress=20, message="DOCX 분석 중...")
            try:
                tokenizer = AutoTokenizer.from_pretrained("./models/KURE-v1")
                sections = split_docx_by_token_window(file_path, 700, 100, tokenizer)
                total_pages = len(sections)
            except Exception as e:
                logging.error(f"❌ DOCX 처리 중 오류: {e}")
                raise Exception(f"DOCX 처리 실패: {str(e)}")
        
        elif ext == ".pptx":
            from util.pptx import split_pptx_by_token_window
            from transformers import AutoTokenizer
            task_manager.update_task_status(task_id, TaskStatus.PROCESSING,
                                          progress=20, message="PPTX 분석 중...")
            try:
                tokenizer = AutoTokenizer.from_pretrained("./models/KURE-v1")
                sections = split_pptx_by_token_window(file_path, 700, 100, tokenizer)
                total_pages = len(sections)
            except Exception as e:
                logging.error(f"❌ PPTX 처리 중 오류: {e}")
                raise Exception(f"PPTX 처리 실패: {str(e)}")
        
        if not sections:
            raise Exception("문서에서 추출된 내용이 없습니다")
        
        # 임베딩 처리
        task_manager.update_task_status(task_id, TaskStatus.PROCESSING,
                                      progress=50, message="텍스트 임베딩 중...")
        
        metadata_base = {
            "original_filename": metadata["original_filename"],
            "tags": metadata.get("tags", ""),
            "sosok": metadata["sosok"],
            "site": metadata["site"],
            "file_id": os.path.basename(file_path),
            "total_pdf_pages": total_pages
        }
        
        # 진행률 업데이트를 위한 콜백
        processed_count = 0
        def update_progress():
            nonlocal processed_count
            processed_count += 1
            progress = 50 + int((processed_count / len(sections)) * 40)
            task_manager.update_task_status(
                task_id, TaskStatus.PROCESSING,
                progress=progress,
                processed_pages=processed_count,
                message=f"임베딩 중... ({processed_count}/{len(sections)})"
            )
        
        # 섹션별로 임베딩 (진행률 업데이트 포함)
        embedded_docs = []
        for section in sections:
            if len(section.get("content", "").strip()) < 1:
                continue
            
            meta = {
                **metadata_base,
                "section_title": section.get("title", ""),
                "section_id": section.get("section_id", f"{len(embedded_docs) + 1}"),
                "section_number": len(embedded_docs) + 1,
                "page_number": section.get("page_number", section.get("start_page", 1)),
                "total_pdf_pages": total_pages
            }
            
            from haystack import Document
            content_with_header = f"문서: {metadata['original_filename']}\n<h2>{section.get('title', '')}</h2>\n{section['content']}"
            doc = Document(content=content_with_header, meta=meta)
            
            try:
                embedded_section = embedder.run([doc])["documents"]
                embedded_docs.extend(embedded_section)
            except Exception as e:
                logging.warning(f"⚠️ 임베딩 실패 (섹션 {len(embedded_docs) + 1}): {e}")
                continue
            
            update_progress()
            
            # 메모리 관리
            if len(embedded_docs) % 50 == 0:
                gc.collect()
        
        # 문서 저장
        task_manager.update_task_status(task_id, TaskStatus.PROCESSING,
                                      progress=90, message="데이터베이스 저장 중...")
        
        valid_docs = [doc for doc in embedded_docs if doc.embedding is not None and len(doc.embedding) > 0]
        if valid_docs:
            try:
                # 배치로 저장 (메모리 효율성)
                batch_size = 100
                for i in range(0, len(valid_docs), batch_size):
                    batch = valid_docs[i:i+batch_size]
                    document_store.write_documents(batch)
                    await asyncio.sleep(0.1)  # 다른 작업에 CPU 양보
            except Exception as e:
                logging.error(f"❌ 문서 저장 실패: {e}")
                raise Exception(f"데이터베이스 저장 실패: {str(e)}")
        else:
            raise Exception("유효한 임베딩 문서가 없습니다")
        
        # 임시 파일 정리
        try:
            os.remove(file_path)
            os.remove(meta_path)
        except Exception as e:
            logging.warning(f"⚠️ Failed to clean up temporary files: {e}")
        
        # 메모리 정리
        del embedded_docs
        gc.collect()
        
        # 작업 완료
        result = {
            "status": "성공",
            "original_filename": metadata["original_filename"],
            "num_sections": len(valid_docs),
            "total_pages": total_pages
        }
        task_manager.complete_task(task_id, result)
        
    except Exception as e:
        error_msg = str(e)
        logging.error(f"❌ Error processing task {task_id}: {error_msg}")
        import traceback
        traceback.print_exc()
        
        # 작업 실패 처리
        task_manager.fail_task(task_id, error_msg)
        
        # 실패 시 임시 파일 정리
        try:
            file_path = task_manager.get_task_file_path(task_id)
            if file_path and os.path.exists(file_path):
                os.remove(file_path)
            meta_path = file_path + ".meta.json"
            if os.path.exists(meta_path):
                os.remove(meta_path)
        except:
            pass
        
        # 메모리 정리
        gc.collect()