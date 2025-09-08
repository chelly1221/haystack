"""
Background File Processor Service
독립적으로 temp 디렉토리를 모니터링하여 새로운 파일을 처리하는 서비스
"""

import os
import json
import time
import asyncio
import logging
import shutil
import unicodedata
from pathlib import Path
from typing import Dict, List
from task_manager_sqlite import task_manager, TaskStatus

class BackgroundFileProcessor:
    def __init__(self, document_store, embedder):
        self.document_store = document_store
        self.embedder = embedder
        self.temp_dir = "./uploads/temp"
        self.final_dir = "./uploads"
        self.processed_dir = os.path.join(self.temp_dir, "processed")
        self.scan_interval = 2  # 2초마다 스캔
        self.is_running = False
        
        # 디렉토리 생성
        os.makedirs(self.temp_dir, exist_ok=True)
        os.makedirs(self.final_dir, exist_ok=True)
        os.makedirs(self.processed_dir, exist_ok=True)
        
        logging.info("🎯 Background File Processor initialized")

    async def start(self):
        """백그라운드 프로세서 시작"""
        self.is_running = True
        logging.info("🚀 Starting background file processor...")
        
        # 두 개의 동시 작업: 파일 스캔 + 큐 처리
        await asyncio.gather(
            self.file_scanner(),
            self.queue_processor()
        )

    async def file_scanner(self):
        """파일 스캔 루프"""
        while self.is_running:
            try:
                await self.scan_and_process()
                await asyncio.sleep(self.scan_interval)
            except Exception as e:
                logging.error(f"❌ File scanner error: {e}")
                await asyncio.sleep(self.scan_interval)

    async def queue_processor(self):
        """처리 큐 소비 루프"""
        logging.info("🔄 Starting queue processor...")
        
        while self.is_running:
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
                await self.process_file_task(task_id)
                
            except Exception as e:
                logging.error(f"❌ Queue processor error: {e}")
                await asyncio.sleep(1)

    async def stop(self):
        """백그라운드 프로세서 중지"""
        self.is_running = False
        logging.info("🛑 Background file processor stopped")

    async def scan_and_process(self):
        """temp 디렉토리 스캔 및 새로운 파일 처리"""
        try:
            # temp 디렉토리에서 .meta 파일 찾기
            for meta_file in Path(self.temp_dir).glob("*.meta"):
                if not meta_file.is_file():
                    continue
                
                # 해당 파일이 존재하는지 확인
                file_path = str(meta_file).replace(".meta", "")
                if not os.path.exists(file_path):
                    logging.warning(f"⚠️ Meta file without corresponding file: {meta_file}")
                    meta_file.unlink()  # 메타파일 삭제
                    continue
                
                # 처리 중인지 확인 (락 파일 체크)
                lock_file = file_path + ".lock"
                if os.path.exists(lock_file):
                    continue
                
                # 파일 처리 시작
                await self.process_file(file_path, str(meta_file))
                
        except Exception as e:
            logging.error(f"❌ Error scanning temp directory: {e}")

    async def process_file(self, file_path: str, meta_file_path: str):
        """단일 파일 처리"""
        lock_file = file_path + ".lock"
        
        try:
            # 락 파일 생성
            with open(lock_file, "w") as f:
                f.write(str(time.time()))
            
            # 메타데이터 로드
            with open(meta_file_path, "r", encoding="utf-8") as f:
                metadata = json.load(f)
            
            file_uuid = metadata["uuid"]
            original_filename = metadata["original_filename"]
            logging.info(f"🔄 Processing file: {original_filename} (UUID: {file_uuid})")
            
            # 데이터베이스에 작업 생성
            task_manager.create_task_with_id(
                file_uuid, 
                original_filename,
                metadata["sosok"],
                metadata["site"]
            )
            
            # 작업 상태 업데이트
            task_manager.update_task_status(
                file_uuid,
                TaskStatus.UPLOADING,
                progress=10,
                message="파일 처리 시작..."
            )
            
            # 최종 파일 경로 생성
            normalized_filename = unicodedata.normalize("NFC", original_filename.strip())
            unique_filename = f"{file_uuid}_{normalized_filename}"
            final_file_path = os.path.join(self.final_dir, unique_filename)
            
            # 파일 이동
            shutil.move(file_path, final_file_path)
            logging.info(f"📁 Moved file to: {final_file_path}")
            
            task_manager.update_task_status(
                file_uuid,
                TaskStatus.UPLOADING,
                progress=30,
                message="파일 이동 완료, 처리 시작..."
            )
            
            # 파일 경로 저장
            task_manager.set_task_file_path(file_uuid, final_file_path)
            
            # 최종 위치에 메타데이터 저장 (기존 시스템 호환성)
            final_meta_path = final_file_path + ".meta.json"
            final_metadata = {
                "task_id": file_uuid,
                "file_path": final_file_path,
                "tags": metadata["tags"],
                "sosok": metadata["sosok"],
                "site": metadata["site"],
                "top_margin": metadata["top_margin"],
                "bottom_margin": metadata["bottom_margin"],
                "margin_settings": metadata["margin_settings"],
                "overwrite_decisions": metadata["overwrite_decisions"],
                "original_filename": metadata["original_filename"]
            }
            with open(final_meta_path, "w", encoding="utf-8") as f:
                json.dump(final_metadata, f, ensure_ascii=False, indent=2)
            
            # 처리 큐에 작업 추가
            await task_manager.processing_queue.put(file_uuid)
            
            task_manager.update_task_status(
                file_uuid,
                TaskStatus.QUEUED,
                progress=100,
                message="처리 대기 중..."
            )
            
            # 처리 완료 - 메타파일과 락파일 정리
            os.remove(meta_file_path)
            if os.path.exists(lock_file):
                os.remove(lock_file)
                
            logging.info(f"✅ File processing completed: {original_filename}")
            
        except Exception as e:
            logging.error(f"❌ Error processing file {file_path}: {e}")
            
            # 에러 발생 시 작업 상태 업데이트
            if 'file_uuid' in locals():
                task_manager.update_task_status(
                    file_uuid,
                    TaskStatus.FAILED,
                    message=f"처리 실패: {str(e)}"
                )
            
            # 락 파일 정리
            if os.path.exists(lock_file):
                os.remove(lock_file)

    async def process_file_task(self, task_id: str):
        """실제 파일 처리 로직 (큐에서 소비된 작업 처리)"""
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
                
            with open(meta_path, "r", encoding="utf-8") as f:
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
                
                # PDF 분할 처리
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
                            doc_id=None, window_size=2048,
                            document_title=metadata["original_filename"]
                        )
                    
                    total_pages = len(sections) if sections else 0
                    
                except Exception as e:
                    logging.error(f"❌ PDF processing error: {e}")
                    raise Exception(f"PDF 처리 실패: {str(e)}")
            
            elif ext in [".docx", ".pptx", ".hwpx"]:
                # 기타 문서 형식 처리
                task_manager.update_task_status(task_id, TaskStatus.PROCESSING,
                                              progress=30, message=f"{ext.upper()} 처리 중...")
                
                # TODO: DOCX, PPTX, HWPX 처리 로직 추가
                sections = []  # 임시
                total_pages = 1  # 임시
            
            # 임베딩 처리
            if sections:
                task_manager.update_task_status(task_id, TaskStatus.PROCESSING,
                                              progress=60, message="임베딩 생성 중...")
                
                # 문서를 Qdrant에 저장
                await self.store_document_sections(task_id, sections, metadata)
            
            # 완료 처리
            task_manager.update_task_status(task_id, TaskStatus.COMPLETED,
                                          progress=100, message="처리 완료")
            
            # 메타데이터 파일 정리
            if os.path.exists(meta_path):
                os.remove(meta_path)
            
            logging.info(f"✅ Task completed: {task_id}")
            
            # 메모리 정리
            gc.collect()
            
        except Exception as e:
            logging.error(f"❌ Task processing failed for {task_id}: {e}")
            task_manager.update_task_status(task_id, TaskStatus.FAILED,
                                          message=f"처리 실패: {str(e)}")

    async def store_document_sections(self, task_id: str, sections: list, metadata: dict):
        """문서 섹션을 Qdrant에 저장"""
        try:
            # 임베딩 생성 및 저장
            embedded_docs = await embed_document_sections(
                sections, self.embedder, self.document_store
            )
            
            # 추가 메타데이터 설정
            for doc in embedded_docs:
                doc.meta.update({
                    "task_id": task_id,
                    "sosok": metadata.get("sosok", ""),
                    "site": metadata.get("site", ""),
                    "tags": metadata.get("tags", ""),
                    "upload_time": time.time()
                })
            
            # Qdrant에 저장
            self.document_store.write_documents(embedded_docs)
            
            logging.info(f"📚 Stored {len(embedded_docs)} document sections for task {task_id}")
            
        except Exception as e:
            logging.error(f"❌ Failed to store document sections for {task_id}: {e}")
            raise



# 전역 프로세서 인스턴스
background_processor = None

async def start_background_processor(document_store, embedder):
    """백그라운드 프로세서 시작"""
    global background_processor
    background_processor = BackgroundFileProcessor(document_store, embedder)
    asyncio.create_task(background_processor.start())
    logging.info("🎯 Background file processor started")

async def stop_background_processor():
    """백그라운드 프로세서 중지"""
    global background_processor
    if background_processor:
        await background_processor.stop()
        background_processor = None