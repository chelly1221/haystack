from fastapi import APIRouter, UploadFile, File, Form
from typing import List, Optional
# Simple document class to replace Haystack Document
class SimpleDocument:
    def __init__(self, content: str, meta: dict = None):
        self.content = content
        self.meta = meta or {}
        self.id = meta.get('id') if meta else None
import os, shutil, uuid, unicodedata, logging, gc
import pdfplumber
import asyncio, re
import json
from decimal import Decimal
from util.pdf import split_pdf_by_section_headings, split_pdf_by_token_window
from util.embedding import embed_document_sections
from util.docx import split_docx_by_section_headings, split_docx_by_token_window
from util.pptx import split_pptx_by_section_headings, split_pptx_by_token_window
from util.hwpx import parse_hwpx_content_with_page
UPLOAD_DIR = "./uploads"
os.makedirs(UPLOAD_DIR, exist_ok=True)

def get_upload_router(vector_store, embedder):
    router = APIRouter()

    @router.post("/upload-pdf/")
    async def upload_pdf(
        files: List[UploadFile] = File(...),
        tags: Optional[str] = Form(None),
        sosok: Optional[str] = Form(None),
        site: Optional[str] = Form(None),
        type: Optional[str] = Form(None),
        top_margin: Optional[Decimal] = Form(Decimal("0.1")),
        bottom_margin: Optional[Decimal] = Form(Decimal("0.1")),
        margin_settings: Optional[str] = Form(None),  # JSON string with per-file margins
        overwrite_decisions: Optional[str] = Form(None)  # JSON string with overwrite decisions
    ):
        results = []
        
        # Normalize sosok and site values - CRITICAL: must match documents.py logic exactly
        sosok = sosok.strip() if sosok else ""
        site = site.strip() if site else ""
        
        logging.info(f"📥 Upload request - sosok: '{sosok}', site: '{site}', files: {len(files)}")
        
        # Parse margin settings if provided
        margin_map = {}
        if margin_settings:
            try:
                margin_data = json.loads(margin_settings)
                margin_map = margin_data
                logging.info(f"📐 Margin settings: {margin_map}")
            except json.JSONDecodeError:
                logging.warning("Failed to parse margin_settings JSON")
        
        # Parse overwrite decisions if provided
        overwrite_map = {}
        if overwrite_decisions:
            try:
                overwrite_map = json.loads(overwrite_decisions)
                logging.info(f"📝 Overwrite decisions: {overwrite_map}")
            except json.JSONDecodeError:
                logging.warning("Failed to parse overwrite_decisions JSON")

        for idx, file in enumerate(files):
            embedded_docs = []
            try:
                normalized_filename = unicodedata.normalize("NFC", file.filename.strip())
                ext = os.path.splitext(file.filename.lower())[-1]
                if ext not in [".pdf", ".hwpx", ".docx", ".pptx"]:
                    results.append({
                        "status": "실패",
                        "message": "PDF, HWPX, DOCX, PPTX 파일만 지원됩니다.",
                        "original_filename": normalized_filename
                    })
                    continue

                # Check for overwrite decision (just for logging)
                overwrite_action = overwrite_map.get(normalized_filename, None)
                if overwrite_action == "overwrite":
                    logging.info(f"🔄 Overwrite decision received for: {normalized_filename}")
                    # Note: Actual deletion is handled by frontend before upload

                # Generate unique filename
                unique_filename = f"{uuid.uuid4().hex}_{normalized_filename}"
                
                # If keep-both is selected, add timestamp to filename
                if overwrite_action == "keep-both":
                    import time
                    timestamp = int(time.time())
                    name_parts = normalized_filename.rsplit('.', 1)
                    if len(name_parts) == 2:
                        unique_filename = f"{uuid.uuid4().hex}_{name_parts[0]}_{timestamp}.{name_parts[1]}"
                    else:
                        unique_filename = f"{uuid.uuid4().hex}_{normalized_filename}_{timestamp}"
                
                file_path = os.path.join(UPLOAD_DIR, unique_filename)

                with open(file_path, "wb") as buffer:
                    shutil.copyfileobj(file.file, buffer, length=8192)
                file.file.close()

                # Get margins for this specific file
                file_margins = margin_map.get(file.filename, {})
                file_top_margin = Decimal(file_margins.get("top_margin", str(top_margin)))
                file_bottom_margin = Decimal(file_margins.get("bottom_margin", str(bottom_margin)))

                loop = asyncio.get_event_loop()
                if ext == ".pdf":
                    def detect_maintenance_doc(path):
                        with pdfplumber.open(path) as pdf:
                            for page in pdf.pages:
                                text = page.extract_text()
                                if text:
                                    first_line = text.strip().splitlines()[0]
                                    cleaned = re.sub(r"\\s+", "", first_line)
                                    return cleaned.startswith("유지보수교범")
                        return False

                    is_maintenance_pdf = await loop.run_in_executor(None, detect_maintenance_doc, file_path)

                    if is_maintenance_pdf:
                        # Pass document title as the last parameter
                        sections = await loop.run_in_executor(
                            None,
                            lambda: split_pdf_by_section_headings(
                                file_path,
                                None,
                                file_top_margin,
                                file_bottom_margin,
                                doc_id=None,
                                extract_text_tables=True,
                                auto_detect_header_footer=True,
                                document_title=normalized_filename
                            )
                        )
                    else:
                        sections = await loop.run_in_executor(
                            None,
                            split_pdf_by_token_window,
                            file_path,
                            file_top_margin,
                            file_bottom_margin,
                            700,
                            100,
                            "./models/KURE-v1",
                            None,  # doc_id
                            True,  # extract_text_tables
                            True   # auto_detect_header_footer
                        )

                    def get_total_pages(path):
                        with pdfplumber.open(path) as pdf:
                            return len(pdf.pages)
                    total_pages = await loop.run_in_executor(None, get_total_pages, file_path)

                elif ext == ".hwpx":
                    from util.hwpx import split_hwpx_by_pages
                    sections = await loop.run_in_executor(None, split_hwpx_by_pages, file_path, unique_filename)
                    total_pages = max([s.get("page_number", 0) for s in sections]) if sections else 1

                elif ext == ".docx":
                    from transformers import AutoTokenizer
                    tokenizer = AutoTokenizer.from_pretrained("./models/KURE-v1")
                    sections = await loop.run_in_executor(
                        None,
                        split_docx_by_token_window,
                        file_path,
                        700,
                        100,
                        tokenizer
                    )
                    total_pages = len(sections)

                elif ext == ".pptx":
                    from transformers import AutoTokenizer
                    tokenizer = AutoTokenizer.from_pretrained("./models/KURE-v1")
                    sections = await loop.run_in_executor(
                        None,
                        split_pptx_by_token_window,
                        file_path,
                        700,
                        100,
                        tokenizer
                    )
                    total_pages = len(sections)

                if not sections:
                    fail_msg = {
                        ".pdf": "PDF에서 추출된 페이지가 없습니다.",
                        ".hwpx": "HWPX 문서에서 추출된 내용이 없습니다.",
                        ".docx": "DOCX 문서에서 추출된 내용이 없습니다.",
                        ".pptx": "PPTX 문서에서 추출된 내용이 없습니다."
                    }.get(ext, "문서에서 추출된 내용이 없습니다.")

                    results.append({
                        "status": "실패",
                        "message": fail_msg,
                        "original_filename": normalized_filename
                    })
                    continue

                # CRITICAL: metadata must match exactly what documents.py expects
                metadata_base = {
                    "original_filename": normalized_filename,
                    "tags": ", ".join([tag.strip() for tag in tags.split(",") if tag.strip()]) if tags else "",
                    "sosok": sosok,  # Already normalized with strip()
                    "site": site,    # Already normalized with strip()
                    "file_id": unique_filename,
                    "total_pdf_pages": total_pages
                }

                logging.info(f"📝 Metadata for new document: filename='{metadata_base['original_filename']}', sosok='{metadata_base['sosok']}', site='{metadata_base['site']}'")

                embedded_docs = embed_document_sections(sections, metadata_base, total_pages, embedder)

                # Filter invalid docs
                embedded_docs = [doc for doc in embedded_docs if doc.embedding is not None and len(doc.embedding) > 0]

                if embedded_docs:
                    logging.info(f"📌 Saving {len(embedded_docs)} embedded documents for: {normalized_filename}")
                    vector_store.write_documents(embedded_docs)
                    
                else:
                    logging.warning(f"⚠ No valid embedded documents to write for: {normalized_filename}")
                    results.append({
                        "status": "실패",
                        "message": "임베딩된 문서가 없어서 저장되지 않았습니다.",
                        "original_filename": normalized_filename
                    })
                    continue

                try:
                    os.remove(file_path)
                except OSError as e:
                    logging.warning(f"⚠️ Failed to delete uploaded file {file_path}: {e}")

                # Prepare result with overwrite status
                result_item = {
                    "status": "성공",
                    "message": f"{ext.upper()[1:]} 파일 임베딩 후 저장됨",
                    "original_filename": file.filename,
                    "num_pages": len(embedded_docs),
                    "total_pdf_pages": total_pages
                }
                
                # Add overwrite status if applicable
                if overwrite_action == "overwrite":
                    result_item["overwritten"] = True
                    result_item["message"] = f"{ext.upper()[1:]} 파일 업로드 완료 (덮어쓰기)"
                elif overwrite_action == "keep-both":
                    result_item["kept_both"] = True
                    result_item["message"] = f"{ext.upper()[1:]} 파일 중복 저장됨"
                
                results.append(result_item)

                del embedded_docs
                gc.collect()

            except Exception as e:
                logging.error(f"❌ Error processing file {file.filename}: {str(e)}")
                import traceback
                traceback.print_exc()
                results.append({
                    "status": "실패",
                    "message": f"처리 중 오류 발생: {str(e)}",
                    "original_filename": file.filename,
                    "num_pages": len(embedded_docs) if isinstance(embedded_docs, list) else 0
                })

        logging.info(f"📊 Upload complete. Processed {len(files)} files with {len(results)} results")
        return {"results": results}

    return router
