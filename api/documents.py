from fastapi import APIRouter, HTTPException, Query, Form
from typing import List, Optional
from pydantic import BaseModel
from haystack import Document
import logging
import unicodedata
from collections import defaultdict
import asyncio
import os
import re

router = APIRouter()

class DuplicateCheckRequest(BaseModel):
    filenames: List[str]
    sosok: Optional[str] = None
    site: Optional[str] = None

def check_document_access(doc_meta, sosok, site):
    """Check if user has access to document based on sosok/site permissions"""
    doc_sosok = doc_meta.get("sosok", "").strip()
    doc_site = doc_meta.get("site", "").strip()
    
    # Admin access - can see everything
    if sosok == "관리자" and site == "관리자":
        return True
    
    # Must match sosok
    if sosok and doc_sosok != sosok:
        return False
    
    # Check site access
    if site:
        # Check for department-wide access (e.g., "레이더관제부_전체")
        if site.endswith("_전체"):
            # Allow access to all sites in the same sosok
            # When site is "부서_전체", user can see all sites in that sosok
            return True
        else:
            # Exact site match required
            if doc_site != site:
                return False
    
    return True

def get_documents_router(document_store):
    @router.get("/list-documents/")
    async def list_documents(sosok: Optional[str] = Query(None), site: Optional[str] = Query(None)):
        try:
            loop = asyncio.get_event_loop()
            docs = await loop.run_in_executor(None, lambda: document_store.filter_documents(filters={}))

            # Apply enhanced permission filtering
            filtered_docs = []
            for doc in docs:
                meta = doc.meta or {}
                if check_document_access(meta, sosok, site):
                    filtered_docs.append(doc)

            # Rest of the logic is unchanged
            page_count = defaultdict(int)
            pdf_page_total = defaultdict(int)
            page_numbers = defaultdict(set)

            for doc in filtered_docs:
                meta = doc.meta or {}
                file_id = meta.get("file_id", "")
                page_count[file_id] += 1
                if "page_number" in meta:
                    page_numbers[file_id].add(meta["page_number"])
                if "total_pdf_pages" in meta and pdf_page_total.get(file_id, 0) == 0:
                    pdf_page_total[file_id] = meta["total_pdf_pages"]

            result = []
            seen_files = set()

            for doc in filtered_docs:
                meta = doc.meta or {}
                file_id = meta.get("file_id", "")
                if file_id in seen_files:
                    continue
                seen_files.add(file_id)

                result.append({
                    "id": doc.id,
                    "filename": meta.get("original_filename", "unknown.pdf"),
                    "tags": [tag.strip() for tag in meta.get("tags", "").split(",") if tag.strip()]
                            if isinstance(meta.get("tags", ""), str) else [],
                    "file_id": file_id,
                    "num_sections": page_count.get(file_id, 0),
                    "total_pdf_pages": pdf_page_total.get(file_id, 0),
                    "page_numbers": sorted(list(page_numbers.get(file_id, set()))),
                    "sosok": meta.get("sosok", ""),
                    "site": meta.get("site", "")
                })

            return {"documents": result}

        except Exception as e:
            logging.error("❌ Error during listing documents: %s", e)
            raise HTTPException(status_code=500, detail=str(e))

    @router.post("/check-duplicate/")
    async def check_duplicate(request: DuplicateCheckRequest):
        """Check if files with the same names already exist in the same sosok/site"""
        try:
            loop = asyncio.get_event_loop()
            docs = await loop.run_in_executor(None, lambda: document_store.filter_documents(filters={}))
            
            # Extract from request body
            filenames = request.filenames
            sosok = request.sosok.strip() if request.sosok else ""
            site = request.site.strip() if request.site else ""
            normalized_filenames = [unicodedata.normalize("NFC", f.strip()) for f in filenames]
            
            # Log for debugging
            logging.info(f"🔍 Checking duplicates for: {normalized_filenames} in sosok='{sosok}', site='{site}'")
            
            # Find duplicates - Fixed to check all files
            duplicates = []
            file_info_map = {}  # Track file info by filename
            
            # First, collect all unique files with matching criteria
            for doc in docs:
                meta = doc.meta or {}
                doc_filename = meta.get("original_filename", "").strip()
                doc_sosok = meta.get("sosok", "").strip()
                doc_site = meta.get("site", "").strip()
                doc_file_id = meta.get("file_id", "")
                
                # Check if this document matches our criteria
                # For duplicate check, we need exact match (not permission-based)
                if (doc_sosok == sosok and 
                    doc_site == site and 
                    doc_filename in normalized_filenames):
                    
                    # Use file_id as unique key to avoid duplicate entries for same file
                    if doc_file_id not in file_info_map:
                        file_info_map[doc_file_id] = {
                            "filename": doc_filename,
                            "file_id": doc_file_id,
                            "upload_date": meta.get("upload_date", ""),
                            "tags": meta.get("tags", "")
                        }
                        logging.info(f"✅ Duplicate found: {doc_filename} (file_id: {doc_file_id})")
            
            # Now check each requested filename and add ALL duplicates
            for filename in normalized_filenames:
                duplicates_for_this_file = []
                for file_info in file_info_map.values():
                    if file_info["filename"] == filename:
                        duplicates_for_this_file.append(file_info)
                
                # Add all duplicates for this filename
                duplicates.extend(duplicates_for_this_file)
                
                if duplicates_for_this_file:
                    logging.info(f"📋 Found {len(duplicates_for_this_file)} duplicate(s) for: {filename}")
            
            logging.info(f"📊 Total duplicates found: {len(duplicates)}")
            return {"duplicates": duplicates}
            
        except Exception as e:
            logging.error("❌ Error checking duplicates: %s", e)
            raise HTTPException(status_code=500, detail=str(e))

    @router.get("/check-duplicate/")
    async def check_duplicate_get(
        filenames: List[str] = Query(...),
        sosok: Optional[str] = Query(None),
        site: Optional[str] = Query(None)
    ):
        """GET version of check-duplicate for compatibility"""
        request = DuplicateCheckRequest(
            filenames=filenames,
            sosok=sosok,
            site=site
        )
        return await check_duplicate(request)

    @router.put("/update-document-tags/")
    async def update_document_tags(
        file_id: str = Query(...),
        tags: str = Form(...),
        sosok: Optional[str] = Query(None),
        site: Optional[str] = Query(None)
    ):
        """Update tags for a specific document"""
        try:
            file_id = unicodedata.normalize("NFC", file_id.strip())
            
            logging.info(f"🏷️ Updating tags for file_id: {file_id} with tags: {tags}")
            
            loop = asyncio.get_event_loop()
            docs = await loop.run_in_executor(None, lambda: document_store.filter_documents(filters={}))
            
            # Find all documents with this file_id
            matching_docs = []
            for doc in docs:
                meta = doc.meta or {}
                if meta.get("file_id", "") == file_id:
                    # Check if user has permission to update this document
                    if check_document_access(meta, sosok, site):
                        matching_docs.append(doc)
                    else:
                        raise HTTPException(status_code=403, detail="Permission denied")
            
            if not matching_docs:
                raise HTTPException(status_code=404, detail="Document not found")
            
            # Update tags for all matching documents
            updated_count = 0
            for doc in matching_docs:
                # Update the metadata
                doc.meta["tags"] = tags
                
                # Delete and re-add the document with updated metadata
                # Since Haystack doesn't have a direct update_document_meta method
                document_store.delete_documents(document_ids=[doc.id])
                
                # Create new document with updated metadata
                new_doc = Document(
                    content=doc.content,
                    meta=doc.meta,
                    embedding=doc.embedding
                )
                document_store.write_documents([new_doc])
                updated_count += 1
            
            logging.info(f"✅ Updated tags for {updated_count} documents with file_id: {file_id}")
            
            return {
                "status": "success",
                "message": f"Tags updated for {updated_count} documents",
                "file_id": file_id,
                "new_tags": tags
            }
            
        except HTTPException:
            raise
        except Exception as e:
            logging.error("❌ Error updating tags: %s", e)
            raise HTTPException(status_code=500, detail=str(e))

    @router.delete("/delete-document/")
    async def delete_document(
        file_id: Optional[str] = Query(None), 
        filename: Optional[str] = Query(None),
        sosok: Optional[str] = Query(None),
        site: Optional[str] = Query(None)
    ):
        try:
            if not file_id and not filename:
                raise HTTPException(status_code=400, detail="file_id or filename must be provided.")

            file_id = unicodedata.normalize("NFC", file_id.strip()) if file_id else None
            filename = unicodedata.normalize("NFC", filename.strip()) if filename else None

            loop = asyncio.get_event_loop()
            docs = await loop.run_in_executor(None, lambda: document_store.filter_documents(filters={}))

            ids_to_delete = []
            deleted_file_ids = set()

            for doc in docs:
                meta = doc.meta or {}
                doc_file_id = meta.get("file_id", "").strip()
                doc_filename = meta.get("original_filename", "").strip()

                # Check permission before deletion
                if not check_document_access(meta, sosok, site):
                    continue

                if file_id and doc_file_id == file_id:
                    ids_to_delete.append(doc.id)
                    deleted_file_ids.add(doc_file_id)
                elif filename and doc_filename == filename:
                    ids_to_delete.append(doc.id)
                    deleted_file_ids.add(doc_file_id)

            if not ids_to_delete:
                return {"status": "error", "message": "No documents matched the given identifier or permission denied."}

            document_store.delete_documents(document_ids=ids_to_delete)

            upload_dir = "./uploads"
            deleted_files = []
            for fid in deleted_file_ids:
                for f in os.listdir(upload_dir):
                    if fid in f:
                        try:
                            os.remove(os.path.join(upload_dir, f))
                            deleted_files.append(f)
                            logging.info(f"🗑 Deleted file from disk: {f}")
                        except Exception as e:
                            logging.warning(f"⚠ Failed to delete uploaded file {f}: {e}")

            return {
                "status": "success",
                "message": f"Deleted {len(ids_to_delete)} document(s) and {len(deleted_files)} file(s)."
            }

        except Exception as e:
            logging.error("❌ Error during deletion: %s", e)
            raise HTTPException(status_code=500, detail=str(e))

    @router.get("/filter-documents-by-tags/")
    async def filter_documents_by_tags(
        tags: List[str] = Query(...),
        sosok: Optional[str] = Query(None),
        site: Optional[str] = Query(None)
    ):
        try:
            loop = asyncio.get_event_loop()
            docs = await loop.run_in_executor(None, lambda: document_store.filter_documents(filters={}))

            # Filter documents by tags and permissions
            matched_file_ids = set()
            file_info = {}  # Store file information

            for doc in docs:
                meta = doc.meta or {}
                file_id = meta.get("file_id", "")

                # Check permission
                if not check_document_access(meta, sosok, site):
                    continue

                raw_tags = meta.get("tags", [])
                doc_tags = (
                    [tag.strip() for tag in raw_tags.split(",") if tag.strip()]
                    if isinstance(raw_tags, str)
                    else [tag.strip() for tag in raw_tags if isinstance(tag, str)]
                )

                if all(tag in doc_tags for tag in tags):
                    matched_file_ids.add(file_id)
                    # Store file information only once per file_id
                    if file_id not in file_info:
                        file_info[file_id] = {
                            "id": doc.id,
                            "filename": meta.get("original_filename", "unknown.pdf"),
                            "tags": doc_tags,
                            "file_id": file_id,
                            "sosok": meta.get("sosok", ""),
                            "site": meta.get("site", ""),
                            "total_pdf_pages": meta.get("total_pdf_pages", 0),
                            "num_sections": 0  # Will be counted later
                        }

            # Count sections for each matched file
            for doc in docs:
                meta = doc.meta or {}
                file_id = meta.get("file_id", "")
                if file_id in matched_file_ids:
                    if file_id in file_info:
                        file_info[file_id]["num_sections"] += 1

            # Convert to list
            result = list(file_info.values())

            return {"documents": result}

        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))

    @router.get("/get-page-content/")
    async def get_page_content(
        file_id: str = Query(...), 
        page_number: int = Query(...),
        sosok: Optional[str] = Query(None),
        site: Optional[str] = Query(None)
    ):
        try:
            loop = asyncio.get_event_loop()
            docs = await loop.run_in_executor(None, lambda: document_store.filter_documents(filters={}))

            def section_sort_key(title):
                match = re.match(r'^(\d+(?:\.\d+)*)', title)
                if not match:
                    return []
                return [int(part) for part in match.group(1).split(".")]

            matching_docs = []
            for doc in docs:
                meta = doc.meta or {}
                if (str(meta.get("file_id")) == str(file_id) and 
                    str(meta.get("page_number")) == str(page_number)):
                    # Check permission
                    if check_document_access(meta, sosok, site):
                        matching_docs.append({
                            "title": meta.get("section_title", "제목 없음"),
                            "content": doc.content,
                            "section_id": meta.get("section_id", "")
                        })

            def section_sort_key(id_str):
                match = re.match(r'^(\d+(?:\.\d+)*)', id_str)
                if not match:
                    return [999]
                return [int(part) for part in match.group(1).split(".")]

            matching_docs.sort(key=lambda d: section_sort_key(d["section_id"]))

            if matching_docs:
                return {
                    "status": "success",
                    "page_number": page_number,
                    "sections": matching_docs
                }

            return {"status": "error", "message": "해당 페이지에는 내용이 없습니다."}

        except Exception as e:
            logging.error("❌ Error retrieving page content: %s", e)
            raise HTTPException(status_code=500, detail=f"Failed to retrieve page content: {str(e)}")

    @router.get("/list-tags/")
    async def list_tags(sosok: Optional[str] = Query(None), site: Optional[str] = Query(None)):
        try:
            loop = asyncio.get_event_loop()
            docs = await loop.run_in_executor(None, lambda: document_store.filter_documents(filters={}))

            tag_set = set()

            for doc in docs:
                meta = doc.meta or {}
                
                # Check permission
                if not check_document_access(meta, sosok, site):
                    continue

                raw_tags = meta.get("tags", [])
                tags = (
                    [tag.strip() for tag in raw_tags.split(",") if tag.strip()]
                    if isinstance(raw_tags, str)
                    else [tag.strip() for tag in raw_tags if isinstance(tag, str)]
                )
                tag_set.update(tags)

            return {"tags": sorted(tag_set)}

        except Exception as e:
            logging.error("❌ 태그 리스트 가져오기 오류: %s", e)
            raise HTTPException(status_code=500, detail=str(e))

    return router