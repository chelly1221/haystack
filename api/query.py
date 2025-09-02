from fastapi import APIRouter, Query, Request, HTTPException
from fastapi.responses import StreamingResponse
from typing import List, Optional
from haystack import Document
from util.embedding import embed_query, cosine_similarity
from llama_server_generator import LlamaServerGenerator
import asyncio
import traceback
import json

router = APIRouter()

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
            dept_name = site[:-3]  # Remove "_전체"
            # Need to check if this document belongs to the department
            # This would require additional metadata or logic to determine department
            # For now, we'll implement a simple check
            return True  # Allow access to all sites in the department
        else:
            # Exact site match required
            if doc_site != site:
                return False
    
    return True

def get_query_router(document_store, embedder):
    @router.get("/query-stream/")
    async def stream_query_answer(
        user_query: str = Query(...),
        tags: Optional[List[str]] = Query(None),
        doc_names: Optional[List[str]] = Query(None),
        sosok: Optional[str] = Query(None),
        site: Optional[str] = Query(None),
        top_n: int = 4
    ):
        try:
            sosok = sosok.strip() if sosok else None
            site = site.strip() if site else None
            query_embedding = embed_query(user_query, embedder)

            if tags:
                if isinstance(tags, str):
                    tags = [tags]
                elif len(tags) == 1 and "," in tags[0]:
                    tags = [t.strip() for t in tags[0].split(",") if t.strip()]
            if doc_names:
                if isinstance(doc_names, str):
                    doc_names = [doc_names]
                elif len(doc_names) == 1 and "," in doc_names[0]:
                    doc_names = [d.strip() for d in doc_names[0].split(",") if d.strip()]

            loop = asyncio.get_event_loop()
            all_docs = await loop.run_in_executor(None, lambda: document_store.filter_documents(filters={}))

            # Filter documents with enhanced permission check
            filtered_docs = []
            for doc in all_docs:
                if not check_document_access(doc.meta or {}, sosok, site):
                    continue
                    
                # Apply additional filters
                if tags:
                    tags_list = [tag.strip() for tag in tags]
                    doc_tags = [t.strip() for t in doc.meta.get("tags", "").split(",")]
                    if not all(tag in doc_tags for tag in tags_list):
                        continue
                        
                if doc_names:
                    doc_names_list = [name.strip() for name in doc_names]
                    if doc.meta.get("original_filename", "").strip() not in doc_names_list:
                        continue
                
                filtered_docs.append(doc)

            if not filtered_docs:
                async def error_stream():
                    error_data = json.dumps({"content": "⚠️ 관련 문서를 찾지 못했습니다."})
                    yield f"data: {error_data}\n\n"
                    yield "data: [DONE]\n\n"
                return StreamingResponse(error_stream(), media_type="text/event-stream")

            scored_docs = []
            for doc in filtered_docs:
                if doc.embedding is not None and len(doc.embedding) > 0:
                    score = cosine_similarity(doc.embedding, query_embedding)
                    scored_docs.append((doc, score))

            scored_docs.sort(key=lambda x: x[1], reverse=True)
            top_docs = [doc for doc, _ in scored_docs[:top_n]]

            context = "\n\n".join([doc.content for doc in top_docs if doc.content])
            
            prompt = f"""<|im_start|>system
당신은 주어진 문서를 바탕으로 질문에 정확하고 상세하게 답변하는 한국어 AI 어시스턴트입니다.

다음 규칙을 반드시 준수하세요:
1. 문서에 없는 내용은 추측하지 말고, 문서에 기반한 사실만을 답변하세요.
2. 답변 형식:
   - 질문과 관련된 내용을 최대한 원문그대로 답변하세요.
<|im_end|>
<|im_start|>user
다음 문서들을 참고하여 질문에 답변해주세요.

### 참고 문서:
{context}

### 질문:
{user_query}

URL을 반드시 전부 누락없이 원문대로 표시하세요.
<|im_end|>
<|im_start|>assistant
"""
            
            # Use original LlamaServerGenerator without extra parameters
            generator = LlamaServerGenerator(server_url="http://192.168.10.101:8080")
            
            # Stream
            stream = generator.stream(
                prompt,
                temperature=0.4,
                top_p=0.9
            )

            async def event_generator():
                async for chunk in stream:
                    # JSON 형태로 청크를 래핑하여 전송
                    json_chunk = json.dumps({"content": chunk})
                    yield f"data: {json_chunk}\n\n"
                # 스트림 종료 신호
                yield "data: [DONE]\n\n"

            return StreamingResponse(event_generator(), media_type="text/event-stream")

        except Exception as e:
            traceback.print_exc()
            error_message = f"⌒ 서버 오류 발생: {str(e)}"

            async def err_gen():
                error_data = json.dumps({"error": error_message})
                yield f"data: {error_data}\n\n"
                yield "data: [DONE]\n\n"

            return StreamingResponse(err_gen(), media_type="text/event-stream")

    @router.get("/query-documents/")
    async def get_query_documents(
        user_query: str = Query(...),
        tags: Optional[List[str]] = Query(None),
        doc_names: Optional[List[str]] = Query(None),
        sosok: Optional[str] = Query(None),
        site: Optional[str] = Query(None),
        top_n: int = 4
    ):
        """Get document metadata for the top matching documents without generating response"""
        try:
            sosok = sosok.strip() if sosok else None
            site = site.strip() if site else None
            
            query_embedding = embed_query(user_query, embedder)
            
            # Parse tags and doc_names
            if tags:
                if isinstance(tags, str):
                    tags = [tags]
                elif len(tags) == 1 and "," in tags[0]:
                    tags = [t.strip() for t in tags[0].split(",") if t.strip()]
            if doc_names:
                if isinstance(doc_names, str):
                    doc_names = [doc_names]
                elif len(doc_names) == 1 and "," in doc_names[0]:
                    doc_names = [d.strip() for d in doc_names[0].split(",") if d.strip()]
            
            # Get and filter documents
            loop = asyncio.get_event_loop()
            all_docs = await loop.run_in_executor(None, lambda: document_store.filter_documents(filters={}))
            
            # Filter documents with enhanced permission check
            filtered_docs = []
            for doc in all_docs:
                if not check_document_access(doc.meta or {}, sosok, site):
                    continue
                    
                # Apply additional filters
                if tags:
                    tags_list = [tag.strip() for tag in tags]
                    doc_tags = [t.strip() for t in doc.meta.get("tags", "").split(",")]
                    if not all(tag in doc_tags for tag in tags_list):
                        continue
                        
                if doc_names:
                    doc_names_list = [name.strip() for name in doc_names]
                    if doc.meta.get("original_filename", "").strip() not in doc_names_list:
                        continue
                
                filtered_docs.append(doc)
            
            if not filtered_docs:
                return {"documents": [], "message": "관련 문서를 찾지 못했습니다."}
            
            # Score and sort documents
            scored_docs = []
            for doc in filtered_docs:
                if doc.embedding is not None and len(doc.embedding) > 0:
                    score = cosine_similarity(doc.embedding, query_embedding)
                    scored_docs.append((doc, score))
            
            scored_docs.sort(key=lambda x: x[1], reverse=True)
            top_docs = [doc for doc, score in scored_docs[:top_n]]
            
            # Prepare document metadata
            documents = []
            for doc, score in scored_docs[:top_n]:
                meta = doc.meta or {}
                documents.append({
                    "filename": meta.get("original_filename", "Unknown"),
                    "page_number": meta.get("page_number", ""),
                    "section_title": meta.get("section_title", ""),
                    "section_id": meta.get("section_id", ""),
                    "file_id": meta.get("file_id", ""),
                    "score": round(score, 4),
                    "tags": meta.get("tags", ""),
                    "sosok": meta.get("sosok", ""),
                    "site": meta.get("site", ""),
                    "total_pages": meta.get("total_pdf_pages", 0)
                })
            
            return {
                "query": user_query,
                "documents": documents,
                "total_matched": len(filtered_docs),
                "returned": len(documents)
            }
            
        except Exception as e:
            traceback.print_exc()
            raise HTTPException(status_code=500, detail=f"Document query failed: {str(e)}")

    return router