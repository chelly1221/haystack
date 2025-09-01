from haystack import Document
import numpy as np
import logging

def embed_document_sections(sections, metadata_base, total_pages, embedder):
    embedded_docs = []
    # 문서 제목 추출
    document_title = metadata_base.get("original_filename", "Unknown Document")
    
    for idx, section in enumerate(sections):
        if len(section["content"].strip()) < 1:
            logging.info(f"⏭ Skipping low-content section: {section['title']}")
            continue

        meta = {
            **metadata_base,
            "section_title": section["title"],
            "section_id": section.get("section_id", f"{idx + 1}"),
            "section_number": idx + 1,
            "page_number": section.get("page_number", section.get("start_page")),
            "total_pdf_pages": total_pages
        }

        # 문서 제목과 섹션 제목 모두 포함하여 임베딩
        content_with_header = f"문서: {document_title}\n<h2>{section['title']}</h2>\n{section['content']}"
        doc = Document(content=content_with_header, meta=meta)
        embedded_section = embedder.run([doc])["documents"]
        embedded_docs.extend(embedded_section)
        
        # 로깅 추가 (선택사항)
        logging.info(f"✅ Embedded section from '{document_title}' - Section: {section['title']}")
    
    return embedded_docs

def embed_query(query_text, embedder):
    query_doc = Document(content=query_text)
    result = embedder.run([query_doc])
    documents = result.get("documents", [])
    if not documents or documents[0].embedding is None or len(documents[0].embedding) == 0:
        raise ValueError("❌ Embedding failed: query embedding is empty")
    return documents[0].embedding

def cosine_similarity(a, b):
    a = np.array(a)
    b = np.array(b)
    if a.size == 0 or b.size == 0:
        return 0.0
    return np.dot(a, b) / (np.linalg.norm(a) * np.linalg.norm(b))